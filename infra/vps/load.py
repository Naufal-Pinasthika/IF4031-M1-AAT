#!/usr/bin/env python3
"""Run k6 on the operator's Linux/WSL, not on any Kubernetes node.

Reuses the independently threaded Linux/Windows memory guards of the local
lab harness, but all Kubernetes operations target the real VPS cluster via SSH.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
from provision import Hosts, ROOT, inventory

spec = importlib.util.spec_from_file_location('memory_harness', ROOT / 'scripts/m2/e2e/run-direct-load.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)
NS = 'sistem-informasi-kebencanaan-bnpb'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--vus', type=int, default=10000)
    p.add_argument('--hold', type=int, default=130)
    p.add_argument('--base-url', default='http://127.0.0.1:30083')
    p.add_argument('--k6', default='/mnt/d/aat-m2-e2e/k6/k6-v2.4.0-linux-amd64/k6')
    args = p.parse_args()
    if not 1 <= args.vus <= 10000 or args.hold < 120:
        p.error('Invalid VU or sustained duration')
    args.generator_procs = 6
    args.min_free_mib = 512
    args.max_rss_mib = 4608
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    server = h.nodes[0]

    def kube(*items):
        import shlex
        command = 'sudo -n k3s kubectl --request-timeout=10s -n ' + NS + ' ' + shlex.join(items)
        result = subprocess.run(h.command(server, command), capture_output=True, text=True, timeout=20)
        if result.returncode:
            raise RuntimeError('VPS Kubernetes command failed: ' + result.stderr[-1500:])
        return result.stdout.strip()

    harness.kube = kube
    env = os.environ.copy()
    for line in (ROOT / '.env').read_text().splitlines():
        key, sep, value = line.partition('=')
        if sep and key == 'AGGREGATOR_API_KEY':
            env[key] = value.strip().strip('"').strip("'")
    env.update(AGGREGATOR_BASE_URL=args.base_url, VUS=str(args.vus),
               HOLD_DURATION=str(args.hold)+'s', K6_ADDRESS='127.0.0.1:6566', K6_NO_USAGE_REPORT='true')
    state = dict(passed=False, endpoint=args.base_url,
                 transport='WireGuard' if args.base_url == 'http://10.77.0.1:30083' else 'SSH tunnel (Linux localhost)',
                 target_vus=args.vus, hold_seconds=args.hold, snapshots=[], vu_samples=[],
                 saw_five=False, saw_two=False, longest_observed_full_vu_seconds=0)
    name = 'vps-linux-scale-' + str(args.vus)
    evidence = ROOT / '.e2e'
    evidence.mkdir(exist_ok=True)
    execution_started = None
    full_started = None
    up = down = False
    last_snapshot = -5
    last_status = None

    def during(_):
        nonlocal execution_started, full_started, up, down, last_snapshot, last_status
        now = time.monotonic()
        try:
            with urllib.request.urlopen('http://127.0.0.1:6566/v1/status', timeout=1) as response:
                status = json.load(response)['data']['attributes']
            last_status = now
        except OSError:
            if execution_started is not None and last_status is not None and now-last_status > 10:
                raise RuntimeError('k6 status telemetry stale for over 10 seconds')
            return
        if not status['running']:
            return
        if execution_started is None:
            execution_started = now
        elapsed = now-execution_started
        state['vu_samples'].append(dict(seconds=round(elapsed, 2), vus=status['vus']))
        if status['vus'] >= args.vus:
            if full_started is None:
                full_started = now
            state['longest_observed_full_vu_seconds'] = max(state['longest_observed_full_vu_seconds'], now-full_started)
        else:
            full_started = None
        if elapsed >= 60 and not up:
            kube('scale', 'deployment/aggregator', '--replicas=5')
            up = True
            print('Requested 5 Aggregators during load', flush=True)
        if elapsed >= 110 and not down:
            kube('scale', 'deployment/aggregator', '--replicas=2')
            down = True
            print('Requested 2 Aggregators during load', flush=True)
        if elapsed-last_snapshot >= 5:
            dep = json.loads(kube('get', 'deployment', 'aggregator', '-o=json'))
            ready = dep['status'].get('readyReplicas', 0)
            state['snapshots'].append(dict(seconds=round(elapsed, 2), desired=dep['spec']['replicas'], ready=ready))
            state['saw_five'] |= up and not down and ready == 5
            state['saw_two'] |= down and ready == 2
            last_snapshot = elapsed
            (evidence / (name+'-results.json')).write_text(json.dumps(state, indent=2))

    try:
        with urllib.request.urlopen(args.base_url+'/ready', timeout=10) as response:
            if json.load(response).get('status') != 'ready':
                raise RuntimeError('Load endpoint is not Ready')
        harness.ready(3)
        harness.run(args, name, 'scripts/m2/p2/load.js', env, during)
        if not state['saw_five'] or not state['saw_two']:
            raise RuntimeError('Both requested replica counts were not observed Ready during load')
        if state['longest_observed_full_vu_seconds'] < 120:
            raise RuntimeError('Target VUs were not observed sustained for at least 120 seconds')
        state['passed'] = True
    except BaseException as exc:
        state['error'] = str(exc)
        raise
    finally:
        try:
            harness.ready(3)
            state['restored_three'] = True
            print('Restored three Aggregators; evidence prefix: .e2e/' + name, flush=True)
        except BaseException as exc:
            state.update(passed=False, restored_three=False, restore_error=str(exc))
            raise
        finally:
            (evidence / (name+'-results.json')).write_text(json.dumps(state, indent=2))


if __name__ == '__main__':
    main()
