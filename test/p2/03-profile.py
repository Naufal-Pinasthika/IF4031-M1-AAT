from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import shlex
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import ROOT, parser, run

spec = importlib.util.spec_from_file_location('capacity', Path(__file__).with_name('01-capacity.py'))
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def scenario(h):
    h.changes()
    h.check(h.args.repeat == 1, 'Run one bounded profile per invocation; rerun for an independent capture')
    h.check(h.args.vps, 'Profiling uses the VPS application and an external generator')
    original = {}
    pool = ThreadPoolExecutor(max_workers=8)
    futures = []
    try:
        for app, flag in [('aggregator', 'AGGREGATOR_PROFILE_ENABLED'),
                          ('aggregator-gateway', 'GATEWAY_PROFILE_ENABLED')]:
            deployment = json.loads(h.kube('get', 'deployment/'+app, '-o', 'json'))
            container = deployment['spec']['template']['spec']['containers'][0]
            original[app] = (flag, container['name'], next((e for e in container.get('env', []) if e['name'] == flag), None))
            h.kube('set', 'env', 'deployment/'+app, flag+'=1')
            h.kube('rollout', 'status', 'deployment/'+app, '--timeout=180s')
        pods = {p['name']: p for p in h.pods().values() if p['app'] in original and p['ready']}
        h.artifact('profile-pods.json', pods)

        def capture(name):
            for suffix, path in [('before.json', '/runtime'), ('cpu.pprof', '/cpu?seconds=20'), ('after.json', '/runtime')]:
                payload = h.hosts.ssh(h.server, 'sudo -n k3s kubectl -n sistem-informasi-kebencanaan-bnpb exec '+
                    shlex.quote(name)+' -- wget -qO- '+shlex.quote('http://127.0.0.1:6060'+path), capture=True)
                (h.folder/(name+'-'+suffix)).write_bytes(payload)

        started = False
        def during(elapsed):
            nonlocal started
            if elapsed >= 15 and not started:
                started = True
                futures.extend(pool.submit(capture, name) for name in pods)
        result = capacity.remote_generator(h, 'profile', 'scripts/m2/p2/throughput.js',
            {'VUS': str(h.args.vus), 'DURATION': '60s'}, during)
        for future in futures:
            future.result()
        h.artifact('diagnostic.json', result)
        h.check(result['exit_code'] == 0 and result['valid_timing'], 'Diagnostic workload completed without unexpected failures')
    finally:
        pool.shutdown(wait=True)
        for app, (flag, container, entry) in original.items():
            h.kube('set', 'env', 'deployment/'+app, flag+'-')
            if entry is not None:
                patch = {'spec': {'template': {'spec': {'containers': [{'name': container, 'env': [entry]}]}}}}
                h.kube('patch', 'deployment/'+app, '--type=strategic', '-p', json.dumps(patch))
            h.kube('rollout', 'status', 'deployment/'+app, '--timeout=180s')


if __name__ == '__main__':
    p = parser('Capture private CPU profiles under the unchanged P2 workload; not an acceptance benchmark')
    p.add_argument('--vus', type=int, default=1000)
    p.add_argument('--max-rss-mib', type=int, default=6144)
    p.add_argument('--min-free-mib', type=int, default=512)
    p.add_argument('--generator-procs', type=int, default=6)
    p.add_argument('--generator-gogc', type=int, default=100)
    p.set_defaults(profile=True, seconds=60, repeat=1)
    raise SystemExit(run('p2/03-profile', ['P2 diagnostic only'], p, scenario))
