#!/usr/bin/env python3
"""Repeated fan-out recovery using observed identities, not historical counters.

Run as the VPS operator. The official source is unchanged and PVCs are retained.
This checks identity presence, not every update/version, payload or critical SLO.
"""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import random
import subprocess
import time
import urllib.parse
import urllib.request

NS = 'sistem-informasi-kebencanaan-bnpb'


def kube(*args):
    return subprocess.check_output(['k3s', 'kubectl', '--request-timeout=10s', '-n', NS, *args],
                                   text=True, timeout=100)


def endpoint(app, port):
    pods = json.loads(kube('get', 'pods', '-l', 'app='+app, '-o', 'json'))['items']
    pod = next(p for p in pods if not p['metadata'].get('deletionTimestamp') and any(
        c.get('type') == 'Ready' and c.get('status') == 'True'
        for c in p['status'].get('conditions', [])))
    return 'http://'+pod['status']['podIP']+':'+str(port)


def get(base, path):
    with urllib.request.urlopen(base+path, timeout=10) as response:
        raw = response.read((64 << 20)+1)
    if len(raw) > 64 << 20:
        raise RuntimeError('response exceeds evidence memory bound')
    return json.loads(raw)


def notification_ids():
    base = endpoint('notifier-consumer', 8086)
    ids, cursors, cursor = set(), set(), ''
    for _ in range(4096):
        page = get(base, '/notifications?limit=1000'+(
            '&cursor='+urllib.parse.quote(cursor, safe='') if cursor else ''))
        for item in page['notifications']:
            ids.add(item['hazard_id'])
        cursor = page.get('next_cursor')
        if not cursor:
            return ids
        if cursor in cursors:
            raise RuntimeError('notifier pagination stalled')
        cursors.add(cursor)
    raise RuntimeError('notifier pagination exceeds evidence bound')


def missing_ids(target, events):
    ids = [event['hazard_id'] for event in events]
    if len(ids) != len(set(ids)):
        raise RuntimeError('duplicate dashboard hazard identity')
    return target-set(ids)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rounds = []
    for index in range(3):
        initial = notification_ids()
        duration = random.SystemRandom().uniform(12, 18)
        result = dict(round=index+1, started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                      outage_seconds=duration, initial_notifier_id_count=len(initial), passed=False)
        scaled = False
        try:
            kube('scale', 'statefulset/dashboard-consumer', '--replicas=0')
            scaled = True
            kube('wait', '--for=delete', 'pod/dashboard-consumer-0', '--timeout=60s')
            initial = notification_ids()
            result['initial_notifier_id_count'] = len(initial)
            time.sleep(duration)
            target = notification_ids()
            result['independent_new_notifier_ids'] = len(target-initial)
        finally:
            if scaled:
                # Predetermined removal of injected fault, not a recovery repair.
                kube('scale', 'statefulset/dashboard-consumer', '--replicas=1')
        released = time.monotonic()
        kube('rollout', 'status', 'statefulset/dashboard-consumer', '--timeout=90s')
        base = endpoint('dashboard-consumer', 8085)
        deadline = released+60
        while True:
            events = get(base, '/events')['events']
            missing = missing_ids(target, events)
            if not missing or time.monotonic() >= deadline:
                break
            time.sleep(.5)
        identity_file = args.output.with_name('target-identities-'+str(index+1)+'.json')
        identity_file.write_text(json.dumps(sorted(target)))
        result.update(target_notifier_ids=len(target), dashboard_id_count=len(events),
                      missing_ids=sorted(missing), missing_count=len(missing),
                      target_identity_file=identity_file.name,
                      target_sha256=hashlib.sha256(identity_file.read_bytes()).hexdigest(),
                      catchup_seconds=round(time.monotonic()-released, 3),
                      passed=result['independent_new_notifier_ids'] > 0 and not missing
                      and time.monotonic() <= deadline)
        rounds.append(result)
        args.output.write_text(json.dumps(dict(passed=all(r['passed'] for r in rounds), rounds=rounds,
            scope='Observed notifier hazard identities present at dashboard after outage; not per-update/payload reconciliation or critical latency proof'), indent=2))
        print(json.dumps(result), flush=True)
    raise SystemExit(0 if all(r['passed'] for r in rounds) else 1)


if __name__ == '__main__':
    main()
