#!/usr/bin/env python3
"""Destructive fault tests ONLY for the isolated QEMU lab created by lab.sh.

Usage: python3 scripts/m2/e2e/resilience.py failover|freeze|sensor-outage|network-policy
Every injected fault has a finally-block that restores the lab configuration.
"""
import datetime
import json
from pathlib import Path
import socket
import shlex
import subprocess
import sys
import time

import smoke

ROOT = smoke.ROOT
EVIDENCE = []


def command(*args, timeout=30, check=True):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{args[:4]}: {result.stderr[-1500:]}')
    return result


def kube(*args, **kwargs):
    return command('kubectl', '-n', smoke.NAMESPACE, *args, **kwargs).stdout.strip()


def sql(query):
    # Read-only white-box instrumentation via the VM's local CRI. It avoids
    # conflating Kubernetes remote-exec transport stalls with database health.
    remote = 'container=$(k3s crictl ps --label io.kubernetes.pod.name=aggregator-coordinator-0 -q); k3s crictl exec "$container" psql -U aggregator_coord -d coordination -At -c '+shlex.quote(query)
    return command('bash',str(Path(__file__).with_name('lab.sh')),'ssh','0',remote).stdout.strip()


def leases():
    return json.loads(sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT source,holder_id,fencing_token,lease_until,clock_timestamp() AS database_now FROM source_leases WHERE source IN ('BMKG','PVMBG','SENSOR_PANTAI')) t"))


def pods():
    return json.loads(kube('get', 'pods', '-l', 'app=aggregator', '-o', 'json'))['items']


def wait_new_owner(before, limit=60):
    start = time.monotonic()
    while time.monotonic() - start < limit:
        current = next(x for x in leases() if x['source'] == before['source'])
        if current['holder_id'] != before['holder_id'] and current['fencing_token'] > before['fencing_token']:
            return {'seconds': round(time.monotonic()-start, 2), 'before': before, 'after': current}
        time.sleep(0.5)
    raise AssertionError('No successor within 60 seconds')


def failover():
    before = next(x for x in leases() if x['source'] == 'BMKG')
    kube('delete', 'pod', before['holder_id'], '--wait=false')
    result = wait_new_owner(before)
    EVIDENCE.append(result)
    print(f"Leader deletion: successor in {result['seconds']} s", flush=True)
    kube('rollout', 'status', 'deployment/aggregator', '--timeout=180s', timeout=200)


def monitor(node, action):
    path = f'/var/lib/aat-m2-e2e/node{node}/monitor.sock'
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(3)
        connection.connect(path)
        def response():
            payload = b''
            while b'(qemu)' not in payload:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                payload += chunk
            return payload.decode(errors='replace')
        response()
        connection.sendall((action+'\n').encode())
        return response()


def freeze():
    topology = {x['metadata']['name']:x['spec']['nodeName'] for x in pods()}
    eligible = [x for x in leases() if topology.get(x['holder_id']) in ('aat-node-1', 'aat-node-2')]
    if not eligible:
        raise AssertionError('No source leader on a worker VM; arrange leadership before retrying (control plane must not be frozen)')
    before = eligible[0]
    node = int(topology[before['holder_id']].rsplit('-', 1)[1])
    database_start = sql('SELECT clock_timestamp()')
    monitor(node, 'stop')
    try:
        result = wait_new_owner(before)
        # Keep the old VM paused beyond the configured 15-second lease TTL.
        time.sleep(10)
    finally:
        monitor(node, 'cont')
    resumed_at = sql('SELECT clock_timestamp()')
    time.sleep(8)
    # Admission after expiry with the old holder/epoch must be impossible even
    # if an already committed outbox intent is legitimately replayed later.
    source = before['source']
    old_holder = before['holder_id']
    old_epoch = int(before['fencing_token'])
    count = int(sql(f"SELECT count(*) FROM ingestion_outbox WHERE source='{source}' AND holder_id='{old_holder}' AND fencing_token={old_epoch} AND accepted_at > '{database_start}'::timestamptz + interval '15 seconds'"))
    assert count == 0, f'{count} stale admissions after expiry'
    EVIDENCE.append({'vm': node, 'frozen_at': database_start, 'resumed_at': resumed_at, 'stale_admissions': count, 'handoff': result})
    print(f"VM {node} freeze: successor in {result['seconds']} s; stale admissions={count}", flush=True)


def metrics(pod):
    topology = {x['metadata']['name']:x['spec']['nodeName'] for x in pods()}
    node = topology[pod].rsplit('-',1)[1]
    remote = 'container=$(k3s crictl ps --label '+shlex.quote('io.kubernetes.pod.name='+pod)+' -q); k3s crictl exec "$container" wget -qO- http://127.0.0.1:8083/metrics'
    return command('bash',str(Path(__file__).with_name('lab.sh')),'ssh',node,remote).stdout


def sensor_outage():
    before = next(x for x in leases() if x['source'] == 'SENSOR_PANTAI')
    pod = before['holder_id']
    upstream_before = leases()
    field = smoke.token('FIELD')
    field_issued_at = time.monotonic()
    timings = []
    opened = False
    # Disconnect BNPB from the source without deleting its emptyDir/history.
    # Tester Agent remains explicitly allowed and is never obstructed.
    policy = {'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy',
              'metadata':{'name':'aat-e2e-coastal-outage','namespace':smoke.NAMESPACE},
              'spec':{'podSelector':{'matchLabels':{'app':'sensor-pantai'}},'policyTypes':['Ingress'],
                      'ingress':[{'from':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'tester-agent'}}}]}]}}
    subprocess.run(['kubectl','apply','-f','-'], input=json.dumps(policy), text=True, check=True, capture_output=True)
    try:
        # Existing TCP sessions can survive a NetworkPolicy update. Restart
        # only the current source owner so takeover must establish a new
        # connection through the injected policy (a combined fault test).
        kube('delete','pod',pod,'--wait=false')
        EVIDENCE.append({'combined_fault_leader_handoff':wait_new_owner(before)})
        for _ in range(10):
            if time.monotonic()-field_issued_at > 20:
                field = smoke.token('FIELD')
                field_issued_at = time.monotonic()
            status, _, elapsed = smoke.request(30080, '/events?source=BMKG&limit=10', field)
            assert status == 200, f'BMKG during coastal outage: HTTP {status}'
            timings.append(elapsed)
            pod = next(x for x in leases() if x['source'] == 'SENSOR_PANTAI')['holder_id']
            opened |= 'aggregator_coastal_circuit_state{state="open"} 1' in metrics(pod)
            time.sleep(2)
        assert opened, 'circuit did not open during sensor outage'
    finally:
        kube('delete', 'networkpolicy/aat-e2e-coastal-outage', '--ignore-not-found', '--wait=false', timeout=60)
    start = time.monotonic()
    while time.monotonic()-start < 60:
        pod = next(x for x in leases() if x['source'] == 'SENSOR_PANTAI')['holder_id']
        if 'aggregator_coastal_circuit_state{state="closed"} 1' in metrics(pod):
            break
        time.sleep(1)
    else:
        raise AssertionError('coastal circuit did not recover automatically')
    EVIDENCE.append({'circuit_opened': opened, 'auto_recovery_seconds': round(time.monotonic()-start, 2), 'bmkg_response_ms': timings, 'leaders_before': upstream_before})
    print(f'Coastal circuit opened/recovered automatically; BMKG max={max(timings):.2f} ms', flush=True)


def network_policy():
    # Probe with existing BusyBox nc from different service identities.
    owner = pods()[0]['metadata']['name']
    for caller, target, allowed in [(owner, 'aggregator-coordinator', True), ('deployment/client-api', 'aggregator-coordinator', False), ('deployment/bmkg', 'postgres-a', False), (owner, 'auth-postgres', False), ('deployment/auth', 'auth-postgres', True)]:
        probe = command('kubectl', '-n', smoke.NAMESPACE, 'exec', caller, '--', 'nc', '-z', '-w', '2', target, '5432', timeout=10, check=False)
        connected = probe.returncode == 0
        assert connected == allowed, f'{caller} -> {target}: connected={connected}, expected={allowed}; {probe.stderr}'
        EVIDENCE.append({'caller':caller, 'target':target, 'connected':connected, 'expected':allowed})
    print('Storage owner-only NetworkPolicies: PASS', flush=True)


if __name__ == '__main__':
    case = sys.argv[1]
    passed = False
    try:
        {'failover':failover, 'freeze':freeze, 'sensor-outage':sensor_outage, 'network-policy':network_policy}[case]()
        passed = True
    finally:
        (ROOT / f'.e2e/{case}-results.json').write_text(json.dumps({'ran_at':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'passed':passed, 'evidence':EVIDENCE}, indent=2))
