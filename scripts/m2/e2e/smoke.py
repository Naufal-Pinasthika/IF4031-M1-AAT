#!/usr/bin/env python3
"""Black-box E2E checks against the isolated Kubernetes lab; never records tokens."""
import base64
import datetime
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[3]
NAMESPACE = 'sistem-informasi-kebencanaan-bnpb'
os.environ['KUBECONFIG'] = str(ROOT / '.e2e/kubeconfig')
VALUES = {}
for line in (ROOT / '.env').read_text().splitlines():
    if line and not line.startswith('#') and '=' in line:
        key, value = line.split('=', 1)
        VALUES[key] = value.strip().strip('\"\'')
BASE = os.environ.get('E2E_NODE', 'http://192.168.93.10')
RESULTS = []


def request(port, path, headers=None, method='GET', body=None):
    correlation = 'e2e-' + uuid.uuid4().hex
    headers = {'X-Correlation-ID': correlation, **(headers or {})}
    if body is not None:
        body = json.dumps(body).encode()
        headers['Content-Type'] = 'application/json'
    started = time.monotonic()
    req = urllib.request.Request(f'{BASE}:{port}{path}', headers=headers, method=method, data=body)
    try:
        response = urllib.request.urlopen(req, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read()
        status = response.status
    elapsed = round((time.monotonic() - started) * 1000, 2)
    try:
        data = json.loads(payload)
    except ValueError:
        data = payload.decode(errors='replace')[:2000]
    # Auth bodies contain credentials; never persist them in evidence.
    RESULTS.append({'path': path, 'status': status, 'latency_ms': elapsed, 'correlation_id': correlation})
    return status, data, elapsed


def token(client):
    basic = base64.b64encode(f"{VALUES[client+'_CLIENT_ID']}:{VALUES[client+'_CLIENT_SECRET']}".encode()).decode()
    status, pair, _ = request(30084, '/v1/token', {'Authorization': 'Basic ' + basic}, 'POST')
    assert status == 200, f'{client} authentication failed: {status}'
    return {'Authorization': 'Bearer ' + pair['access_token']}


def kube(*args):
    return subprocess.check_output(['kubectl', '-n', NAMESPACE, *args], text=True)


def audit_all(source):
    items = []
    path = '/internal/audit/events?source=' + source + '&limit=1000'
    while True:
        for attempt in range(5):
            status, page, _ = request(30083, path, {'Authorization': 'Bearer ' + VALUES['P1_AUDIT_TOKEN']})
            if status == 200 and not page.get('partial', True):
                break
            time.sleep(1)
        assert status == 200 and not page['partial'], f'audit unavailable: {status}, units={page.get("unavailable_units")}'
        items.extend(page['items'])
        if not page.get('next_cursor'):
            return items
        path = '/internal/audit/events?source=' + source + '&limit=1000&cursor=' + page['next_cursor']


def notifications_all():
    items, seen = [], set()
    path = '/notifications?limit=1000'
    while True:
        status, page, _ = request(30086, path)
        assert status == 200, f'notifications: {status}'
        items.extend(page['notifications'])
        cursor = page.get('next_cursor')
        if not cursor:
            return items
        assert cursor not in seen, 'notification cursor did not advance'
        seen.add(cursor)
        path = '/notifications?limit=1000&cursor=' + urllib.parse.quote(cursor, safe='')


def main():
    for port, path in [(30083, '/ready'), (30080, '/health'), (30084, '/health'), (30090, '/healthz'), (30086, '/health')]:
        status, _, _ = request(port, path)
        assert status == 200, f'{port}{path}: {status}'
    field, media, ops = token('FIELD'), token('MEDIA'), token('OPS')
    for name, headers in [('Field', field), ('Media', media)]:
        status, payload, elapsed = request(30080, '/events?limit=20', headers)
        assert status == 200, f'{name} events: {status}'
        assert isinstance(payload, dict) and payload.get('partial') is False, (
            f'{name} events incomplete: partial={payload.get("partial") if isinstance(payload, dict) else None}, '
            f'unavailable_units={payload.get("unavailable_units") if isinstance(payload, dict) else None}')
        print(f'{name} Client API: HTTP {status}, {elapsed} ms', flush=True)
    assert request(30080, '/unprocessed', media)[0] == 403
    assert request(30080, '/unprocessed', field)[0] == 403
    assert request(30080, '/unprocessed', ops)[0] == 200
    assert request(30083, '/internal/audit/events')[0] == 401
    assert request(30090, '/v1/stations')[0] == 401
    print('Ops/audit/source authentication boundaries: PASS', flush=True)

    deadline = time.monotonic() + 90
    sources = {}
    while time.monotonic() < deadline:
        sources = {name: audit_all(name) for name in ['BMKG', 'PVMBG', 'SENSOR_PANTAI']}
        if all(sources.values()):
            break
        time.sleep(2)
    assert all(sources.values()), f'sources without canonical events: {[k for k,v in sources.items() if not v]}'
    for source, items in sources.items():
        identities = [(x['source'], x['source_ref_id']) for x in items]
        assert len(identities) == len(set(identities)), f'duplicate {source} identities'
        print(f'{source}: {len(items)} canonical events, unique identities', flush=True)
    coastal = sources['SENSOR_PANTAI']
    critical = {x['hazard_id'] for x in coastal if x['attributes'].get('critical') is True}
    assert critical, 'no critical coastal event observed; critical path remains untested'
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        notifications = notifications_all()
        delivered = {x['hazard_id'] for x in notifications if x.get('critical')}
        if critical <= delivered:
            break
        time.sleep(0.5)
    assert critical <= delivered, f'{len(critical-delivered)} admitted critical events missing at notifier'
    coastal_notifications = [x for x in notifications if x['source'] == 'SENSOR_PANTAI' and x.get('critical')]
    latency = sorted(x['ingress_latency_ms'] for x in coastal_notifications)
    p95 = latency[max(0, (95*len(latency)+99)//100-1)]
    print(f'Coastal critical notifications: {len(critical)} admitted IDs delivered; ingress-to-notifier p95={p95} ms', flush=True)
    RESULTS.append({'critical_admitted': len(critical), 'critical_delivered': len(critical & delivered), 'critical_ingress_p95_ms': p95,
                    'note': 'Delivery reconciled against admitted canonical events; not a full sensor-reference completeness proof.'})
    # Latency target is reported independently so a mismatch does not hide the other evidence.
    RESULTS.append({'critical_latency_target_passed': p95 <= 5000})
    topology = json.loads(kube('get', 'pods', '-l', 'app=aggregator', '-o', 'json'))
    active = [pod for pod in topology['items'] if not pod['metadata'].get('deletionTimestamp')]
    ready = [pod for pod in active if any(condition.get('type') == 'Ready' and condition.get('status') == 'True'
                                        for condition in pod.get('status', {}).get('conditions', []))]
    assert len(ready) >= 3, 'fewer than three Aggregators Ready'
    assert len({pod['spec']['nodeName'] for pod in ready}) >= 3, 'Ready Aggregators not spread across three nodes'
    RESULTS.append({'aggregator_nodes': {pod['metadata']['name']:pod['spec']['nodeName'] for pod in topology['items']}})
    print('HTTP/auth/ingestion/broker/notifier smoke checks: PASS', flush=True)


if __name__ == '__main__':
    passed = False
    try:
        main()
        passed = True
    finally:
        destination = ROOT / '.e2e/smoke-results.json'
        destination.write_text(json.dumps({'ran_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'passed': passed, 'checks': RESULTS}, indent=2))
