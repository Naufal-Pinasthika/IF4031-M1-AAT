from pathlib import Path
import datetime as dt
import json
import random
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from coastal import hazard, p95, timestamp
from coastal_sandbox import CoastalSandbox


def scenario(h):
    h.check(h.args.count >= 20, 'Campaign has enough independently identified critical observations')
    results = []
    with CoastalSandbox(h) as lab:
        expected, critical, invalid, duplicates = {}, set(), 0, 0
        for repetition in range(h.args.repeat):
            for age in ('fresh', 'aged'):
                round_critical = set()
                origin = '2001-01-01T00:00:00Z' if age == 'fresh' else '2097-09-17T00:00:00Z'
                # Timestamp origin is deliberately unrelated to wall time or
                # source speed. Its age never changes expected acceptance.
                lab.control('/admin/config', {'mode': 'normal', 'origin': origin})
                rng = random.Random(h.args.fault_seed + repetition)
                modes = ['timeout', 'reset', 'unavailable', 'malformed', 'stalled', 'owner_crash']
                rng.shuffle(modes)
                for mode in modes:
                    fault_started = dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')
                    lab.control('/admin/config', {'mode': mode})
                    ref = f'{lab.name}-{repetition}-{age}-during-{mode}'
                    injected = {'observation_id': ref, 'station_id': 'contract-station', 'observed_at': origin,
                                'sea_level_anomaly_m': .4, 'wave_height_m': 1.8, 'quality': 'ok', 'critical': True}
                    lab.control('/admin/append', {'items': [injected]})
                    expected[hazard(ref)] = ref
                    critical.add(hazard(ref))
                    round_critical.add(hazard(ref))
                    if mode == 'owner_crash':
                        owner = h.sql("SELECT to_json(holder_id) FROM source_leases WHERE source='SENSOR_PANTAI'")
                        h.check(owner.startswith(lab.name+'-agg-'), 'Only a run-owned polling owner is a crash target')
                        h.kube('delete', 'pod', owner, '--grace-period=0', '--force', '--wait=false')
                    before = lab.control('/admin/status')
                    time.sleep(h.args.fault_seconds)
                    after = lab.control('/admin/status')
                    count = len(after['requests']) - len(before['requests'])
                    h.check(count <= 2 + int(h.args.fault_seconds / .1), 'Fault traffic stays within the configured source poll bound')
                    if mode == 'unavailable' and count:
                        logs = h.kube('logs', '-l', 'app='+lab.name+'-agg', '--since-time='+fault_started,
                                      '--tail=500', '--max-log-requests=3')
                        recorded = []
                        for line in logs.splitlines():
                            try:
                                row = json.loads(line)
                            except ValueError:
                                continue
                            if row.get('msg') == 'coastal_request_failed' and row.get('status') == 503:
                                recorded.append(row)
                        h.artifact(f'round-{repetition+1}-{age}-http-errors.json', recorded)
                        h.check(bool(recorded) and all(r.get('correlation_id') and r.get('latency_ms') is not None for r in recorded),
                                'Real HTTP 503 source attempts retain status, correlation ID and duration')
                    lab.control('/admin/config', {'mode': 'normal'})
                    # Advancing generated_at, not healthz, must restore source use.
                    time.sleep(2.5)
                items = []
                for i in range(h.args.count):
                    ref = f'{lab.name}-{repetition}-{age}-{i}'
                    item = {'observation_id': ref, 'station_id': 'contract-station', 'observed_at': origin,
                            'sea_level_anomaly_m': .4, 'wave_height_m': 1.8, 'quality': 'ok', 'critical': i % 3 == 0}
                    items.append(item)
                    expected[hazard(ref)] = ref
                    if item['critical']:
                        critical.add(hazard(ref))
                        round_critical.add(hazard(ref))
                bad = {'observation_id': f'bad-{repetition}-{age}', 'station_id': 'contract-station', 'critical': True}
                items.insert(rng.randrange(len(items)), bad)
                invalid += 1
                items.insert(rng.randrange(len(items)), dict(items[-1]))
                duplicates += 1
                lab.control('/admin/append', {'items': items})
                def notifications():
                    found, cursor = {}, ''
                    for _ in range(100):
                        path = '/notifications?limit=1000' + ('&cursor=' + cursor if cursor else '')
                        code, page, _ = h.request('p5notifier', path)
                        if code != 200:
                            return None
                        for item in page['notifications']:
                            found[item['hazard_id']] = item
                        cursor = page.get('next_cursor')
                        if not cursor:
                            break
                    return found if set(expected) <= found.keys() else None
                found = h.wait(notifications, 'The pipeline did not recover and deliver every accepted fixture identity')
                source = lab.control('/admin/status')
                latencies = []
                timings = []
                for identity in round_critical:
                    value = timestamp(found[identity]['created_at'])
                    latencies.append(value - source['available'][expected[identity]])
                    timings.append({'source_ref_id': expected[identity], 'hazard_id': identity,
                                    'available_at_unix': source['available'][expected[identity]],
                                    'created_at': found[identity]['created_at'],
                                    'latency_seconds': latencies[-1]})
                h.artifact(f'round-{repetition+1}-{age}-critical.json', sorted(timings, key=lambda r: r['latency_seconds'], reverse=True))
                h.check(len(found) == len(expected), 'No invalid or duplicated fixture observation creates a user notification')
                parked = h.sql("SELECT count(*)::int AS count FROM unprocessed_messages WHERE source='SENSOR_PANTAI' AND status='parked'")
                h.check(parked >= invalid, 'Invalid observations are durably quarantined with a reason')
                result = {'round': repetition+1, 'clock_origin': age, 'unique_notifications': len(found),
                          'critical': len(critical), 'round_critical': len(round_critical), 'critical_p95_seconds': p95(latencies), 'critical_max_seconds': max(latencies), 'fault_order': modes,
                          'invalid_items': invalid, 'duplicate_items': duplicates,
                          'scope': 'Controlled HTTP source through candidate Aggregator, Kafka and durable notifier on VPS; not official-source acceptance'}
                results.append(result)
                h.note(result)
                h.artifact('campaign.json', results)
                h.check(all(value >= -.1 for value in latencies), 'Fixture availability and delivery clocks are comparable')
                h.check(p95(latencies) <= 5, 'Critical source-availability to durable notifier p95 meets five seconds')


if __name__ == '__main__':
    p = parser('Repeated VPS pipeline recovery with arbitrary source-clock origins, bursts, invalid items and transport faults')
    p.add_argument('--notifier-image-tag', required=True)
    p.add_argument('--count', type=int, default=180)
    p.add_argument('--fault-seed', type=int, default=731)
    p.add_argument('--fault-seconds', type=float, default=3)
    raise SystemExit(run('p5/06-resilience', ['P5.1 (controlled)', 'P5.2', 'P5.4'], p, scenario))
