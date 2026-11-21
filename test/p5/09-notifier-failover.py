from pathlib import Path
import json
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from coastal import hazard, p95, timestamp
from notifier_sandbox import RedundantNotifierSandbox


def scenario(h):
    h.check(h.args.count >= 20, 'Each crash window contains at least twenty new critical identities')
    with RedundantNotifierSandbox(h) as lab:
        expected = set()

        def append(prefix, count):
            items = [{'observation_id': prefix+'-'+str(i), 'station_id': 'contract-station',
                'observed_at': '2097-01-01T00:00:00Z', 'sea_level_anomaly_m': .4,
                'wave_height_m': 1.8, 'quality': 'ok', 'critical': True} for i in range(count)]
            lab.control('/admin/append', {'items': items})
            expected.update(hazard(item['observation_id']) for item in items)
            return items

        priming = append(lab.name+'-priming', 3)
        identities = {hazard(item['observation_id']) for item in priming}

        def warm():
            for index in range(3):
                raw = h.kube('logs', lab.name+'-notifier-'+str(index), '--tail=1000')
                records = []
                for line in raw.splitlines():
                    try:
                        records.append(json.loads(line))
                    except ValueError:
                        pass
                received = {r.get('hazard_id') for r in records
                    if r.get('msg') == 'notification_delivered' and r.get('lane') == 'critical'}
                if not identities <= received:
                    return False
            return True

        h.wait(warm, 'All three independent critical readers must be active before crash injection')
        results = []
        for repetition in range(h.args.repeat):
            name = lab.name+'-notifier-'+str(repetition % 3)
            before = h.pods()
            target_uid, target = next((uid, p) for uid, p in before.items() if p['name'] == name)
            survivors = {uid: p for uid, p in before.items() if p['app'] == lab.name+'-notifier' and uid != target_uid}
            h.check(len(survivors) == 2 and all(p['ready'] for p in survivors.values()), 'Two independent Ready notifiers survive the selected crash')
            started = time.monotonic()
            h.kube('delete', 'pod', name, '--force', '--grace-period=0', '--wait=false')
            items = append(lab.name+'-crash-'+str(repetition), h.args.count)

            def receipts():
                found = lab.notifier_sql("SELECT COALESCE(json_agg(payload),'[]'::json) FROM notifier_notifications")
                mapping = {n['hazard_id']: n for n in found}
                return mapping if expected <= mapping.keys() else None

            found = h.wait(receipts, 'Surviving critical readers did not durably deliver every crash-window identity')
            source = lab.control('/admin/status')
            latencies = [timestamp(found[hazard(item['observation_id'])]['created_at'])
                -source['available'][item['observation_id']] for item in items]
            h.check(len(found) == len(expected), 'Shared durable notification identities have no duplicates')
            rows = lab.notifier_sql('SELECT count(*)::int FROM notifier_notifications')
            h.check(rows == len(expected), 'Durable row count matches independently generated notification identities')
            after = h.pods()
            h.check(all(uid in after and after[uid]['restarts'] == p['restarts'] for uid, p in survivors.items()),
                'The crash does not restart either surviving notifier')
            h.check(target_uid not in after, 'The exact selected notifier pod was removed')
            result = {'round': repetition+1, 'target': name, 'new_critical': len(items),
                'critical_p95_seconds': p95(latencies), 'critical_max_seconds': max(latencies),
                'observed_recovery_seconds': time.monotonic()-started, 'unique_notifications': len(found),
                'scope': 'Controlled source through three candidate Aggregators, real Kafka, redundant notifier and its replicated PostgreSQL on VPS; not official-source acceptance'}
            results.append(result)
            h.artifact('campaign.json', results)
            h.note(result)
            h.check(min(latencies) >= -.1, 'Source and notifier clocks are comparable')
            h.check(p95(latencies) <= 5, 'Critical source-availability to durable notification p95 stays within five seconds after notifier crash')
            h.kube('rollout', 'status', 'statefulset/'+lab.name+'-notifier', '--timeout=180s')
            identities = {hazard(item['observation_id']) for item in items}
            h.wait(warm, 'Replacement resumes its stable independent critical group')


if __name__ == '__main__':
    p = parser('Verify redundant critical readers preserve delivery and deduplication across repeated VPS notifier pod crashes')
    p.add_argument('--notifier-image-tag', required=True)
    p.add_argument('--count', type=int, default=30)
    raise SystemExit(run('p5/09-notifier-failover', ['P5.1 (controlled)', 'P5.6 (notifier crash)'], p, scenario))
