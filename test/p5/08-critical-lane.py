from pathlib import Path
import datetime as dt
import json
import shlex
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run, Inconclusive
from coastal import hazard, p95, timestamp
from coastal_sandbox import CoastalSandbox


def scenario(h):
    with CoastalSandbox(h) as lab:
        started = time.monotonic()
        h.wait(lambda: h.sql("SELECT to_json(EXISTS(SELECT 1 FROM source_clock_progress WHERE source='SENSOR_PANTAI'))"),
               'Source contact was never established before the steady-state backlog experiment')
        h.note({'startup_source_contact_seconds': time.monotonic()-started,
                'scope': 'Steady-state backlog isolation; startup availability is recorded separately, not asserted to meet five seconds'})
        general_sent = 0
        critical_sent = 0
        for repetition in range(h.args.repeat):
            now = dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')
            messages = []
            for index in range(h.args.backlog):
                identity = f'{lab.name}-backlog-{repetition}-{index}'
                messages.append(json.dumps({'event_type': 'hazard.event.upserted', 'schema_version': 1,
                    'published_at': now, 'correlation_id': identity,
                    'data': {'hazard_id': identity, 'source': 'BMKG', 'source_ref_id': identity,
                             'hazard_type': 'SEISMIC', 'severity': 'NORMAL', 'occurred_at': now, 'ingested_at': now}}, separators=(',', ':')))
            command = ['sudo', '-n', 'k3s', 'kubectl', '-n', NS, 'exec', '-i', 'kafka-0', '--',
                       'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m', '/opt/kafka/bin/kafka-console-producer.sh',
                       '--bootstrap-server', 'localhost:9092', '--topic', lab.name, '--producer-property', 'acks=all']
            h.hosts.ssh(h.server, shlex.join(command), ('\n'.join(messages)+'\n').encode(), capture=True)
            general_sent += len(messages)
            code, before, _ = h.request('p5notifier', '/stats')
            pending_before = general_sent - (before['notification_count']-critical_sent)
            h.check(code == 200, 'Notifier statistics are available while the general lane drains')
            if pending_before <= 0:
                raise Inconclusive('General lane already drained before critical injection; this run cannot prove backlog isolation')
            items = []
            for index in range(30):
                items.append({'observation_id': f'{lab.name}-critical-{repetition}-{index}', 'station_id': 'contract-station',
                              'observed_at': '2097-01-01T00:00:00Z', 'sea_level_anomaly_m': .5,
                              'wave_height_m': 2, 'quality': 'ok', 'critical': True})
            lab.control('/admin/append', {'items': items})
            critical_sent += len(items)
            def received():
                code, stats, _ = h.request('p5notifier', '/stats')
                return stats if code == 200 and stats['coastal_critical_count'] == critical_sent else None
            stats = h.wait(received, 'Critical messages remained blocked behind the general backlog')
            source = lab.control('/admin/status')
            latencies = []
            for item in items:
                code, receipt, _ = h.request('p5notifier', '/notifications/confirm', body={'hazard_id': hazard(item['observation_id'])}, method='POST')
                h.check(code == 200 and receipt.get('durability_confirmed'), 'Critical notification is durably confirmed')
                latencies.append(timestamp(receipt['notification']['created_at'])-source['available'][item['observation_id']])
            remaining = general_sent-(stats['notification_count']-critical_sent)
            result = {'round': repetition+1, 'general_pending_before': pending_before,
                      'general_pending_when_critical_delivered': remaining, 'critical': len(items),
                      'critical_p95_seconds': p95(latencies), 'critical_max_seconds': max(latencies)}
            h.note(result)
            h.artifact(f'round-{repetition+1}.json', result)
            h.check(remaining > 0, 'Critical delivery completed before the general backlog drained')
            h.check(p95(latencies) <= 5, 'Critical delivery p95 remains at most five seconds despite ordinary backlog')


if __name__ == '__main__':
    p = parser('Prove the VPS critical pipeline progresses while a measured ordinary Kafka backlog remains')
    p.add_argument('--notifier-image-tag', required=True)
    p.add_argument('--backlog', type=int, default=10000)
    raise SystemExit(run('p5/08-critical-lane', ['P5.1 (controlled)', 'P5.5 (event isolation)'], p, scenario))
