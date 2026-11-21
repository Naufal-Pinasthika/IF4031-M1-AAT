from pathlib import Path
import json
import math
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


PAYLOADS = {
    'truncated': (b'[{"report_id":', 'invalid_json'),
    'deep': (b'[' * 100 + b'0' + b']' * 100, 'nesting_too_deep'),
    'invalid-utf8': (b'["\xff"]', 'invalid_utf8'),
    'oversized': (b'["' + b'x' * (32 * 64 * 1024) + b'"]', 'payload_too_large'),
}


def scenario(h):
    h.changes()
    before = h.pods()
    h.artifact('pods-before.json', before)
    hold = max(12, 2 * int(h.values.get('AGGREGATOR_POLL_INTERVAL_MS', '5000')) / 1000 + 3)
    try:
        for round_number in range(h.args.repeat):
            for mode, (expected, reason) in PAYLOADS.items():
                h.schema(mode)
                time.sleep(hold)
                logs = h.kube('logs', 'deployment/pvmbg', '--since=' + str(math.ceil(hold)) + 's', '--tail=500')
                polls = []
                for line in logs.splitlines():
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get('path') == '/volcanic-reports' and record.get('status') == 200:
                        polls.append(record)
                h.artifact(f'round-{round_number}-{mode}-polls.json', polls)
                h.check(bool(polls), 'Fresh upstream requests occurred during poison mode ' + mode)

                def preserved():
                    for message in h.unprocessed():
                        if message['status'] == 'parked' and reason in message.get('reason', ''):
                            raw, metadata = h.raw_message(message['message_id'])
                            if raw == expected:
                                h.check(bool(metadata['source']) and bool(metadata['received_at']) and bool(metadata['reason']),
                                        'Preserved message has source, time and reason')
                                return {'message_id': message['message_id'], 'size_bytes': len(raw), 'reason': metadata['reason']}
                    return None

                retained = h.wait(preserved, 'Complete original bytes were not retained for ' + mode)
                h.note({'round': round_number, 'mode': mode, 'fresh_polls': len(polls), 'retained': retained})
            h.schema('legacy')
            start = h.sql("SELECT to_json(count(*)) FROM durable_inbox WHERE source='PVMBG' AND status='done'")
            h.wait(lambda: h.sql("SELECT to_json(count(*)) FROM durable_inbox WHERE source='PVMBG' AND status='done'") > start,
                   'Valid reports did not continue after poison messages')
    finally:
        h.schema('legacy')
    after = h.pods()
    h.artifact('pods-after.json', after)
    for uid, pod in before.items():
        if pod['app'] in ('aggregator', 'pvmbg', 'client-api', 'notifier-consumer', 'dashboard-consumer'):
            h.check(uid in after and after[uid]['restarts'] == pod['restarts'], pod['name'] + ': no replacement or restart during poison campaign')


if __name__ == '__main__':
    raise SystemExit(run('p3/02-poison', ['P3.2', 'P3.3'], parser('Check poison-message isolation and intact retention'), scenario))
