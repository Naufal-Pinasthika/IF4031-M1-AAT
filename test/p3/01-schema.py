from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def rows(h, ordinal):
    return h.sql("SELECT COALESCE(json_agg(r ORDER BY ordinal),'[]'::json) FROM "
                 "(SELECT m.ordinal,m.message_id,m.source_ref_id,m.status,m.reason,m.completed_at,"
                 "convert_from(c.payload,'UTF8')::jsonb AS body FROM durable_inbox m "
                 "JOIN inbox_chunks c USING(message_id) WHERE source='PVMBG' "
                 f"AND kind='volcanic-report' AND chunk=0 AND ordinal>{int(ordinal)}) r")


def scenario(h):
    h.changes()
    original = json.loads(h.kube('get', 'configmap', 'aggregator-config', '-o', 'json'))['data'].get('PVMBG_ADAPTER_VERSION')
    try:
        for round_number in range(h.args.repeat):
            h.schema('legacy')
            h.adapter('1')
            for mode in ('additive', 'rename', 'type-change', 'missing'):
                start = h.sql("SELECT to_json(COALESCE(max(ordinal),0)) FROM durable_inbox")
                h.schema(mode)

                def candidates():
                    records = rows(h, start)
                    if mode == 'additive':
                        return [r for r in records if 'confidence_level' in r['body'] and r['status'] == 'done']
                    if mode == 'rename':
                        return [r for r in records if 'report_ref' in r['body'] and r['status'] == 'parked']
                    if mode == 'type-change':
                        return [r for r in records if isinstance(r['body'].get('eruption_count_24h'), str) and r['status'] == 'parked']
                    return [r for r in records if 'alert_level' not in r['body'] and r['status'] == 'parked']

                observed = h.wait(candidates, 'No newly generated report demonstrated mode ' + mode)
                h.artifact(f'round-{round_number}-{mode}.json', observed)
                h.check(all(r['reason'] for r in observed) if mode != 'additive' else True,
                        mode + ': policy outcome and rejection reason recorded')
            h.note({'round': round_number, 'adapter': '1', 'schema_policy_verified': True})
    finally:
        try:
            h.schema('legacy')
        finally:
            h.adapter(original)


if __name__ == '__main__':
    raise SystemExit(run('p3/01-schema', ['P3.1'], parser('Check the declared PVMBG schema policy'), scenario))
