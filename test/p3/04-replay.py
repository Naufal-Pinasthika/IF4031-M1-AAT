from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from sandbox import Sandbox


def scenario(h):
    with Sandbox(h) as lab:
        for iteration in range(h.args.repeat):
            if iteration:
                lab.control('/admin/reports', {'reports': []})
                lab.adapter(1)
            ref = f'{lab.name}-{iteration}'
            base = {'report_id': ref, 'volcano_id': 'MERAPI', 'alert_level': 'Siaga',
                    'eruption_count_24h': 2, 'ash_column_height_m': 10, 'reported_at': '2026-11-21T12:00:00Z'}
            lab.control('/admin/reports', {'mode': 'rename', 'reports': [base]})
            first = h.wait(lambda: h.sql("SELECT row_to_json(r) FROM (SELECT message_id,status FROM durable_inbox "
                                        f"WHERE source_ref_id='{ref}' AND status='parked' LIMIT 1) r"),
                           'Renamed required field was not parked')
            raw, metadata = h.raw_message(first['message_id'])
            h.check(json.loads(raw)['report_ref'] == ref and metadata['reason'] and metadata['received_at'],
                    'Ops retrieves the intact incompatible message and rejection metadata')
            later = {**base, 'eruption_count_24h': '7', 'reported_at': '2026-11-21T12:01:00Z'}
            other = {**base, 'report_id': ref + '-other', 'confidence_level': .9, 'future_field': {'value': 1}}
            lab.control('/admin/reports', {'reports': [later, other]})
            h.wait(lambda: h.sql(f"SELECT to_json(count(*)) FROM durable_inbox WHERE source_ref_id='{ref}-other' AND status='done'") == 1,
                   'A parked identity blocked an unrelated additive-schema report')
            h.check(h.sql(f"SELECT to_json(count(*)) FROM durable_inbox WHERE source_ref_id='{ref}' AND status='done'") == 0,
                    'A later revision cannot bypass its parked predecessor')
            for role in ('FIELD', 'MEDIA'):
                forged = {'X-Client-Role': 'ops', 'X-Client-ID': h.values['OPS_CLIENT_ID']}
                for path, body in [('/unprocessed', None), ('/unprocessed/' + first['message_id'], None),
                                   ('/unprocessed/replay', {'source': 'PVMBG'})]:
                    h.check(h.api(path, role, body, forged)[0] == 403, role + ' cannot inspect or replay, including forged Ops headers')
            h.check(h.request('client', '/unprocessed')[0] == 401, 'Unauthenticated inspection is rejected')
            lab.adapter(2)
            h.check(h.api('/unprocessed/replay', body={'source': 'PVMBG'})[0] == 202, 'Authorized Ops replay is accepted')
            h.wait(lambda: h.sql(f"SELECT to_json(count(*)) FROM durable_inbox WHERE source_ref_id='{ref}' AND status='done'") == 2,
                   'Not all compatible archived revisions recovered')
            ordered = h.sql("SELECT json_agg(r ORDER BY ordinal) FROM (SELECT ordinal,completed_at FROM durable_inbox "
                            f"WHERE source_ref_id='{ref}' ORDER BY ordinal) r")
            h.check(ordered[0]['completed_at'] <= ordered[1]['completed_at'], 'Replay completed revisions in arrival order')
            def projected():
                event = next((e for e in h.complete_audit('PVMBG') if e['source_ref_id'] == ref), None)
                return event if event and event['attributes']['eruption_count_24h'] == 7 else None
            h.wait(projected, 'Canonical state does not contain the final recovered revision')
            before = h.sql("SELECT to_json(count(*)) FROM ingestion_outbox WHERE source='PVMBG'")
            for _ in range(3):
                h.check(h.api('/unprocessed/replay', body={'source': 'PVMBG'})[0] == 202, 'Repeated Ops replay succeeds')
            h.check(h.sql("SELECT to_json(count(*)) FROM ingestion_outbox WHERE source='PVMBG'") == before,
                    'Repeated replay generates no duplicate canonical or broker intent')
            h.artifact(f'round-{iteration}-order.json', ordered)
            h.note({'iteration': iteration, 'recovered_revisions': 2, 'ordering_verified': True, 'duplicate_effects': 0})


if __name__ == '__main__':
    raise SystemExit(run('p3/04-replay', ['P3.1', 'P3.4', 'P3.5'], parser('Verify schema isolation, authorized replay and revision ordering'), scenario))
