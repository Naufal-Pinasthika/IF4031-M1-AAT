from pathlib import Path
import copy
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from coastal import hazard
from coastal_sandbox import CoastalSandbox


def scenario(h):
    with CoastalSandbox(h) as lab:
        expected, invalid = set(), {}
        for repetition in range(h.args.repeat):
            items = []
            for index, mutation in enumerate(('missing_id', 'unknown_station', 'missing_time', 'null_wave',
                                               'string_sea', 'string_critical', 'invalid_time')):
                valid = {'observation_id': f'{lab.name}-{repetition}-valid-{index}',
                    'station_id': 'contract-station', 'observed_at': '2097-01-01T00:00:00Z',
                    'sea_level_anomaly_m': .4, 'wave_height_m': 1.8, 'quality': 'ok', 'critical': index % 2 == 0}
                expected.add(hazard(valid['observation_id']))
                bad = copy.deepcopy(valid)
                bad['observation_id'] = f'{lab.name}-{repetition}-invalid-{index}'
                if mutation == 'missing_id':
                    del bad['observation_id']
                elif mutation == 'unknown_station':
                    bad['station_id'] = 'unknown-station'
                elif mutation == 'missing_time':
                    del bad['observed_at']
                elif mutation == 'null_wave':
                    bad['wave_height_m'] = None
                elif mutation == 'string_sea':
                    bad['sea_level_anomaly_m'] = 'not-a-number'
                elif mutation == 'string_critical':
                    bad['critical'] = 'true'
                else:
                    bad['observed_at'] = 'not-a-timestamp'
                invalid[(repetition, mutation)] = bad
                items.extend((bad, valid))
            lab.control('/admin/append', {'items': items})

            def snapshot():
                events = h.complete_audit('SENSOR_PANTAI')
                return events if expected <= {e['hazard_id'] for e in events} else None

            events = h.wait(snapshot, 'Valid neighbors of malformed observations did not reach the canonical store')
            h.check(len(events) == len(expected) and {e['hazard_id'] for e in events} == expected,
                    'Complete canonical Audit API contains every valid identity and no invalid observation')
            rows = h.sql("SELECT COALESCE(json_agg(row_to_json(x)),'[]'::json) FROM ("
                "SELECT message_id,reason,status,retryable,received_at,convert_from(raw_payload,'UTF8')::jsonb AS payload "
                "FROM unprocessed_messages WHERE source='SENSOR_PANTAI') x")
            for payload in invalid.values():
                matches = [r for r in rows if r['payload'] == payload]
                h.check(bool(matches), 'Each invalid observation is preserved intact')
                h.check(all(r['reason'] and r['received_at'] and r['status'] == 'parked' and not r['retryable'] for r in matches),
                        'Schema-invalid observations retain reason/time and do not enter a hot retry loop')
            h.artifact(f'round-{repetition+1}.json', {'valid': events, 'quarantined': rows})
            h.note({'round': repetition+1, 'canonical_valid': len(events), 'invalid_preserved': len(invalid),
                    'scope': 'Controlled contract violations through candidate Aggregator on VPS; not official-source coverage'})


if __name__ == '__main__':
    p = parser('Prove malformed coastal observations are parked intact while valid neighbors reach the canonical Audit API')
    p.add_argument('--notifier-image-tag', required=True)
    raise SystemExit(run('p5/10-contract', ['P5.2'], p, scenario))
