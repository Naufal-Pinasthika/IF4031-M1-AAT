from pathlib import Path
import math
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from sandbox import Sandbox
from wildfire import fixture, verify


def settled(h, last):
    state = h.satellite()
    return state if state['committed_offset'] == last and state['pending_projections'] == 0 else None


def process(h, events, fault, label):
    initial, changed, complete = events
    with Sandbox(h, satellite=True) as lab:
        lab.control('/admin/feed', {'events': initial})
        state = h.wait(lambda: settled(h, len(initial) - 1), 'Initial fires did not reach canonical storage')
        first = h.complete_audit('SATELIT')
        verify(h, initial, first, state)
        lab.control('/admin/feed', {'events': changed})
        state = h.wait(lambda: settled(h, len(changed) - 1), 'Update/retract did not reach canonical storage')
        updated = h.complete_audit('SATELIT')
        verify(h, changed, updated, state)
        old = next(e for e in first if e['attributes']['detection_count'] == 3)
        new = next(e for e in updated if e['attributes']['detection_count'] == 3)
        h.check(old['source_ref_id'] == new['source_ref_id'] and old['hazard_id'] == new['hazard_id'],
                'An updated fire retains its canonical identity; the retracted fire disappears')
        lab.control('/admin/feed', {'events': complete, 'cut_every': 137 if fault else 0, 'delay': .001})
        observations, killed, finished = [], False, None
        end = time.monotonic() + h.args.timeout
        while True:
            state, source = h.satellite(), lab.control('/admin/status')
            lease = h.sql("SELECT row_to_json(r) FROM (SELECT holder_id,fencing_token,lease_until,clock_timestamp() AS observed_at "
                          "FROM source_leases WHERE source='SATELIT') r")
            observations.append({'state': state, 'source': source, 'lease': lease})
            h.artifact(label + '-observations.json', observations)
            if fault and not killed and state['committed_offset'] > len(changed) + 5:
                lab.kill_owner()
                killed = True
                h.note({'run': label, 'fault': 'owner_killed_during_fire_recognition', 'offset': state['committed_offset']})
            if source['ended_at'] is not None and finished is None:
                finished = time.monotonic()
            if state['committed_offset'] == len(complete) - 1 and state['pending_projections'] == 0:
                h.check(finished is not None, 'The source recorded the end of its finite burst')
                elapsed = source['observed_at'] - source['ended_at']
                h.check(elapsed <= 60, 'The system is current within 60 seconds of the controlled 60x burst ending')
                break
            h.check(source['ended_at'] is None or source['observed_at'] - source['ended_at'] <= 60,
                    'Catch-up exceeded 60 seconds after burst end')
            h.check(time.monotonic() < end, 'Stream processing did not complete within the bounded test window')
            time.sleep(1)
        h.check(not fault or killed, 'Failure was injected while the stream was in progress')
        connections = source['connections']
        h.check(not fault or len(connections) >= 3, 'Fault run exercised both stream reconnection and owner recovery')
        actual = h.complete_audit('SATELIT')
        normalized = verify(h, complete, actual, state)
        h.check(state['latency_samples'] > 0 and state['p95_ingress_to_canonical_ms'] is not None,
                'Real ingress-to-canonical p95 samples are present')
        acknowledgements = h.sql("SELECT COALESCE(json_agg(r),'[]'::json) FROM ("
            "SELECT split_part(o.correlation_id,'-',2)::bigint AS offset,extract(epoch FROM max(p.delivered_at)) AS completed_at "
            "FROM ingestion_outbox o JOIN ingestion_projections p USING(version) WHERE o.source='SATELIT' "
            "AND p.target LIKE 'canonical%' GROUP BY o.version,o.correlation_id) r")
        latencies = sorted(1000 * (float(r['completed_at']) - source['burst_started_at']
                           - max(0, r['offset'] - source['prefix']) * source['interval_seconds'])
                           for r in acknowledgements if r['offset'] >= source['prefix'])
        h.check(bool(latencies), 'Controlled source-availability-to-canonical delivery samples are present')
        source_p95 = latencies[math.ceil(.95 * len(latencies)) - 1]
        h.artifact(label + '-source-to-canonical-ms.json', latencies)
        h.artifact(label + '-observations.json', observations)
        h.artifact(label + '-canonical.json', actual)
        h.note({'run': label, 'frames': len(complete), 'fires': len(actual), 'connections': len(connections),
                'catchup_seconds_upper_bound': elapsed, 'p95_ingress_to_canonical_ms': state['p95_ingress_to_canonical_ms'],
                'p95_source_available_to_canonical_ms': source_p95})
        return normalized


def scenario(h):
    h.check(h.args.groups > 0 and h.args.members >= 2, 'Burst contains positive groups with at least two detections each')
    for iteration in range(h.args.repeat):
        events = fixture(4300 + iteration, h.args.groups, h.args.members)
        h.artifact(f'round-{iteration}-controlled-input.json', events[2])
        baseline = process(h, events, False, f'round-{iteration}-baseline')
        recovered = process(h, events, True, f'round-{iteration}-fault')
        h.check(baseline == recovered, 'Fault recovery and no-fault processing have identical canonical results and identities')
        h.note({'round': iteration, 'baseline_equals_fault': True})


if __name__ == '__main__':
    p = parser('Verify complete stream results, event-time semantics, burst catch-up and failure recovery')
    p.add_argument('--groups', type=int, default=80)
    p.add_argument('--members', type=int, default=20)
    p.set_defaults(timeout=300)
    raise SystemExit(run('p4/02-recovery', ['P4.1', 'P4.2', 'P4.3', 'P4.4', 'P4.5', 'P4.8'], p, scenario))
