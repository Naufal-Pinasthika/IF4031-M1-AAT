from pathlib import Path
import concurrent.futures
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from storage import StorageSandbox, fixture, verify_analytics


def read_during_fault(h, lab):
    code, page, elapsed = h.request('aggregator', '/internal/events?limit=50',
                                  {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']})
    h.check(code == 200 and elapsed < 5, 'Read stays bounded and never returns HTTP 500 during recovery')
    h.check(not page['partial'] or bool(page['unavailable_units']), 'Incomplete reads explicitly name unavailable units')
    code, analytics, elapsed = h.api('/analytics')
    h.check(code == 200 and elapsed < 5, 'Dashboard data remains accessible during storage loss')
    h.check(not analytics['partial'] or bool(analytics['unavailable_units']), 'Dashboard flags incomplete numbers')
    return page, analytics


def scenario(h):
    with StorageSandbox(h) as lab:
        h.wait(lab.ready, 'Initial replicas did not become ready')
        events = fixture(600)
        lab.import_events(events)
        original = lab.assert_dataset(events)
        h.artifact('initial-audit.json', original)
        for round in range(h.args.repeat):
            index = 1 if h.args.node_fault else 1 + round % 2
            def inject():
                lab.erase(index)
                if h.args.node_fault:
                    lab.stop_worker(index)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fault = pool.submit(inject)
                while not fault.done():
                    read_during_fault(h, lab)
                    time.sleep(1)
                fault.result()
            saw_partial, saw_node_down, requests = False, False, 0
            pending, during = [], []
            started = time.monotonic()
            while True:
                page, summary = read_during_fault(h, lab)
                saw_partial |= page['partial'] and summary['partial']
                if h.args.node_fault:
                    ready = lab.worker_ready(index)
                    saw_node_down |= not ready
                batch = fixture(1, start=len(events) + len(pending), skew=True)
                during += lab.import_events(batch, require_success=False)
                pending += batch
                requests += 2
                elapsed = time.monotonic() - started
                if not h.args.node_fault and elapsed >= 12 or h.args.node_fault and elapsed >= 85 and ready:
                    break
                h.check(elapsed < 120, 'Worker did not automatically return within the declared 120-second bound')
                time.sleep(1)
            h.check(saw_partial, 'Lost-unit reads and dashboard explicitly become partial')
            if h.args.node_fault:
                h.check(saw_node_down and lab.worker_ready(index), 'Real worker became NotReady and restarted automatically')
            degraded_audit = lab.available_audit()
            available = {e['hazard_id']: e for e in degraded_audit}
            h.check(all(available.get(e['hazard_id']) == e for e in original),
                    'Every pre-fault record remains unchanged and readable from its surviving replica')
            h.check(verify_analytics(h, degraded_audit)['partial'],
                    'Degraded analytical counts match available Audit API data and remain visibly partial')
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                replacement = pool.submit(lab.replace, index)
                while not replacement.done():
                    read_during_fault(h, lab)
                    batch = fixture(5, start=len(events) + len(pending), skew=True)
                    during += lab.import_events(batch, require_success=False)
                    pending += batch
                    requests += 2
                    time.sleep(1)
                replacement.result()
            h.wait(lab.ready, 'Empty replacement did not automatically recover')
            original_ids = {e['hazard_id'] for e in original}
            def originals_replicated():
                rows = lab.physical()
                copies = {}
                for name, records in rows.items():
                    for record in records:
                        copies.setdefault(record['hazard_id'], set()).add(name)
                return rows if all(len(copies.get(key, set())) == 2 for key in original_ids) else None
            h.artifact(f'round-{round}-before-write-retries.json',
                       h.wait(originals_replicated, 'Previously acknowledged records did not automatically regain both replicas'))
            recovered = h.complete_audit('')
            by_id = {e['hazard_id']: e for e in recovered}
            h.check(all(by_id.get(e['hazard_id']) == e for e in original),
                    'Every previously acknowledged event is unchanged before any write retry')
            lab.import_events(pending)
            events += pending
            def replicated():
                if not lab.ready():
                    return None
                rows = lab.physical()
                copies = {}
                for name, records in rows.items():
                    for record in records:
                        copies.setdefault(record['hazard_id'], set()).add(name)
                return rows if len(copies) == len(events) and all(len(v) == 2 for v in copies.values()) else None
            physical = h.wait(replicated, 'Restored system did not regain two copies of every record')
            actual = lab.assert_dataset(events)
            original = actual
            verify_analytics(h, actual)
            h.artifact(f'round-{round}-physical.json', physical)
            h.artifact(f'round-{round}-writes-during-recovery.json', during)
            h.check(any(r['body']['accepted'] > 0 for r in during), 'Writes continued while the missing unit was replaced')
            h.note({'round': round, 'lost_unit': lab.stores[index], 'fault': 'worker-and-temporary-disk' if h.args.node_fault else 'temporary-storage-disk',
                    'recovery_read_requests': requests, 'records_after_recovery': len(actual),
                    'full_replication_restored': True, 'assistant_checksum_used': False})


if __name__ == '__main__':
    p = parser('Verify repeated empty-disk replacement, live reads/writes and restored replication')
    p.add_argument('--node-fault', action='store_true', help='Stop worker B for 70s with automatic restart; affects its real workloads')
    p.set_defaults(timeout=240)
    raise SystemExit(run('p1/03-recovery', ['P1.4', 'P1.5', 'P1.6', 'P1.7'], p, scenario))
