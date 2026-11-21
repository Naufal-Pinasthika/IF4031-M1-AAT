from pathlib import Path
import concurrent.futures
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from storage import StorageSandbox, fixture


def scenario(h):
    h.check(h.args.records >= 30, 'Use at least 30 records per import phase to exercise every primary')
    with StorageSandbox(h) as lab:
        h.wait(lab.ready, 'Fresh storage did not become ready')
        events = []
        for round in range(h.args.repeat):
            for skew in (False, True):
                batch = fixture(h.args.records, start=len(events), skew=skew)
                label = f'round-{round}-' + ('regional-burst' if skew else 'catalog')
                h.artifact(label + '-input.json', batch)
                lab.load(label + '-before')
                samples = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    loading = pool.submit(lab.import_events, batch)
                    while not loading.done():
                        samples.append({'observed_at': time.time(),
                            'pods': h.kube('top', 'pod', '-l', 'aat-e2e=' + lab.suffix),
                            'nodes': h.kube('top', 'nodes')})
                        time.sleep(1)
                    loading.result()
                h.artifact(label + '-during-load.json', samples)
                lab.load(label + '-after')
                events += batch
                physical = lab.physical()
                counts = [sum(r['partition_id'] == i for r in physical[name]) for i, name in enumerate(lab.stores)]
                h.check(all(0 < len(rows) < len(events) for rows in physical.values()),
                        'No physical unit contains the entire dataset')
                h.check(min(counts) > 0 and max(counts) / min(counts) < 2,
                        'Primary data is distributed despite strongly skewed geographic input')
                copies = {}
                for name, rows in physical.items():
                    for row in rows:
                        copies.setdefault(row['hazard_id'], set()).add(name)
                h.check(len(copies) == len(events) and all(len(units) == 2 for units in copies.values()),
                        'Every canonical identity has exactly two copies on separate nodes')
                h.artifact(label + '-physical.json', physical)
                h.note({'phase': label, 'total': len(events), 'primary_counts': counts})
        actual = lab.assert_dataset(events)
        for index, name in enumerate(lab.stores):
            event = next(e for e in actual if any(r['hazard_id'] == e['hazard_id'] and r['partition_id'] == index for r in physical[name]))
            code, location, _ = h.request('aggregator', '/internal/audit/location?hazard_id=' + event['hazard_id'],
                {'Authorization': 'Bearer ' + h.values['P1_AUDIT_TOKEN']})
            h.check(code == 200 and len(location['locations']) == 2, 'Audit API exposes both physical copies')
            h.check({p['role'] for p in location['locations']} == {'primary', 'replica'} and
                    len({p['node'] for p in location['locations']}) == 2, 'Primary and replica have distinct actual node identities')


if __name__ == '__main__':
    p = parser('Verify physical sharding, replication and geographically skewed imports')
    p.add_argument('--records', type=int, default=300)
    p.set_defaults(timeout=180)
    raise SystemExit(run('p1/01-sharding', ['P1.1', 'P1.2'], p, scenario))
