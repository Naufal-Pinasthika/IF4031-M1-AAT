from pathlib import Path
import sys
import urllib.parse
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from storage import StorageSandbox, fixture, verify_analytics


def scenario(h):
    with StorageSandbox(h) as lab:
        h.wait(lab.ready, 'Fresh storage did not become ready')
        events = fixture(600)
        lab.import_events(events)
        audit = lab.assert_dataset(events)
        expected = sorted(audit, key=lambda e: (e['occurred_at'], e['hazard_id']), reverse=True)
        for round in range(h.args.repeat):
            result, cursor, seen = [], None, set()
            while True:
                query = {'limit': 50, **({'cursor': cursor} if cursor else {})}
                code, page, _ = h.request('aggregator', '/internal/events?' + urllib.parse.urlencode(query),
                    {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']})
                h.check(code == 200 and not page['partial'], 'Healthy cross-shard page is complete')
                result.extend(page['events'])
                cursor = page.get('next_cursor')
                if cursor is None:
                    break
                h.check(cursor not in seen, 'Pagination cursor makes progress')
                seen.add(cursor)
            h.check(result == expected, 'All 50-item pages have correct global order with no missing or duplicate event')
            summary = verify_analytics(h, audit)
            h.check(not summary['partial'], 'Healthy analytical totals are labelled complete')
            query = urllib.parse.urlencode({'from': '2025-01-04T00:00:00Z', 'to': '2025-01-07T23:59:59Z'})
            verify_analytics(h, [e for e in audit if '2025-01-04' <= e['occurred_at'][:10] <= '2025-01-07'], query)
            h.artifact(f'round-{round}-analytics.json', summary)
        code, _, _ = h.request('client', '/dashboard')
        h.check(code == 200, 'Analytical dashboard HTML is served by Client API')


if __name__ == '__main__':
    p = parser('Verify global pagination and all analytical aggregates against Audit API')
    p.set_defaults(timeout=180)
    raise SystemExit(run('p1/02-queries', ['P1.3', 'P1.7'], p, scenario))
