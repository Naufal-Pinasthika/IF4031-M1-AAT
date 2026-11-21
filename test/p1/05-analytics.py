from pathlib import Path
import sys
import urllib.parse
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    timings = []
    for attempt in range(h.args.repeat):
        for bounds in ({}, {'from': '2025-01-01T00:00:00Z', 'to': '2025-01-31T23:59:59Z'}):
            query = urllib.parse.urlencode(bounds)
            code, data, elapsed = h.api('/analytics' + ('?' + query if query else ''))
            h.check(code == 200, 'Live analytical endpoint responds successfully')
            h.check(elapsed < 5, 'Analytical response fits the unchanged five-second gateway budget')
            h.check(data['partial'] is False, 'Healthy live storage produces explicitly complete analytics')
            h.check(set(data['by_hazard']) == {'SEISMIC', 'TSUNAMI', 'VOLCANIC', 'WILDFIRE', 'COASTAL'},
                    'All five hazard series are present, including zero counts')
            for name in ('by_hazard', 'by_severity', 'by_area'):
                h.check(sum(data[name].values()) == data['total'], name + ' reconciles with the total')
            h.check(sum(sum(day['counts'].values()) for day in data['daily']) == data['total'],
                    'Daily UTC buckets reconcile with the total')
            days = [day['date'] for day in data['daily']]
            h.check(days == sorted(set(days)), 'UTC daily buckets are ordered and unique')
            if bounds:
                h.check(all('2025-01-01' <= day <= '2025-01-31' for day in days),
                        'Filtered UTC buckets lie within the requested range')
            timings.append({'attempt': attempt, 'bounds': bounds, 'seconds': elapsed, 'total': data['total']})
    h.artifact('analytics-timings.json', timings)
    h.check(h.request('client', '/dashboard')[0] == 200, 'Dashboard HTML remains available')
    h.note({'writes_performed': False, 'browser_charts': 'manual verification required',
            'exact_audit_comparison': 'Separate 02-queries scenario uses an isolated stable dataset; this scenario checks the live request budget and response integrity'})


if __name__ == '__main__':
    p = parser('Verify repeated live analytical response latency and aggregate integrity without changing VPS data')
    raise SystemExit(run('p1/05-analytics', ['P1.7'], p, scenario))
