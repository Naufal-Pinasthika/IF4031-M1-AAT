import gzip
import json
from pathlib import Path
import shlex
import sys
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run


def scenario(h):
    with gzip.open(h.args.baseline, 'rt') as stream:
        baseline = {line.rstrip('\n') for line in stream}
    h.check(bool(baseline), 'A retained pre-fault identity baseline is required')
    for repetition in range(h.args.repeat):
        cursor, cursors, identities = None, set(), set()
        for number in range(2000):
            query = urllib.parse.urlencode({'limit': 1000, **({'cursor': cursor} if cursor else {})})
            code, page, _ = h.request('aggregator', '/internal/audit/events?'+query,
                {'Authorization': 'Bearer '+h.values['P1_AUDIT_TOKEN']}, private=True)
            if code != 200 or page.get('partial') is not False:
                h.artifact('round-'+str(repetition+1)+'-failed-page.json', page)
                if h.hosts:
                    filter_code = "import sys; [print(x.rstrip()) for x in sys.stdin if 'canonical_read_partial' in x or 'canonical_read_query' in x]"
                    logs = h.hosts.ssh(h.server, f'sudo -n k3s kubectl -n {NS} logs -l app=aggregator '
                        '--prefix=true --max-log-requests=5 --since=30s --tail=50000 | python3 -c '+shlex.quote(filter_code), capture=True)
                    (h.folder/'canonical-diagnostics.log').write_bytes(logs)
            h.check(code == 200 and page.get('partial') is False,
                    'Every Audit page is complete; a partial page is retained and fails the run')
            for event in page['items']:
                if event['source'] != 'SATELIT':
                    identities.add(event['source']+'\t'+event['source_ref_id']+'\t'+event['hazard_id'])
            cursor = page.get('next_cursor')
            if not cursor:
                break
            h.check(cursor not in cursors, 'Audit pagination advances without a repeated cursor')
            cursors.add(cursor)
        else:
            h.check(False, 'Audit enumeration must terminate within 2,000 pages')
        missing = sorted(baseline-identities)
        h.artifact('round-'+str(repetition+1)+'-missing.json', missing)
        h.note({'round': repetition+1, 'pages': number+1, 'baseline_identities': len(baseline),
                'current_non_satellite_identities': len(identities), 'missing': len(missing)})
        h.check(not missing, 'Every retained pre-wipe identity is visible through complete Audit pages')


if __name__ == '__main__':
    p = parser('Read-only repeated Audit comparison after autonomous store recovery')
    p.add_argument('--baseline', type=Path, required=True, help='Private pre-fault identities.txt.gz; excludes retractable satellite fires')
    raise SystemExit(run('tester-agent/10-audit-recovery', ['Post-wipe API completeness and baseline identity preservation'], p, scenario))
