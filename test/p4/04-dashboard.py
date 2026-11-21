from pathlib import Path
import math
import sys
import time
import urllib.parse
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    h.open_dashboard()
    h.check('dashboard' in h.urls, 'Private VPS dashboard Service is available')
    code, _, _ = h.request('dashboard', '/')
    h.check(code == 200, 'Operations dashboard HTML is accessible through the private SSH tunnel')
    html = (h.folder / h.report['requests'][-1]['response_file']).read_text()
    h.check('id="satellite"' in html and 'streamCard' in html, 'Dashboard renders a satellite stream panel')
    names = ('lag_offsets', 'pending_projections', 'p95_ingress_to_canonical_ms',
             'late_detections', 'singleton_detections', 'latency_samples')
    query = 'max by(__name__)({__name__=~"bnpb_satellite_(' + '|'.join(names) + ')"})'
    for iteration in range(h.args.repeat):
        code, body, _ = h.request('dashboard', '/api/query?' + urllib.parse.urlencode({'query': query}))
        h.check(code == 200 and body.get('status') == 'success', 'The dashboard executes its real Prometheus query')
        samples = {r['metric']['__name__']: float(r['value'][1]) for r in body['data']['result']}
        h.check(all('bnpb_satellite_' + name in samples for name in names), 'Every displayed satellite measurement has a scraped series')
        h.check(all(math.isfinite(value) and value >= 0 for value in samples.values()), 'Lag and latency panel values are measured, finite and nonnegative')
        h.check(samples['bnpb_satellite_latency_samples'] > 0, 'Latency display is backed by actual canonical-delivery samples')
        h.note({'iteration': iteration, 'satellite_panel': samples})
        if iteration + 1 < h.args.repeat:
            time.sleep(6)


if __name__ == '__main__':
    raise SystemExit(run('p4/04-dashboard', ['P4.4'], parser('Verify the live dashboard exposes measured stream lag and latency'), scenario))
