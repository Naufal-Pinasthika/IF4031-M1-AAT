from pathlib import Path
import json
import socket
import subprocess
import sys
import time
import urllib.parse
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run, Inconclusive, Failure
from coastal import hazard, p95, valid_observation
from coastal_sandbox import CoastalSandbox

IMAGE = 'sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d'


class OfficialSource(CoastalSandbox):
    def configure(self, config):
        super().configure(config)
        config.update(COASTAL_URL='http://' + self.name + '-official:8080', COASTAL_MAX_STALENESS_MS='30000',
                      COASTAL_HTTP_TIMEOUT_MS='1000', COASTAL_POLL_INTERVAL_MS='500')
        source, service = self.deployment(self.name + '-official', 'sensor', IMAGE, 8080,
            [{'name': 'PANTAI_PROFILE', 'value': 'practice'}, {'name': 'PANTAI_SEED', 'value': '42'},
             {'name': 'PANTAI_SPEED', 'value': '60'},
             {'name': 'PANTAI_AUTH_TOKEN', 'valueFrom': {'secretKeyRef': {'name': self.name, 'key': 'PANTAI_AUTH_TOKEN'}}}],
            volumeMounts=[{'name': 'data', 'mountPath': '/data'}])
        source['spec']['template']['spec']['containers'][0]['readinessProbe']['httpGet']['path'] = '/healthz'
        source['spec']['template']['spec']['volumes'] = [{'name': 'data', 'emptyDir': {}}]
        self.apply([source, service])


def scenario(h):
    h.check(h.args.seconds > 0, 'Observation windows have positive length')
    outcomes = []
    with OfficialSource(h) as lab:
        name = lab.name + '-official'
        h.kube('rollout', 'status', 'deployment/' + name, '--timeout=180s')
        fresh_ip = h.kube('get', 'service', name, '-o', 'jsonpath={.spec.clusterIP}').strip()
        ports = []
        command = h.hosts.command(h.server, '', multiplex=False)[:-2]
        for label, target in [('fresh', fresh_ip+':8080'), ('long_running', h.server['private_ip']+':30090')]:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            command += ['-L', f'127.0.0.1:{port}:{target}']
            h.urls[label] = 'http://127.0.0.1:'+str(port)
            ports.append(port)
        command += ['-o', 'ExitOnForwardFailure=yes', '-N', h.server['user']+'@'+h.server['host']]
        tunnel = subprocess.Popen(command)
        h.extra_processes.append(tunnel)
        time.sleep(.5)
        h.check(tunnel.poll() is None, 'Both official-source observation tunnels are active')
        before = h.pods()
        prod_uid, prod = next((uid, p) for uid, p in before.items() if p['app'] == 'sensor-pantai')
        h.note({'long_running_source': prod, 'clock_age': 'Unknown via contract; no state file is read or modified',
                'fresh_source': name, 'image': IMAGE, 'speed_dependency_in_application': False})
        observer_cursor, last_empty = '', None
        seen_critical = {}
        code, station_page, _ = h.request('fresh', '/v1/stations', {'Authorization': 'Bearer '+lab.token})
        h.check(code == 200, 'Fresh official-source station contract is available')
        stations = {item['station_id']: item for item in station_page['items']}
        # Do not add a second observations poller to the unhealthy production
        # source. Its contract health and existing poll-ledger record suffice.
        for repetition in range(h.args.repeat):
            deadline = time.monotonic()+h.args.seconds
            samples = []
            while time.monotonic() < deadline:
                row = {'at': time.time()}
                for label in ('fresh', 'long_running'):
                    try:
                        code, body, elapsed = h.request(label, '/healthz')
                        row[label] = {'status': code, 'seconds': elapsed}
                    except Failure as error:
                        row[label] = {'error': str(error)}
                # Bound availability only after actually reaching an empty
                # page, never by assuming a short page is the tail. A single
                # request per slow sampling loop can forever trail live data.
                # This observer is separate from application polling, limited
                # to three successful pages per sample and never retries errors.
                for _ in range(3):
                    request_started = time.monotonic()
                    code, page, _ = h.request('fresh', '/v1/observations?'+urllib.parse.urlencode(
                        {'limit': 1000, **({'cursor': observer_cursor} if observer_cursor else {})}),
                        {'Authorization': 'Bearer '+lab.token})
                    if code != 200 or not isinstance(page, dict) or not isinstance(page.get('items'), list):
                        break
                    for item in page['items']:
                        if valid_observation(item, stations) and item['critical'] and item['observation_id'] not in seen_critical:
                            seen_critical[item['observation_id']] = {'lower_bound': last_empty, 'round': repetition+1}
                    if page.get('next_cursor'):
                        observer_cursor = page['next_cursor']
                    if not page['items']:
                        last_empty = request_started
                        break
                    if not page.get('next_cursor'):
                        break
                for ref, critical in seen_critical.items():
                    if critical.get('confirmed'):
                        continue
                    code, receipt, _ = h.request('p5notifier', '/notifications/confirm', body={'hazard_id': hazard(ref)}, method='POST')
                    if code == 200 and receipt.get('durability_confirmed') and receipt['notification']['critical']:
                        critical['confirmed'] = True
                        critical['upper_bound_seconds'] = time.monotonic()-critical['lower_bound'] if critical['lower_bound'] is not None else None
                    else:
                        h.check(code == 404, 'Critical receipt is absent or successfully durable, never an ignored server error')
                row['accepted'] = h.sql("SELECT count(*)::int FROM ingestion_outbox WHERE source='SENSOR_PANTAI'")
                code, stats, elapsed = h.request('p5notifier', '/stats')
                row['notifier'] = {'status': code, 'seconds': elapsed, 'stats': stats}
                samples.append(row)
                time.sleep(.5)
            pods = h.pods()
            fresh = next(p for p in pods.values() if p['app'] == name)
            long = pods.get(prod_uid)
            code, notifications, _ = h.request('p5notifier', '/notifications?limit=1000')
            critical = [n for n in notifications.get('notifications', []) if n.get('critical')]
            upper_bounds = [c['upper_bound_seconds'] for c in seen_critical.values()
                            if c['round'] == repetition+1 and c.get('upper_bound_seconds') is not None]
            outcome = {'round': repetition+1, 'samples': samples, 'fresh_source': fresh,
                       'long_running_source': long, 'observed_critical_notifications': len(critical),
                       'critical_observer': dict(seen_critical), 'new_latency_samples': len(upper_bounds),
                       'critical_p95_upper_bound_seconds': p95(upper_bounds),
                       'scope': 'Fresh official source uses isolated candidate pipeline; long-running production source is observed read-only'}
            outcomes.append(outcome)
            h.artifact('source-age.json', outcomes)
            h.check(fresh['restarts'] == 0, 'Fresh official source did not restart during the window')
            h.check(all(s['notifier']['status'] == 200 for s in samples), 'Candidate notifier remains responsive during source comparison')
            if upper_bounds:
                if p95(upper_bounds) > 5:
                    raise Inconclusive('Observed latency upper bound exceeds five seconds; exact source availability is unknown, so this does not prove either an SLO pass or violation')
                h.check(True, 'Fresh official-source conservative critical latency p95 is at most five seconds')
        h.check(all(c.get('confirmed') for c in seen_critical.values()), 'Every critical identity observed from the fresh official source is durably delivered')
        if not all(x['new_latency_samples'] for x in outcomes):
            raise Inconclusive('At least one observation window had no bounded new critical sample; retain collected samples but do not claim a latency result for every window')
        if any(not x['long_running_source']['ready'] for x in outcomes):
            raise Inconclusive('Fresh official-source critical latency passed, but recovery of the long-running source was not observed')
        return 'observed'


if __name__ == '__main__':
    p = parser('Compare a fresh immutable official source with the existing long-running source without reading or resetting source state')
    p.add_argument('--notifier-image-tag', required=True)
    p.add_argument('--seconds', type=int, default=60)
    raise SystemExit(run('p5/07-source-age', ['P5.1 (observation)', 'P5.4 (observation)'], p, scenario))
