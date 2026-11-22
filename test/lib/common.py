import argparse
import base64
import datetime as dt
import json
import os
from pathlib import Path
import shlex
import shutil
import select
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]
NS = 'sistem-informasi-kebencanaan-bnpb'


class Failure(RuntimeError):
    pass


class Inconclusive(RuntimeError):
    pass


def parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument('--env-file', type=Path, default=ROOT / '.env')
    p.add_argument('--vps', action='store_true', help='Use private inventory, SSH tunnels and remote kubectl')
    p.add_argument('--ingress-vps', action='store_true', help='Originate NodePort connections on the external inventory load_generator, not a cluster host')
    p.add_argument('--kubeconfig', type=Path, help='Local kubectl configuration when not using --vps')
    p.add_argument('--client-url', default=os.getenv('TEST_CLIENT_URL', 'http://127.0.0.1:30080'))
    p.add_argument('--auth-url', default=os.getenv('TEST_AUTH_URL', 'http://127.0.0.1:30084'))
    p.add_argument('--aggregator-url', default=os.getenv('TEST_AGGREGATOR_URL', 'http://127.0.0.1:30083'))
    p.add_argument('--mock-url', default=os.getenv('TEST_PVMBG_URL', 'http://127.0.0.1:8082'))
    p.add_argument('--feed-url', default=os.getenv('TEST_SATELLITE_URL'))
    p.add_argument('--output', type=Path, help='New private evidence directory; default .e2e/report-tests/<case>/<run>')
    p.add_argument('--timeout', type=int, default=90, help='Bounded recovery/processing wait in seconds')
    p.add_argument('--repeat', type=int, default=3)
    p.add_argument('--image-tag', help='Use an already imported Aggregator image tag for isolated E2E workloads')
    p.add_argument('--allow-changes', action='store_true', help='Allow the explicitly documented mock/config/pod actions')
    return p


class Harness:
    def __init__(self, case, criteria, args):
        self.case, self.args = case, args
        self.run_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
        self.folder = args.output or ROOT / '.e2e/report-tests' / case / self.run_id
        self.report = {'case': case, 'run_id': self.run_id, 'criteria': criteria,
                       'status': 'failed', 'checks': [], 'requests': [], 'observations': [],
                       'started_at': dt.datetime.now(dt.timezone.utc).isoformat()}
        self.tokens, self.values, self.tunnel = {}, {}, None
        self.token_deadlines = {}
        self.hosts, self.server = None, None
        self.created = False
        self.extra_processes = []
        self.dashboard_pid = None

    def open(self):
        if self.args.repeat < 1 or self.args.timeout < 1:
            raise Failure('--repeat and --timeout must be positive')
        os.umask(0o077)
        self.folder.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.created = True
        self.curl = shutil.which('curl')
        if not self.curl:
            raise Failure('curl is required (Python standard library is the only Python dependency)')
        for line in self.args.env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith('export '):
                line = line[7:]
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                self.values[key] = value.strip().strip('\"\'')
        self.urls = {k: getattr(self.args, k + '_url').rstrip('/') for k in ('client', 'auth', 'aggregator', 'mock')}
        self.urls['feed'] = (self.args.feed_url or self.values.get('SATELLITE_URL', 'https://satellite.naufarrel.dev')).rstrip('/')
        if self.args.vps:
            sys.path.insert(0, str(ROOT / 'infra/vps'))
            from provision import Hosts, inventory
            self.hosts = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
            self.server = self.hosts.nodes[0]
            mock_ip = self.kube('get', 'service', 'pvmbg', '-o', 'jsonpath={.spec.clusterIP}').strip()
            targets = {'client': self.server['private_ip'] + ':30080',
                       'auth': self.server['private_ip'] + ':30084',
                       'aggregator': self.server['private_ip'] + ':30083', 'mock': mock_ip + ':8082'}
            command = self.hosts.command(self.server, '', multiplex=False)[:-2]
            ports = {}
            for name, target in targets.items():
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    ports[name] = sock.getsockname()[1]
                command += ['-L', f'127.0.0.1:{ports[name]}:{target}']
                self.urls[name] = f'http://127.0.0.1:{ports[name]}'
            command += ['-o', 'ExitOnForwardFailure=yes', '-N', self.server['user'] + '@' + self.server['host']]
            self.tunnel = subprocess.Popen(command)
            end = time.monotonic() + 20
            while True:
                if self.tunnel.poll() is not None:
                    raise Failure('SSH tunnel exited; check inventory, key and local ports')
                try:
                    with socket.create_connection(('127.0.0.1', ports['auth']), timeout=1):
                        break
                except OSError:
                    if time.monotonic() >= end:
                        raise Failure('SSH tunnel did not become available')
                    time.sleep(.2)
            if self.args.ingress_vps:
                origin = self.hosts.data.get('load_generator')
                self.check(bool(origin) and origin['host'] not in {n['host'] for n in self.hosts.nodes},
                           'Ingress observer originates outside every application cluster node')
                command = self.hosts.command(origin, '', multiplex=False)[:-2]
                for name in ('client', 'auth', 'aggregator'):
                    with socket.socket() as sock:
                        sock.bind(('127.0.0.1', 0))
                        port = sock.getsockname()[1]
                    command += ['-L', f'127.0.0.1:{port}:{targets[name]}']
                    self.urls[name] = f'http://127.0.0.1:{port}'
                command += ['-o', 'ExitOnForwardFailure=yes', '-N', origin['user'] + '@' + origin['host']]
                tunnel = subprocess.Popen(command)
                self.extra_processes.append(tunnel)
                time.sleep(.5)
                self.check(tunnel.poll() is None, 'External ingress SSH observer is active')
                self.artifact('ingress-origin.json', {'origin': origin['name'], 'targets': targets,
                    'scope': 'Same NodePorts and API assertions; TCP originates outside the cluster. The mock/control channel remains on the control-plane host.'})
        elif self.args.ingress_vps:
            raise Failure('--ingress-vps requires --vps')
        self.artifact('environment.json', {'urls': self.urls, 'via_vps': self.args.vps,
                                          'curl': subprocess.check_output([self.curl, '--version'], text=True).splitlines()[0]})
        self.artifact('deployment.json', {'pods': self.pods()})

    def artifact(self, name, data):
        path = self.folder / name
        path.write_text(json.dumps(data, indent=2, ensure_ascii=True) + '\n')
        return path

    def open_dashboard(self):
        self.check(bool(self.hosts), 'Private dashboard verification requires --vps')
        forward_command = f'k3s kubectl -n {NS} port-forward --address 127.0.0.1 service/observability :8090'
        command = 'sudo -n sh -c ' + shlex.quote("echo AAT_FORWARD_PID=$$,$(awk '{print $22}' /proc/$$/stat); exec " + forward_command)
        forward = subprocess.Popen(self.hosts.command(self.server, command, multiplex=False),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.extra_processes.append(forward)
        self.check(bool(select.select([forward.stdout], [], [], 20)[0]), 'Private dashboard port-forward became ready')
        pid_line = forward.stdout.readline().strip()
        self.check(pid_line.startswith('AAT_FORWARD_PID='), 'Dashboard forward has a recorded cleanup identity')
        pid, self.dashboard_started = pid_line.split('=')[1].split(',')
        self.dashboard_pid = int(pid)
        line = forward.stdout.readline()
        self.check(line.startswith('Forwarding from 127.0.0.1:'), 'Dashboard forward binds only remote loopback')
        remote_port = int(line.split(':')[1].split()[0])
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            local_port = sock.getsockname()[1]
        command = self.hosts.command(self.server, '', multiplex=False)[:-2]
        command += ['-L', f'127.0.0.1:{local_port}:127.0.0.1:{remote_port}', '-o', 'ExitOnForwardFailure=yes',
                    '-N', self.server['user'] + '@' + self.server['host']]
        tunnel = subprocess.Popen(command)
        self.extra_processes.append(tunnel)
        self.urls['dashboard'] = f'http://127.0.0.1:{local_port}'
        time.sleep(.5)
        self.check(tunnel.poll() is None, 'Private dashboard SSH tunnel is active')

    def note(self, data):
        self.report['observations'].append(data)
        print(json.dumps(data), flush=True)

    def check(self, condition, message):
        self.report['checks'].append({'check': message, 'passed': bool(condition)})
        if not condition:
            raise Failure(message)

    def changes(self):
        self.check(self.args.allow_changes, 'Mutating scenarios require --allow-changes; read the case README first')

    def request(self, service, path, headers=None, body=None, method='GET', private=False):
        correlation = self.case.replace('/', '-') + '-' + uuid.uuid4().hex
        headers = {'X-Correlation-ID': correlation, **(headers or {})}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        config = ['url = ' + json.dumps(self.urls[service] + path), 'request = ' + json.dumps(method)]
        config += ['header = ' + json.dumps(k + ': ' + v) for k, v in headers.items()]
        # Credentials live in curl's stdin, never argv, console, or evidence.
        # Auth response bodies are temporary and never retained as artifacts.
        with tempfile.TemporaryDirectory(prefix='aat-report-') as directory:
            response = Path(directory) / 'response'
            data_args = []
            if body is not None:
                body_file = Path(directory) / 'request.json'
                body_file.write_text(json.dumps(body), encoding='utf-8')
                data_args = ['--data-binary', '@' + str(body_file)]
            call = subprocess.run([self.curl, '--disable', '--silent', '--show-error', '--max-time', '20',
                                   '--config', '-', '--output', str(response), '--write-out',
                                   '{"status":%{http_code},"time_total":%{time_total}}', *data_args],
                                  input='\n'.join(config) + '\n', text=True, capture_output=True, timeout=25)
            if call.returncode:
                raise Failure(f'curl transport failure for {service}{path}: exit {call.returncode}')
            timing = json.loads(call.stdout)
            raw = response.read_bytes()
        record = {'service': service, 'method': method, 'path': path, **timing, 'correlation_id': correlation}
        if not private:
            name = f'{len(self.report["requests"]):04d}-{correlation.rsplit("-", 1)[-1]}-response.json'
            (self.folder / name).write_bytes(raw)
            record['response_file'] = name
        self.report['requests'].append(record)
        try:
            data = json.loads(raw)
        except ValueError:
            data = {'non_json_bytes': len(raw)}
        print(f'{method} {service}{path.split("?")[0]} HTTP {timing["status"]} time_total={timing["time_total"]:.6f}s', flush=True)
        return timing['status'], data, timing['time_total']

    def auth(self, role):
        credentials = self.values[role + '_CLIENT_ID'] + ':' + self.values[role + '_CLIENT_SECRET']
        header = {'Authorization': 'Basic ' + base64.b64encode(credentials.encode()).decode()}
        code, pair, _ = self.request('auth', '/v1/token', header, method='POST', private=True)
        self.check(code == 200, role + ' authentication succeeds')
        lifetime = pair['expires_in']
        self.check(lifetime > 0, 'Access token has a positive declared lifetime')
        self.tokens[role] = {'Authorization': 'Bearer ' + pair['access_token']}
        self.token_deadlines[role] = time.monotonic() + lifetime - min(5, lifetime / 10)

    def api(self, path, role='OPS', body=None, extra=None, method=None):
        if role not in self.tokens or time.monotonic() >= self.token_deadlines[role]:
            self.auth(role)
        method = method or ('POST' if body is not None else 'GET')
        result = self.request('client', path, {**self.tokens[role], **(extra or {})}, body, method)
        if result[0] == 401:
            self.auth(role)
            result = self.request('client', path, {**self.tokens[role], **(extra or {})}, body, method)
        return result

    def unprocessed(self):
        result, cursor, seen = [], None, set()
        for _ in range(1000):
            path = '/unprocessed?' + urllib.parse.urlencode({'source': 'PVMBG', 'limit': 1000, **({'cursor': cursor} if cursor else {})})
            code, page, _ = self.api(path)
            self.check(code == 200, 'Ops can enumerate unprocessed PVMBG messages')
            result.extend(page['items'])
            cursor = page.get('next_cursor')
            if not cursor:
                return result
            self.check(cursor not in seen, 'Unprocessed pagination advances')
            seen.add(cursor)
        raise Failure('Unprocessed pagination exceeded 1000 pages')

    def audit(self, source, retry_unavailable=False):
        events, cursor, cursors, identities = [], None, set(), set()
        for _ in range(1000):
            query = {'source': source, 'limit': 1000, **({'cursor': cursor} if cursor else {})}
            code, page, _ = self.request('aggregator', '/internal/audit/events?' + urllib.parse.urlencode(query),
                                         {'Authorization': 'Bearer ' + self.values['P1_AUDIT_TOKEN']})
            if retry_unavailable and code == 503:
                raise Inconclusive('Canonical snapshot is temporarily unavailable while replicas initialize/recover')
            self.check(code == 200, 'Development Audit API is accessible')
            if page.get('partial'):
                raise Inconclusive('Audit snapshot is partial; complete comparison cannot be established')
            for event in page['items']:
                identity = (event['source'], event['source_ref_id'])
                if identity in identities:
                    raise Inconclusive('An identity appeared on multiple live pages; capture a stable snapshot before concluding duplicate storage')
                identities.add(identity)
                events.append(event)
            cursor = page.get('next_cursor')
            if not cursor:
                return events
            self.check(cursor not in cursors, 'Audit pagination advances')
            cursors.add(cursor)
        raise Failure('Audit pagination exceeded 1000 pages')

    def session(self):
        code, session, _ = self.request('feed', '/v1/session', {'Authorization': 'Bearer ' + self.values['SATELLITE_TOKEN']})
        self.check(code == 200, 'Configured satellite session is accessible')
        return session

    def complete_audit(self, source):
        def snapshot():
            try:
                return {'events': self.audit(source, retry_unavailable=True)}
            except Inconclusive as error:
                self.note({'awaiting_complete_audit': str(error)})
                return None
        return self.wait(snapshot, 'Canonical replicas did not recover to a complete audit snapshot')['events']

    def satellite(self):
        code, state, _ = self.api('/satellite-status')
        self.check(code == 200, 'Satellite status is accessible')
        return state

    def raw_message(self, message_id):
        raw = bytearray()
        for chunk in range(4096):
            code, part, _ = self.api('/unprocessed/' + urllib.parse.quote(message_id, safe='') + f'?chunk={chunk}')
            self.check(code == 200, 'Ops can inspect retained raw bytes')
            raw.extend(base64.b64decode(part['payload_base64']))
            if not part['has_more']:
                return bytes(raw), part
        raise Failure('Raw-message chunk bound exceeded')

    def schema(self, mode):
        code, _, _ = self.request('mock', '/admin/schema-version',
                                  {'Authorization': 'Bearer ' + self.values['PVMBG_TOKEN']}, {'mode': mode}, 'POST')
        self.check(code == 200, 'PVMBG mock mode set to ' + mode)

    def adapter(self, version):
        self.kube('patch', 'configmap', 'aggregator-config', '--type=merge', '-p',
                  json.dumps({'data': {'PVMBG_ADAPTER_VERSION': version}}))
        self.kube('rollout', 'restart', 'deployment/aggregator')
        self.kube('rollout', 'status', 'deployment/aggregator', '--timeout=180s')

    def wait(self, predicate, message):
        end = time.monotonic() + self.args.timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() >= end:
                raise Failure(message)
            time.sleep(2)

    def kube(self, *args):
        command = ['kubectl', '-n', NS, *args]
        if self.hosts:
            return self.hosts.ssh(self.server, 'sudo -n k3s ' + shlex.join(command), capture=True).decode()
        if self.args.kubeconfig:
            command[1:1] = ['--kubeconfig', str(self.args.kubeconfig)]
        return subprocess.check_output(command, text=True, timeout=210)

    def sql(self, sql):
        raw = self.kube('exec', 'aggregator-coordinator-0', '--', 'psql', '-U', 'aggregator_coord',
                        '-d', 'coordination', '-At', '-c', sql)
        return json.loads(raw)

    def pods(self):
        pods = json.loads(self.kube('get', 'pods', '-o', 'json'))['items']
        return {p['metadata']['uid']: {'name': p['metadata']['name'], 'app': p['metadata']['labels'].get('app'),
                                      'node': p['spec'].get('nodeName'),
                                      'ready': any(c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', [])),
                                      'restarts': sum(c['restartCount'] for c in p['status'].get('containerStatuses', [])),
                                      'images': [c['image'] for c in p['spec']['containers']],
                                      'dns_config': p['spec'].get('dnsConfig', {}),
                                      'resources': {c['name']: c.get('resources', {}) for c in p['spec']['containers']}}
                for p in pods if not p['metadata'].get('deletionTimestamp')}

    def restart_receiver(self):
        owner = self.sql("SELECT to_json(holder_id) FROM source_leases WHERE source='SATELIT'")
        self.check(bool(owner), 'Satellite stream has a recorded owner')
        pod = json.loads(self.kube('get', 'pod', owner, '-o', 'json'))
        self.check(pod['metadata']['labels'].get('app') == 'aggregator', 'Fault target is an Aggregator pod')
        self.note({'fault': 'kill_receiver_pod', 'pod': owner, 'node': pod['spec']['nodeName']})
        self.kube('delete', 'pod', owner, '--grace-period=0', '--force', '--wait=false')
        self.kube('rollout', 'status', 'deployment/aggregator', '--timeout=180s')

    def close(self):
        if self.dashboard_pid:
            cleanup = ("from pathlib import Path\nimport os,signal,sys\npid=int(sys.argv[1])\n"
                       "p=Path('/proc/'+str(pid)+'/cmdline')\n"
                       "if p.exists():\n"
                       " cmd=p.read_bytes().replace(b'\\x00',b' ').decode()\n"
                       " started=Path('/proc/'+str(pid)+'/stat').read_text().split()[21]\n"
                       " if cmd.strip()!='kubectl' or started!=sys.argv[2]: raise RuntimeError('Refuse unrelated process cleanup')\n"
                       " os.kill(pid,signal.SIGTERM)\n")
            self.hosts.ssh(self.server, 'sudo -n python3 - ' + str(self.dashboard_pid) + ' ' + self.dashboard_started, cleanup.encode(), capture=True)
        for process in reversed(self.extra_processes):
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if self.tunnel:
            self.tunnel.terminate()
            try:
                self.tunnel.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.tunnel.kill()
                self.tunnel.wait()
        if self.created:
            self.report['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
            self.artifact('result.json', self.report)
            print('Evidence:', self.folder, flush=True)


def run(case, criteria, p, function):
    args = p.parse_args()
    h = Harness(case, criteria, args)
    try:
        h.open()
        h.report['status'] = function(h) or 'passed'
        return 0
    except Inconclusive as error:
        h.report.update(status='inconclusive', error=str(error))
        print('INCONCLUSIVE:', error, file=sys.stderr)
        return 2
    except (Exception, KeyboardInterrupt) as error:
        h.report.update(status='failed', error=str(error))
        print('FAILED:', error, file=sys.stderr)
        return 1
    finally:
        h.close()
