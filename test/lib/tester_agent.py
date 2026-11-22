from concurrent.futures import ThreadPoolExecutor
import json
import re
import shlex
import subprocess
import threading
import time

from common import Failure


def logs(h, tolerate_unavailable=False):
    try:
        raw = h.kube('-n', 'tester-agent', '--request-timeout=10s', 'logs', '-l', 'app=tester-agent',
                     '--prefix', '--tail=400', '--max-log-requests=3', '--ignore-errors=true')
    except subprocess.CalledProcessError:
        if not tolerate_unavailable:
            raise
        return 'Agent logs temporarily unavailable', []
    rows = []
    for line in raw.splitlines():
        match = re.search(r'^(.*?)\s*(\{.*)$', line)
        if match:
            try:
                row = json.loads(match[2])
                row['agent'] = match[1].strip()
                rows.append(row)
            except ValueError:
                pass
    return raw, rows


def identity(row):
    return row['agent'], row.get('seq'), row.get('mac', row.get('action'))


def host_observations(h):
    def observe(node):
        before = time.time()
        command = 'date -u +%s.%N; timedatectl show -p NTPSynchronized -p NTP; sudo -n iptables-save -t filter'
        try:
            result = subprocess.run(h.hosts.command(node, command, multiplex=False),
                capture_output=True, timeout=8)
            after = time.time()
            text = result.stdout.decode(errors='replace')
            first, _, rest = text.partition('\n')
            return {'node': node['name'], 'exit': result.returncode,
                    'offset_seconds': float(first)-(before+after)/2,
                    'uncertainty_seconds': (after-before)/2, 'clock_and_firewall': rest}
        except (ValueError, subprocess.TimeoutExpired) as error:
            return {'node': node['name'], 'observation_error': type(error).__name__}
    with ThreadPoolExecutor(max_workers=len(h.hosts.nodes)) as pool:
        return list(pool.map(observe, h.hosts.nodes))


def read_samples(h):
    samples = []
    for service in ('aggregator', 'field'):
        began = time.monotonic()
        try:
            if service == 'aggregator':
                code, body, seconds = h.request('aggregator', '/internal/events?limit=20',
                    {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']})
            else:
                code, body, seconds = h.api('/events?limit=20', 'FIELD')
            partial = body.get('partial') if isinstance(body, dict) else None
            valid = code == 200 and isinstance(body, dict) and isinstance(body.get('events'), list) \
                and isinstance(partial, bool) and isinstance(body.get('unavailable_units'), list)
            samples.append({'service': service, 'status': code, 'seconds': seconds,
                'partial': partial, 'valid_page': valid})
        except Exception as error:
            samples.append({'service': service, 'seconds': time.monotonic()-began,
                            'valid_page': False, 'error': str(error) if isinstance(error, Failure) else type(error).__name__})
    return samples


def cleanup_practice_request(h, request_id, name, prefix):
    raw_queue = h.kube('-n', 'tester-agent', 'get', 'configmap/tester-run', '--ignore-not-found=true', '-o', 'json')
    current = json.loads(raw_queue) if raw_queue.strip() else {}
    raw = current.get('data', {}).get('run')
    queued = json.loads(raw) if raw else {}
    if queued.get('id') != request_id:
        h.note({'practice_cleanup': 'unchanged: request is absent or belongs to another run', 'id': request_id})
        return
    h.check(queued.get('mode') == 'practice' and queued.get('name') == name,
            'Queue cleanup targets only this submitted practice request')
    h.artifact(prefix+'-practice-queue-before.private.json', current)
    patch = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
             {'op': 'test', 'path': '/data/run', 'value': raw}, {'op': 'remove', 'path': '/data/run'}]
    h.kube('-n', 'tester-agent', 'patch', 'configmap/tester-run', '--type=json', '-p', json.dumps(patch))
    h.note({'practice_cleanup': 'removed own persistent trigger after observation', 'id': request_id,
            'agent_spec_changed': False})


def practice(h, name, round_number):
    prefix = f'round-{round_number}'
    raw_queue = h.kube('-n', 'tester-agent', 'get', 'configmap/tester-run', '--ignore-not-found=true', '-o', 'json')
    queue = json.loads(raw_queue) if raw_queue.strip() else {}
    h.check(not queue.get('data', {}).get('run'),
            'No other persisted practice/exam request may be overwritten by this campaign')
    before_raw, before_rows = logs(h)
    seen = {identity(row) for row in before_rows}
    h.artifact(prefix+'-agent-before.json', {'log': before_raw})
    before = h.pods()
    h.auth('FIELD')
    baseline_hosts = host_observations(h)
    h.artifact(prefix+'-hosts-before.json', baseline_hosts)
    command = ['sudo', '-n', 'k3s', 'kubectl', '-n', 'tester-agent', 'exec',
               'daemonset/tester-agent', '--', '/app', 'run', 'practice', name]
    actions, samples, host_samples = [], [], []
    started = time.monotonic()
    stop = threading.Event()
    def sample_reads():
        while not stop.is_set():
            rows = read_samples(h)
            samples.extend({'at_seconds': time.monotonic()-started, **row} for row in rows)
            stop.wait(1)
    observer = threading.Thread(target=sample_reads, daemon=True)
    observer.start()
    next_hosts = 0
    began = completed = False
    started_agents, cleaned_agents = set(), set()
    stable_since = None
    ready_after_fault = False
    request_id = None
    try:
        submission = subprocess.run(h.hosts.command(h.server, shlex.join(command)), capture_output=True, timeout=30)
        (h.folder/(prefix+'-submission.log')).write_bytes(submission.stdout+submission.stderr)
        match = re.search(rb'Permintaan ([0-9a-f]+) dikirim', submission.stdout)
        request_id = match[1].decode() if match else None
        h.check(submission.returncode == 0, 'Official '+name+' request was submitted')
        h.check(request_id is not None, 'The official submission identifies this campaign request for safe cleanup')
        while time.monotonic()-started < h.args.timeout:
            raw, rows = logs(h, tolerate_unavailable=True)
            for row in rows:
                key = identity(row)
                if key in seen:
                    continue
                seen.add(key)
                actions.append(row)
                action = row.get('action', '')
                if 'skenario "'+name+'"' in action and 'mulai' in action:
                    began = True
                    started_agents.add(row['agent'])
                if row['agent'] in started_agents and 'pemulihan akhir selesai' in action:
                    cleaned_agents.add(row['agent'])
            completed = bool(started_agents) and started_agents <= cleaned_agents
            elapsed = time.monotonic()-started
            current_reads = samples[-2:]
            if elapsed >= next_hosts or completed:
                host_samples.append({'at_seconds': elapsed, 'nodes': host_observations(h)})
                next_hosts = elapsed+5
            if completed and recovered(h, before) and len(current_reads) == 2 and \
                    elapsed-current_reads[-1]['at_seconds'] < 5 and \
                    all(row.get('valid_page') and row.get('partial') is False for row in current_reads):
                stable_since = stable_since or time.monotonic()
                if time.monotonic()-stable_since >= 5:
                    ready_after_fault = True
                    break
            else:
                stable_since = None
            time.sleep(1)
    finally:
        stop.set()
        observer.join(timeout=60)
        h.artifact(prefix+'-observation.json', {'scenario': name, 'before_pods': before,
            'actions': actions, 'samples': samples, 'hosts': host_samples,
            'scenario_started': began, 'agent_cleanup_observed': completed,
            'started_agents': sorted(started_agents), 'cleaned_agents': sorted(cleaned_agents),
            'application_recovered': ready_after_fault,
            'read_observer_stopped': not observer.is_alive(),
            'elapsed_seconds': time.monotonic()-started})
        # An Agent reboot can replay the persisted "go" trigger. Remove only
        # our exact practice ID, after (never during) the measured fault window.
        if request_id:
            cleanup_practice_request(h, request_id, name, prefix)
    h.check(not observer.is_alive(), 'Independent read observer stopped cleanly')
    h.check(began and completed, 'Official '+name+' execution and cleanup were observed without manual intervention')
    failures = [row for row in actions if re.search(r'\bgagal\b|\berror\b', row.get('action', ''), re.I)]
    h.check(not failures, 'Agent did not report failed fault application or cleanup')
    h.check(ready_after_fault, 'Previously Ready pods and complete reads recovered stably within the observation deadline')
    after = h.pods()
    after_hosts = host_observations(h)
    h.artifact(prefix+'-after.json', {'pods': after, 'hosts': after_hosts})
    bad = [row for row in samples if not row['valid_page']]
    h.note({'scenario': name, 'round': round_number, 'samples': len(samples), 'invalid_reads': len(bad),
            'agent_elapsed_seconds': time.monotonic()-started,
            'scope': 'Official practice execution, bounded read availability and pod recovery; not full P1/P2/P5 acceptance'})
    h.check(not bad, 'Every observed read returned a complete or explicitly partial page, never a 500 or hanging request')
    return {'actions': actions, 'hosts_before': baseline_hosts, 'hosts_during': host_samples,
            'hosts_after': after_hosts, 'before_pods': before, 'after_pods': after}


def recovered(h, before):
    nodes = json.loads(h.kube('get', 'nodes', '-o', 'json'))['items']
    ready_nodes = {n['metadata']['name'] for n in nodes if any(
        c['type'] == 'Ready' and c['status'] == 'True' for c in n['status'].get('conditions', []))}
    if any(pod['ready'] and pod['node'] not in ready_nodes for pod in before.values()):
        return False
    expected = {}
    for pod in before.values():
        if pod['ready'] and pod['app']:
            expected[pod['app']] = expected.get(pod['app'], 0)+1
    current = {}
    for pod in h.pods().values():
        if pod['ready'] and pod['app']:
            current[pod['app']] = current.get(pod['app'], 0)+1
    return all(current.get(app, 0) >= count for app, count in expected.items())
