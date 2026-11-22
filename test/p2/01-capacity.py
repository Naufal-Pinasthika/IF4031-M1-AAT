from pathlib import Path
import importlib.util
import json
import os
import re
import resource
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import ROOT, Failure, parser, run


def ready(h, replicas):
    h.kube('scale', 'deployment/aggregator', f'--replicas={replicas}')
    h.kube('rollout', 'status', 'deployment/aggregator', '--timeout=180s')
    h.wait(lambda: len([p for p in h.pods().values() if p['app'] == 'aggregator' and p['ready']]) == replicas,
           f'{replicas} Ready Aggregators were not observed')


def transport(h):
    targets = []
    for _ in range(h.args.tunnels):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        command = h.hosts.command(h.server, '', multiplex=False)[:-2]
        command += ['-L', f'127.0.0.1:{port}:{h.server["private_ip"]}:30083',
                    '-o', 'ExitOnForwardFailure=yes', '-N', h.server['user'] + '@' + h.server['host']]
        with (h.folder / f'tunnel-{port}.log').open('w') as log:
            process = subprocess.Popen(command, stderr=log)
        h.extra_processes.append(process)
        deadline = time.monotonic() + 15
        while True:
            h.check(process.poll() is None and time.monotonic() < deadline, 'Independent entrypoint tunnel became available')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=1):
                    break
            except OSError:
                time.sleep(.1)
        targets.append(f'http://127.0.0.1:{port}')
    h.artifact('transport.json', {'tunnels': targets, 'remote_target': h.server['private_ip'] + ':30083',
                                'scope': 'All tunnels reach the same gateway NodePort, never individual Aggregator pods'})
    return ','.join(targets)


def remote_generator(h, name, script, env, during):
    node = h.hosts.data['load_generator']
    folder = '.local/state/aat-load/' + h.run_id + '/' + name
    upload = 'import json,pathlib,os;os.umask(0o077);d=json.load(__import__("sys").stdin);p=pathlib.Path.home()/d["folder"];p.mkdir(parents=True,exist_ok=False);[(p/k).write_text(v) for k,v in d["files"].items()]'
    h.hosts.ssh(node, 'python3 -c ' + shlex.quote(upload), json.dumps({'folder': folder, 'files': {
        'worker.py': (ROOT/'test/lib/k6_worker.py').read_text(), 'workload.js': (ROOT/script).read_text()}}).encode(), capture=True)
    remote_path = '~/' + folder
    config = {'env': {**env, 'AGGREGATOR_BASE_URL': env.get('AGGREGATOR_BASE_URL', 'http://' + h.server['private_ip'] + ':30083'),
                     'AGGREGATOR_API_KEY': h.values['AGGREGATOR_API_KEY']},
              'max_rss_mib': h.args.max_rss_mib, 'min_free_mib': h.args.min_free_mib,
              'wall_seconds': h.args.seconds + 100}
    config['gomaxprocs'] = getattr(h.args, 'generator_procs', 6)
    config['gogc'] = getattr(h.args, 'generator_gogc', 100)
    if getattr(h.args, 'profile', False):
        config['profile_seconds'] = 20
    resources = {'server_samples': []}
    with (h.folder / (name+'-control.log')).open('w') as log:
        process = subprocess.Popen(h.hosts.command(node, 'python3 '+remote_path+'/worker.py', multiplex=False),
            stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
        process.stdin.write(json.dumps(config).encode()); process.stdin.close()
        started, last_sample = time.monotonic(), -15
        try:
            while process.poll() is None:
                elapsed = time.monotonic()-started
                if elapsed > config['wall_seconds'] + 30:
                    raise Failure('Remote generator exceeded its control deadline')
                if during:
                    during(elapsed)
                if elapsed-last_sample >= 15:
                    resources['server_samples'].append({'seconds': elapsed, 'nodes': h.kube('top','nodes'),
                                                        'pods': h.kube('top','pods','--containers')})
                    last_sample = elapsed
                time.sleep(1)
        finally:
            if process.poll() is None:
                cleanup = ('import pathlib,json,os,signal;p=pathlib.Path.home()/'+repr(folder)+';'
                    'd=json.loads((p/"worker-identity.json").read_text());s=pathlib.Path("/proc")/str(d["pid"]);'
                    'same=s.exists() and (s/"stat").read_text().split()[21]==d["started"];'
                    'os.kill(d["pid"],signal.SIGTERM) if same else None')
                h.hosts.ssh(node, 'python3 -c '+shlex.quote(cleanup), capture=True)
                process.wait(timeout=20)
    files = [('summary.json',name+'-summary.json'),('resources.json',name+'-resources.json'),('k6.log',name+'.log')]
    if config.get('profile_seconds'):
        files.append(('generator.pprof', name+'-generator.pprof'))
    for remote, local in files:
        payload = h.hosts.ssh(node, 'python3 -c '+shlex.quote('import pathlib,sys;sys.stdout.buffer.write((pathlib.Path.home()/'+repr(folder+'/'+remote)+').read_bytes())'), capture=True)
        (h.folder/local).write_bytes(payload)
    measured = json.loads((h.folder/(name+'-resources.json')).read_text())
    h.artifact(name+'-server-resources.json',resources)
    h.check(not measured['safety_stop_reason'], 'Remote k6 stayed inside generator safety bounds')
    summary = json.loads((h.folder/(name+'-summary.json')).read_text())
    metrics = {k:v.get('values',v) for k,v in summary['metrics'].items()}
    result = {'name':name, 'exit_code':measured['exit_code'], 'metrics':metrics,
              'valid_timing':metrics['http_req_duration']['min']>=0, 'peak_rss_mib':measured['peak_rss_mib']}
    h.note({**result,'metrics':{k:metrics[k] for k in ('http_reqs','unexpected_failures','http_req_duration','successful_requests','responses_2xx','responses_429') if k in metrics}})
    return result


def generator(h, name, script, env, during=None):
    if h.args.generator_vps:
        return remote_generator(h, name, script, env, during)
    spec = importlib.util.spec_from_file_location('local_load_guard', ROOT / 'scripts/m2/e2e/run-direct-load.py')
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    resources = {'peak_rss_mib': 0, 'safety_stop_reason': None, 'server_samples': []}
    stopped = threading.Event()
    host_state = {}
    ps = '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
    monitor = subprocess.Popen([ps, '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File',
        subprocess.check_output(['wslpath', '-w', str(ROOT / 'scripts/m2/e2e/windows-memory-monitor.ps1')], text=True).strip()],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    reader = threading.Thread(target=guard.read_host_memory, args=(monitor, host_state), daemon=True)
    reader.start()
    process = watcher = None
    try:
        deadline = time.monotonic() + 20
        while 'observed_at' not in host_state:
            if monitor.poll() is not None or time.monotonic() > deadline:
                raise Failure('Windows memory telemetry did not start')
            time.sleep(.1)
        h.check(host_state['physical_available_mib'] >= 1536 and host_state['available_mib'] >= 1024,
                'Laptop has initial physical and commit memory headroom')
        h.check(guard.available_mib() >= 1024, 'WSL has initial memory headroom')
        h.check(shutil.disk_usage(ROOT).free >= 1024**3, 'Laptop has at least 1 GiB disk headroom')
        env = {**os.environ, **env, 'GOMAXPROCS': '6',
               'AGGREGATOR_BASE_URLS': h.capacity_targets, 'AGGREGATOR_API_KEY': h.values['AGGREGATOR_API_KEY']}
        with (h.folder / (name + '.log')).open('w') as log:
            process = subprocess.Popen(['k6', 'run', '--quiet', '--summary-export',
                str(h.folder / (name + '-summary.json')), str(ROOT / script)], env=env,
                stdout=log, stderr=subprocess.STDOUT, preexec_fn=guard.child_limits)
            watcher = threading.Thread(target=guard.monitor_generator,
                args=(process, h.args, resources, stopped, host_state), daemon=True)
            watcher.start()
            started = time.monotonic()
            last_sample = -15
            while process.poll() is None:
                h.check(all(p.poll() is None for p in h.extra_processes), 'All capacity transport tunnels remain active')
                if during:
                    during(time.monotonic() - started)
                if time.monotonic() - started - last_sample >= 15:
                    resources['server_samples'].append({'seconds': time.monotonic() - started,
                        'nodes': h.kube('top', 'nodes'), 'pods': h.kube('top', 'pods', '--containers')})
                    last_sample = time.monotonic() - started
                time.sleep(1)
            resources.update(exit_code=process.returncode, elapsed_seconds=time.monotonic() - started)
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        stopped.set()
        if watcher:
            watcher.join(timeout=5)
        if monitor.poll() is None:
            monitor.stdin.write('\n')
            monitor.stdin.flush()
            monitor.wait(timeout=10)
        reader.join(timeout=2)
        h.artifact(name + '-resources.json', resources)
    h.check(not resources['safety_stop_reason'], 'Load generator stayed within laptop memory safety bounds')
    summary = json.loads((h.folder / (name + '-summary.json')).read_text())
    metrics = {k: v.get('values', v) for k, v in summary['metrics'].items()}
    selected = {k: v for k, v in metrics.items() if k in (
        'successful_requests', 'responses_2xx', 'responses_429', 'responses_complete', 'responses_partial',
        'responses_other', 'responses_malformed', 'unexpected_failures', 'http_req_duration', 'http_reqs', 'vus_max')}
    result = {'name': name, 'exit_code': process.returncode, 'metrics': selected,
              'valid_timing': metrics['http_req_duration']['min'] >= 0,
              'peak_rss_mib': resources['peak_rss_mib']}
    h.note(result)
    return result


def polling_snapshot(h):
    return h.sql("""SELECT json_build_object('at',clock_timestamp(),
        'poll_id',COALESCE((SELECT max(poll_id) FROM poll_attempts),0),
        'version',COALESCE((SELECT max(version) FROM ingestion_outbox),0))""")


def polling_evidence(h, before):
    after = polling_snapshot(h)
    polls = h.sql(f"""SELECT COALESCE(json_agg(row_to_json(p)),'[]'::json) FROM poll_attempts p
        WHERE poll_id>{int(before['poll_id'])} AND poll_id<={int(after['poll_id'])}
        AND source IN ('BMKG','PVMBG')""")
    counts = Counter()
    for service in ('bmkg', 'pvmbg'):
        raw = h.kube('logs', 'deployment/' + service, '--since-time=' + before['at'])
        h.artifact(service + '-upstream-logs.json', {'log': raw})
        for line in raw.splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            match = re.fullmatch(r'poll-(BMKG|PVMBG)-(\d+)', row.get('correlation_id', ''))
            if match:
                counts[(match[1], int(match[2]), row['path'])] += 1
    missing, duplicate = [], []
    for poll in polls:
        paths = ['/seismic-events', '/tsunami-warnings'] if poll['source'] == 'BMKG' else ['/volcanic-reports']
        for path in paths:
            n = counts[poll['source'], poll['poll_id'], path]
            if n > 1:
                duplicate.append({'poll_id': poll['poll_id'], 'path': path, 'count': n})
            if poll['status'] == 'completed' and n != 1:
                missing.append({'poll_id': poll['poll_id'], 'path': path, 'count': n})
    cadence = int(json.loads(h.kube('get', 'configmap/aggregator-config', '-o', 'json'))['data']['AGGREGATOR_POLL_INTERVAL_MS'])
    too_close = []
    from datetime import datetime
    for source in ('BMKG', 'PVMBG'):
        rows = sorted((p for p in polls if p['source'] == source), key=lambda p: p['poll_id'])
        for a, b in zip(rows, rows[1:]):
            def stamp(value):
                return datetime.fromisoformat(re.sub(r'\.(\d+)(?=[+-]\d{2}:\d{2}$)',
                    lambda m: '.'+m[1][:6].ljust(6,'0'), value))
            distance = (stamp(b['started_at']) - stamp(a['started_at'])).total_seconds()*1000
            if distance < cadence - 20:
                too_close.append({'source': source, 'a': a['poll_id'], 'b': b['poll_id'], 'distance_ms': distance})
    repeated = h.sql(f"""SELECT COALESCE(json_agg(row_to_json(x)),'[]'::json) FROM
        (SELECT source,hazard_id,count(*) AS admissions FROM ingestion_outbox
        WHERE version>{int(before['version'])} AND version<={int(after['version'])}
        AND source IN ('BMKG','PVMBG') GROUP BY source,hazard_id HAVING count(*)>1) x""")
    h.artifact('polling.json', {'before': before, 'after': after, 'polls': polls,
        'missing_logs': missing, 'duplicate_requests': duplicate, 'cadence_violations': too_close,
        'repeated_admissions': repeated})
    h.check(polls and not missing and not duplicate and not too_close and not repeated,
            'Mock upstream logs and admission ledger show no multiplied polling or repeated admissions during capacity changes')


def scenario(h):
    h.changes()
    h.check(h.args.generator_procs >= 1 and 50 <= h.args.generator_gogc <= 400,
            'Generator runtime parameters are bounded and recorded')
    h.check(h.args.vps and (h.args.generator_vps or bool(shutil.which('k6'))), 'k6 runs outside the application cluster')
    h.check(1 <= h.args.tunnels <= 8, 'Tunnel count is bounded from one to eight')
    h.capacity_targets = '' if h.args.generator_vps else transport(h)
    version = (h.hosts.ssh(h.hosts.data['load_generator'], 'k6 version', capture=True).decode()
               if h.args.generator_vps else subprocess.check_output(['k6', 'version'], text=True))
    h.artifact('generator.json', {'version': version.strip(),
                                 'placement': 'external-vps' if h.args.generator_vps else 'laptop',
                                 'runtime': {'gomaxprocs': h.args.generator_procs, 'gogc': h.args.generator_gogc}
                                            if h.args.generator_vps else {'gomaxprocs': 6},
                                 'memory_limit_mib': h.args.max_rss_mib,
                                 'file_descriptor_limits': resource.getrlimit(resource.RLIMIT_NOFILE)})
    deployment = json.loads(h.kube('get', 'deployment/aggregator', '-o', 'json'))
    original = deployment['spec']['replicas']
    pods = [p for p in h.pods().values() if p['app'] == 'aggregator']
    h.check(len(pods) >= 3 and all(p['ready'] for p in pods) and len({p['node'] for p in pods}) >= 3,
            'At least three Ready Aggregators initially span three nodes')
    code, page, _ = h.request('aggregator', '/internal/events?limit=20',
                            {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']})
    h.check(code == 200 and page.get('partial') is False and len(page.get('events', [])) == 20,
            'Authenticated entrypoint returns a complete nonempty baseline page')
    results = []
    before = polling_snapshot(h)
    h.artifact('polling-before.json', before)
    try:
        if h.args.mode == 'throughput':
            h.check(h.args.seconds >= 60, 'Each throughput case lasts at least 60 seconds')
            for pair in range(h.args.repeat):
                order = [1, 3] if (pair + int(h.args.reverse_first)) % 2 == 0 else [3, 1]
                comparison = {}
                for replicas in order:
                    ready(h, replicas)
                    time.sleep(10)
                    name = f'pair-{pair+1}-{replicas}-replicas'
                    h.artifact(name + '-pods.json', h.pods())
                    case = generator(h, name, 'scripts/m2/p2/throughput.js',
                                     {'VUS': str(h.args.vus), 'DURATION': str(h.args.seconds) + 's'})
                    comparison[str(replicas)] = case
                    h.artifact(f'pair-{pair+1}-completed-cases.json', comparison)
                one = comparison['1']['metrics']['successful_requests']['rate']
                three = comparison['3']['metrics']['successful_requests']['rate']
                ratio = three / one if one > 0 else None
                good = ratio is not None and ratio >= 2 and all(v['exit_code'] == 0 and v['valid_timing'] for v in comparison.values())
                results.append({'pair': pair+1, 'one_complete_rps': one, 'three_complete_rps': three,
                                'ratio': ratio, 'passed': good, 'cases': comparison})
                h.note({k: v for k, v in results[-1].items() if k != 'cases'})
                h.artifact('comparisons.json', results)
        else:
            h.check(h.args.vus >= 10000 and h.args.seconds >= 120,
                    'Scaling acceptance requires at least 10000 VUs held for at least 120 seconds')
            for repetition in range(h.args.repeat):
                ready(h, 3)
                state = {'up': False, 'down': False, 'five_ready': False, 'two_ready': False, 'samples': []}
                last_sample = -10
                def during(elapsed):
                    nonlocal last_sample
                    if elapsed >= 60 and not state['up']:
                        h.kube('scale', 'deployment/aggregator', '--replicas=5')
                        state['up'] = True
                    if elapsed >= 110 and not state['down']:
                        h.kube('scale', 'deployment/aggregator', '--replicas=2')
                        state['down'] = True
                    if elapsed - last_sample >= 5:
                        d = json.loads(h.kube('get', 'deployment/aggregator', '-o', 'json'))
                        count = d['status'].get('readyReplicas', 0)
                        state['samples'].append({'seconds': elapsed, 'ready': count, 'desired': d['spec']['replicas']})
                        state['five_ready'] |= state['up'] and not state['down'] and count == 5
                        state['two_ready'] |= state['down'] and count == 2
                        last_sample = elapsed
                try:
                    case = generator(h, f'scale-{repetition+1}', 'scripts/m2/p2/load.js',
                        {'VUS': str(h.args.vus), 'HOLD_DURATION': str(h.args.seconds) + 's'}, during)
                finally:
                    h.artifact(f'scale-{repetition+1}-timeline.json', state)
                good = case['exit_code'] == 0 and case['valid_timing'] and state['five_ready'] and state['two_ready']
                results.append({'round': repetition+1, 'passed': good, 'case': case, 'scaling': state})
    finally:
        h.artifact('comparisons.json', results)
        ready(h, original)
        h.artifact('final-pods.json', h.pods())
    polling_evidence(h, before)
    h.check(results and all(r['passed'] for r in results), 'All repeated capacity cases meet their original thresholds')


if __name__ == '__main__':
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE,
        (max(soft, 65536 if hard == resource.RLIM_INFINITY else min(65536, hard)), hard))
    p = parser('Compare complete-response throughput or test sustained 3-5-2 scaling from the laptop')
    p.add_argument('--mode', choices=['throughput', 'scale'], default='throughput')
    p.add_argument('--vus', type=int, default=1000)
    p.add_argument('--seconds', type=int, default=60)
    p.add_argument('--tunnels', type=int, default=4)
    p.add_argument('--generator-vps', action='store_true', help='Run k6 on inventory.load_generator over direct WireGuard')
    p.add_argument('--generator-procs', type=int, default=6, help='External generator Go CPU parallelism (same for both cases)')
    p.add_argument('--generator-gogc', type=int, default=100, help='External generator GC target, 50..400; memory safety limits still apply')
    p.add_argument('--reverse-first', action='store_true', help='Begin throughput comparison with 3 then 1 replicas')
    p.add_argument('--max-rss-mib', type=int, default=3072)
    p.add_argument('--min-free-mib', type=int, default=512)
    raise SystemExit(run('p2/01-capacity', ['P2.1', 'P2.7 or P2.6 workload/scaling only'], p, scenario))
