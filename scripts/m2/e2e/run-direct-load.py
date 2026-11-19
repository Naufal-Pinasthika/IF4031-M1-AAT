#!/usr/bin/env python3
"""Run the unchanged k6 scenarios directly from the isolated Linux lab.

No Windows TCP relay. Safety stops are failures, not successful test results.
This is a dedicated-lab controller, not a production deployment tool.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[3]
NS = 'sistem-informasi-kebencanaan-bnpb'


def kube(*args):
    result = subprocess.run(['kubectl', '--request-timeout=10s', '-n', NS, *args],
                            text=True, capture_output=True, timeout=15)
    if result.returncode:
        raise RuntimeError(result.stderr[-1500:])
    return result.stdout.strip()


def ready(replicas):
    kube('scale', 'deployment/aggregator', f'--replicas={replicas}')
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if int(kube('get', 'deployment', 'aggregator', '-o=jsonpath={.status.readyReplicas}') or 0) == replicas:
            return
        time.sleep(2)
    raise RuntimeError(f'{replicas} Ready replicas not observed')


def available_mib():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) / 1024


def child_limits():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (100000 if hard == resource.RLIM_INFINITY else min(100000, hard), hard))


def archive_previous_run(evidence, name):
    artifacts = [p for p in evidence.glob(name+'-*') if p.is_file()]
    if artifacts:
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        archive = evidence / 'runs' / f'prior-{name}-{stamp}'
        archive.mkdir(parents=True)
        for artifact in artifacts:
            artifact.replace(archive / artifact.name)


def read_host_memory(helper, state):
    try:
        for line in helper.stdout:
            timestamp_ms, available_kib, physical_kib = line.strip().split(',')
            state['available_mib'] = int(available_kib)/1024
            state['physical_available_mib'] = int(physical_kib)/1024
            # Account for buffered samples after a scheduling/paging stall.
            age = max(0, time.time()-int(timestamp_ms)/1000)
            state['observed_at'] = time.monotonic()-age
    except BaseException as exc:
        state['error'] = str(exc)


def monitor_generator(process, args, result, stopped, host_state):
    # Observation/scaling can block in kubectl. Memory protection must not.
    try:
        started = previous_time = time.monotonic()
        previous_ticks = None
        ticks_per_second = os.sysconf('SC_CLK_TCK')
        while not stopped.is_set() and process.poll() is None:
            try:
                status = Path(f'/proc/{process.pid}/status').read_text()
                fields = Path(f'/proc/{process.pid}/stat').read_text().rsplit(')', 1)[1].split()
            except FileNotFoundError:
                return
            now = time.monotonic()
            ticks = int(fields[11]) + int(fields[12])
            cpu = 0 if previous_ticks is None else 100*(ticks-previous_ticks)/ticks_per_second/max(now-previous_time, 0.001)
            previous_ticks, previous_time = ticks, now
            result['peak_cpu_percent_of_one_core'] = max(cpu, result.get('peak_cpu_percent_of_one_core', 0))
            rss = next((int(x.split()[1])/1024 for x in status.splitlines() if x.startswith('VmRSS:')), 0)
            result['peak_rss_mib'] = max(rss, result['peak_rss_mib'])
            free = available_mib()
            result['lowest_linux_available_mib'] = min(free, result.get('lowest_linux_available_mib', free))
            sample = dict(seconds=round(now-started, 2), rss_mib=round(rss, 2), cpu_percent_of_one_core=round(cpu, 2), linux_available_mib=round(free, 2))
            reason = None
            if rss > args.max_rss_mib or free < args.min_free_mib:
                reason = 'Generator RSS budget or Linux memory headroom exceeded'
            if host_state is not None:
                if host_state.get('error') or time.monotonic()-host_state['observed_at'] > 10:
                    raise RuntimeError('Native Windows memory telemetry failed or became stale')
                commit = host_state['available_mib']
                physical = host_state['physical_available_mib']
                result['lowest_windows_commit_available_mib'] = min(commit, result.get('lowest_windows_commit_available_mib', commit))
                result['lowest_windows_physical_available_mib'] = min(physical, result.get('lowest_windows_physical_available_mib', physical))
                sample.update(windows_commit_available_mib=round(commit, 2), windows_physical_available_mib=round(physical, 2))
                if commit < 512:
                    reason = 'Windows commit headroom fell below 512 MiB'
                if physical < 768:
                    reason = 'Windows physical memory headroom fell below 768 MiB'
            samples = result.setdefault('resource_samples', [])
            if len(samples) < 600:
                samples.append(sample)
            if reason:
                result['safety_stop_reason'] = reason
                process.send_signal(signal.SIGINT)
                return
            stopped.wait(1)
    except BaseException as exc:
        if process.poll() is None:
            result['safety_stop_reason'] = f'Memory safety monitor failed: {exc}'
            process.send_signal(signal.SIGINT)


def run(args, name, script, env, during=None):
    evidence = ROOT / '.e2e'
    # SIGKILL can prevent k6 from replacing its summary. Never let a prior
    # successful summary survive under the filename of a new failed run.
    archive_previous_run(evidence, name)
    result = dict(passed=False, vus=args.vus, peak_rss_mib=0, safety_stop_reason=None,
                  endpoint=env['AGGREGATOR_BASE_URL'], script=script,
                  ran_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    process = None
    watcher = None
    host_helper = None
    host_reader = None
    stopped = threading.Event()
    started = time.monotonic()
    try:
        if available_mib() < args.min_free_mib + 256:
            raise RuntimeError('Insufficient Linux memory headroom before load generation')
        result['placement'] = [{'pod': p['metadata']['name'], 'node': p['spec']['nodeName'],
                                'image': next(c['image'] for c in p['spec']['containers'] if c['name'] == 'aggregator')}
                               for p in json.loads(kube('get', 'pods', '-l', 'app=aggregator', '-o=json'))['items']
                               if not p['metadata'].get('deletionTimestamp')]
        powershell = Path('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
        host_state = None
        if powershell.exists():
            monitor_path = subprocess.check_output(['wslpath', '-w', str(ROOT / 'scripts/m2/e2e/windows-memory-monitor.ps1')], text=True).strip()
            host_helper = subprocess.Popen([str(powershell), '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', monitor_path],
                                           stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            host_state = {}
            host_reader = threading.Thread(target=read_host_memory, args=(host_helper, host_state), daemon=True)
            host_reader.start()
            deadline = time.monotonic()+20
            while 'observed_at' not in host_state:
                if host_helper.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('Native Windows memory monitor did not start')
                time.sleep(0.1)
            if host_state['available_mib'] < 1024:
                raise RuntimeError('Insufficient Windows commit headroom before load generation')
            if host_state['physical_available_mib'] < 1536:
                raise RuntimeError('Insufficient Windows physical memory headroom before load generation')
        with (evidence / f'{name}-stdout.log').open('w') as out, (evidence / f'{name}-stderr.log').open('w') as err:
            generator_env = dict(env, GOMAXPROCS=str(args.generator_procs))
            result['generator_gomaxprocs'] = args.generator_procs
            result['rss_budget_mib'] = args.max_rss_mib
            process = subprocess.Popen([args.k6, 'run', '--quiet', '--summary-export',
                                        str(evidence / f'{name}-summary.json'), str(ROOT / script)],
                                       env=generator_env, stdout=out, stderr=err, preexec_fn=child_limits,
                                       start_new_session=True)
            watcher = threading.Thread(target=monitor_generator, args=(process, args, result, stopped, host_state), daemon=True)
            watcher.start()
            while process.poll() is None:
                if during:
                    during(time.monotonic()-started)
                time.sleep(1)
            result['exit_code'] = process.returncode
            if result['safety_stop_reason']:
                raise RuntimeError(result['safety_stop_reason'])
            if process.returncode:
                raise RuntimeError(f'k6 failed (exit {process.returncode}); inspect retained evidence')
            result['passed'] = True
    except BaseException as exc:
        result['error'] = str(exc)
        raise
    finally:
        stopped.set()
        if watcher:
            watcher.join(timeout=10)
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        if host_helper:
            if host_helper.poll() is None:
                try:
                    host_helper.stdin.write('\n')
                    host_helper.stdin.flush()
                    host_helper.wait(timeout=5)
                except (BrokenPipeError, subprocess.TimeoutExpired):
                    host_helper.terminate()
                    host_helper.wait(timeout=5)
            host_helper.stdin.close()
            if host_reader:
                host_reader.join(timeout=2)
            host_helper.stdout.close()
        result['elapsed_seconds'] = round(time.monotonic()-started, 2)
        (evidence / f'{name}-generator-resources.json').write_text(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case', choices=['profile', 'scale', 'throughput', 'rolling', 'priority'])
    parser.add_argument('--vus', type=int, default=10000)
    parser.add_argument('--host', default='192.168.93.10', choices=['192.168.93.10', '192.168.93.11', '192.168.93.12'])
    parser.add_argument('--k6', default='/var/lib/aat-m2-e2e/tools/k6-v2.4.0-linux-amd64/k6')
    parser.add_argument('--max-rss-mib', type=int, default=1024)
    parser.add_argument('--generator-procs', type=int, default=2)
    parser.add_argument('--min-free-mib', type=int, default=256)
    parser.add_argument('--upgrade-image', default='')
    args = parser.parse_args()
    if not 1 <= args.vus <= 10000 or args.min_free_mib < 256 or args.max_rss_mib < 128 or not 1 <= args.generator_procs <= 12:
        parser.error('Invalid VU or memory safety bounds')
    env = os.environ.copy()
    env['KUBECONFIG'] = str(ROOT / '.e2e/kubeconfig')
    os.environ['KUBECONFIG'] = env['KUBECONFIG']
    for line in (ROOT / '.env').read_text().splitlines():
        key, sep, value = line.partition('=')
        if sep and key.replace('_', '').isalnum() and key.isupper():
            env[key] = value.strip().strip('"').strip("'")
    for key, port in [('AGGREGATOR_BASE_URL', 30083), ('AUTH_BASE_URL', 30084), ('CLIENT_API_BASE_URL', 30080)]:
        env[key] = f'http://{args.host}:{port}'
    env['VUS'] = str(args.vus)
    name = f'direct-{args.case}-{args.vus}'
    if args.case == 'profile':
        # Resource profiling only: same HTTP payload/checks, no Kubernetes writes.
        # A threshold pass is not proof of the full P2/P5 criteria or data completeness.
        count = int(kube('get', 'deployment', 'aggregator', '-o=jsonpath={.status.readyReplicas}') or 0)
        if count != 3:
            raise RuntimeError('Profiling requires three Ready Aggregators before starting')
        env['HOLD_DURATION'] = '120s'
        run(args, name, 'scripts/m2/p2/load.js', env)
    elif args.case == 'scale':
        state = dict(up=False, down=False, saw_five=False, saw_two=False, passed=False, snapshots=[])
        last_sample = -5
        def scaling(seconds):
            nonlocal last_sample
            if seconds >= 60 and not state['up']:
                kube('scale', 'deployment/aggregator', '--replicas=5')
                state['up'] = True
            if seconds >= 110 and not state['down']:
                kube('scale', 'deployment/aggregator', '--replicas=2')
                state['down'] = True
            if seconds-last_sample >= 5:
                count = int(kube('get', 'deployment', 'aggregator', '-o=jsonpath={.status.readyReplicas}') or 0)
                state['snapshots'].append(dict(seconds=round(seconds, 2), ready=count))
                state['saw_five'] |= state['up'] and not state['down'] and count == 5
                state['saw_two'] |= state['down'] and count == 2
                last_sample = seconds
        try:
            ready(3)
            env['HOLD_DURATION'] = '120s'
            run(args, name, 'scripts/m2/p2/load.js', env, scaling)
            if not state['saw_five'] or not state['saw_two']:
                raise RuntimeError('Both requested replica counts were not observed Ready during load')
            state['passed'] = True
        finally:
            (ROOT / f'.e2e/{name}-results.json').write_text(json.dumps(state, indent=2))
            ready(3)
    elif args.case == 'throughput':
        result = dict(passed=False, vus=args.vus, duration_seconds=60)
        try:
            for count in [1, 3]:
                ready(count)
                time.sleep(10)
                env['DURATION'] = '60s'
                run(args, f'{name}-{count}', 'scripts/m2/p2/throughput.js', env)
            summaries = [json.loads((ROOT / f'.e2e/{name}-{n}-summary.json').read_text()) for n in [1, 3]]
            rates = [s['metrics']['successful_requests']['rate'] for s in summaries]
            result.update(one_success_rps=rates[0], three_success_rps=rates[1], ratio=rates[1]/rates[0], passed=rates[1] >= 2*rates[0])
            if not result['passed']:
                raise RuntimeError('Successful complete-response throughput ratio below 2')
        except BaseException as exc:
            result['error'] = str(exc)
            raise
        finally:
            (ROOT / f'.e2e/{name}-results.json').write_text(json.dumps(result, indent=2))
            ready(3)
    elif args.case == 'rolling':
        state = dict(triggered=False, completed_seconds=None, passed=False, image=args.upgrade_image)
        last_sample = -5
        def upgrade(seconds):
            nonlocal last_sample
            if seconds >= 10 and not state['triggered']:
                if args.upgrade_image:
                    kube('set', 'image', 'deployment/aggregator', f'aggregator={args.upgrade_image}')
                else:
                    kube('rollout', 'restart', 'deployment/aggregator')
                state['triggered'] = True
            if state['triggered'] and state['completed_seconds'] is None and seconds-last_sample >= 5:
                deployment = json.loads(kube('get', 'deployment', 'aggregator', '-o=json'))
                status, count = deployment['status'], deployment['spec']['replicas']
                if all(status.get(key) == count for key in ['replicas', 'updatedReplicas', 'availableReplicas', 'readyReplicas']) and status.get('observedGeneration', 0) >= deployment['metadata']['generation']:
                    state['completed_seconds'] = round(time.monotonic()-started, 2)
                last_sample = seconds
        env['DURATION'] = '120s'
        started = time.monotonic()
        try:
            run(args, name, 'scripts/m2/p2/rolling.js', env, upgrade)
            if state['completed_seconds'] is None or state['completed_seconds'] >= 120:
                raise RuntimeError('Complete rollout was not observed before the load finished')
            state['passed'] = True
        finally:
            (ROOT / f'.e2e/{name}-results.json').write_text(json.dumps(state, indent=2))
    else:
        env['MEDIA_VUS'] = str(args.vus)
        env['DURATION'] = '120s'
        run(args, name, 'scripts/m2/p5/05-priority-load.js', env)


if __name__ == '__main__':
    main()
