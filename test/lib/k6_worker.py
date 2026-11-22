from pathlib import Path
import json
import os
import resource
import signal
import subprocess
import sys
import time
import threading
import urllib.request


def available():
    return int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                    if line.startswith('MemAvailable:'))) / 1024


def interrupted(*_):
    raise KeyboardInterrupt()


def main():
    os.umask(0o077)
    config = json.load(sys.stdin)
    folder = Path(__file__).resolve().parent
    result = {'samples': [], 'peak_rss_mib': 0, 'safety_stop_reason': None,
              'k6_version': subprocess.check_output(['k6', 'version'], text=True).strip()}
    procs, gogc = int(config.get('gomaxprocs', 6)), int(config.get('gogc', 100))
    if not 1 <= procs <= os.cpu_count() or not 50 <= gogc <= 400:
        raise ValueError('Generator runtime settings exceed bounded host capacity')
    result['runtime'] = {'gomaxprocs': procs, 'gogc': gogc}
    identity = {'pid': os.getpid(), 'started': Path('/proc/self/stat').read_text().split()[21]}
    (folder / 'worker-identity.json').write_text(json.dumps(identity))
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (65536 if hard == resource.RLIM_INFINITY else min(65536, hard), hard))
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    child = None
    profiler = None
    started = time.monotonic()
    try:
        if available() < 1024:
            raise RuntimeError('Generator has less than 1 GiB initial memory headroom')
        with (folder / 'k6.log').open('w') as log:
            command = ['k6', 'run', '--quiet', '--summary-export', str(folder / 'summary.json')]
            if config.get('profile_seconds'):
                command += ['--profiling-enabled', '--address', '127.0.0.1:6565']
            child = subprocess.Popen([*command,
                str(folder / 'workload.js')], env={**os.environ, **config['env'],
                    'GOMAXPROCS': str(procs), 'GOGC': str(gogc)},
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            if config.get('profile_seconds'):
                def profile():
                    try:
                        time.sleep(15)
                        with urllib.request.urlopen('http://127.0.0.1:6565/debug/pprof/profile?seconds=' +
                                str(config['profile_seconds']), timeout=40) as response:
                            (folder / 'generator.pprof').write_bytes(response.read())
                    except Exception as error:
                        (folder / 'profile-error.txt').write_text(str(error))
                profiler = threading.Thread(target=profile, daemon=True)
                profiler.start()
            previous_ticks, previous_at = 0, started
            while child.poll() is None:
                try:
                    status = Path(f'/proc/{child.pid}/status').read_text()
                    fields = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()
                except FileNotFoundError:
                    break
                now = time.monotonic()
                rss = next((int(x.split()[1])/1024 for x in status.splitlines() if x.startswith('VmRSS:')), 0)
                ticks = int(fields[11]) + int(fields[12])
                cpu = 0 if previous_at == started else 100*(ticks-previous_ticks)/os.sysconf('SC_CLK_TCK')/max(.001, now-previous_at)
                previous_ticks, previous_at = ticks, now
                result['peak_rss_mib'] = max(result['peak_rss_mib'], rss)
                free = available()
                result['samples'].append({'seconds': now-started, 'rss_mib': rss, 'available_mib': free, 'cpu_percent_one_core': cpu})
                if rss > config['max_rss_mib'] or free < config['min_free_mib'] or now-started > config['wall_seconds']:
                    result['safety_stop_reason'] = 'Generator memory budget, memory headroom or bounded campaign deadline exceeded'
                    break
                time.sleep(1)
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            result['exit_code'] = child.returncode
    finally:
        if child and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        if profiler:
            profiler.join(timeout=1)
        result['elapsed_seconds'] = time.monotonic()-started
        (folder / 'resources.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({'exit_code': result.get('exit_code'), 'safety_stop_reason': result['safety_stop_reason']}), flush=True)


if __name__ == '__main__':
    main()
