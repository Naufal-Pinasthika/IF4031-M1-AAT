from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from tester_agent import practice


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Clock faults affect only the VPS lab, never the laptop')
    for repetition in range(h.args.repeat):
        probe = '''import json,time
end=time.monotonic()+70
while time.monotonic()<end:
 print(json.dumps([time.time(),time.monotonic()]),flush=True)
 time.sleep(0.05)
'''
        def observe(node):
            call = subprocess.run(h.hosts.command(node, 'python3 -u -c '+shlex.quote(probe), multiplex=False),
                capture_output=True, timeout=80, check=True)
            samples = [json.loads(line) for line in call.stdout.splitlines()]
            jumps = [b[0]-a[0]-(b[1]-a[1]) for a, b in zip(samples, samples[1:])]
            return {'node': node['name'], 'samples': samples, 'largest_jump_seconds': max(map(abs, jumps), default=0)}
        with ThreadPoolExecutor(max_workers=3) as pool:
            observations = [pool.submit(observe, node) for node in h.hosts.nodes]
            try:
                result = practice(h, 'clock-skew', repetition+1)
            finally:
                clocks = [future.result() for future in observations]
                h.artifact(f'round-{repetition+1}-clock-traces.json', clocks)
        h.check(any(node['largest_jump_seconds'] >= 10 for node in clocks),
                'A large realtime jump relative to monotonic time was directly observed at 20 Hz')
        baseline = {node['node']: node.get('offset_seconds') for node in result['hosts_before']}
        h.check(all(node.get('offset_seconds') is not None and baseline.get(node['node']) is not None
                    and abs(node['offset_seconds']-baseline[node['node']]) < 2 for node in result['hosts_after']),
                'Every node clock returned within two seconds of its independently measured pre-fault offset')


if __name__ == '__main__':
    p = parser('Observe official node clock skew and automatic clock/read recovery')
    p.set_defaults(timeout=180)
    raise SystemExit(run('tester-agent/05-clock-skew', ['Official clock fault and recovery; not complete fencing proof'], p, scenario))
