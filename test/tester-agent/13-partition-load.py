from pathlib import Path
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Failure, parser, run
from tester_agent import practice


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Combined network/load testing targets the VPS lab')
    failures = []
    for ordinal in range(1, h.args.repeat+1):
        output = h.folder / ('round-'+str(ordinal)+'-load')
        command = [sys.executable, str(Path(__file__).parents[1]/'p5/02-load.py'),
            '--vps', '--mode', 'priority', '--seconds', '120', '--repeat', '1',
            '--env-file', str(h.args.env_file), '--output', str(output)]
        with (h.folder/('round-'+str(ordinal)+'-load.log')).open('w') as log:
            load = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            error = None
            try:
                deadline = time.monotonic()+45
                while not (output/'round-1.log').exists():
                    h.check(load.poll() is None and time.monotonic()<deadline, 'Priority workload started before the official fault')
                    time.sleep(.5)
                time.sleep(10)
                h.check(load.poll() is None, 'Load is active when the partition starts')
                try:
                    practice(h, 'partition', ordinal)
                except Failure as exc:
                    error = str(exc)
                h.check(load.poll() is None, 'Load spans the complete partition and recovery observation')
                status = load.wait(timeout=200)
                h.note({'round': ordinal, 'fault': 'official partition', 'load': '300 Media VUs and 10 Field requests/second',
                    'load_seconds': 120, 'load_exit': status, 'fault_assertion': error,
                    'scope': 'Simultaneous network fault and priority traffic; not critical-alert delivery or whole-node-loss proof'})
                if status or error:
                    failures.append(ordinal)
            finally:
                if load.poll() is None:
                    os.killpg(load.pid, signal.SIGINT)
                    try:
                        load.wait(timeout=45)
                    except subprocess.TimeoutExpired:
                        os.killpg(load.pid, signal.SIGTERM)
                        load.wait(timeout=15)
    h.check(not failures, 'All combined fault/load rounds satisfy unchanged read and priority workload thresholds')


if __name__ == '__main__':
    p = parser('Observe actual official partition while priority traffic remains active')
    p.set_defaults(timeout=180)
    raise SystemExit(run('tester-agent/13-partition-load', ['Concurrent network fault and P5 priority workload'], p, scenario))
