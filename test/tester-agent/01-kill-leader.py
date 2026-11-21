from pathlib import Path
import json
import shlex
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run, Failure


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'This campaign runs from the laptop against the VPS cluster')
    for repetition in range(h.args.repeat):
        before = h.pods()
        agg_before = {uid: p for uid, p in before.items() if p['app'] == 'aggregator'}
        h.check(len(agg_before) >= 3 and all(p['ready'] for p in agg_before.values()), 'At least three Aggregators are Ready before fault injection')
        leases_before = h.sql("SELECT COALESCE(json_agg(row_to_json(l)),'[]'::json) FROM source_leases l")
        command = ['sudo', '-n', 'k3s', 'kubectl', '-n', 'tester-agent', 'exec', 'daemonset/tester-agent', '--', '/app', 'run', 'practice', 'kill-leader']
        samples = []
        started = time.monotonic()
        with (h.folder / f'round-{repetition+1}-agent.log').open('wb') as log:
            process = subprocess.Popen(h.hosts.command(h.server, shlex.join(command)), stdout=log, stderr=subprocess.STDOUT)
            try:
                while process.poll() is None:
                    try:
                        code, _, seconds = h.request('aggregator', '/ready')
                        samples.append({'at_seconds': time.monotonic()-started, 'status': code, 'seconds': seconds})
                    except Failure as error:
                        samples.append({'at_seconds': time.monotonic()-started, 'error': str(error)})
                    if time.monotonic()-started > 120:
                        raise Failure('Practice command exceeded 120 seconds; inspect agent logs before running another scenario')
                    time.sleep(.5)
            finally:
                if process.poll() is None:
                    # Stop only our local observation connection, not the Agent.
                    process.terminate()
                    process.wait(timeout=10)
        code = process.returncode
        # The CLI submits a scheduled request and exits before the Agent acts.
        # Wait for the actual pod identity change, not the submission exit.
        deadline = time.monotonic()+h.args.timeout
        while code == 0:
            after = h.pods()
            if any(uid not in after for uid in agg_before):
                break
            if time.monotonic() >= deadline:
                break
            try:
                status, _, seconds = h.request('aggregator', '/ready')
                samples.append({'at_seconds': time.monotonic()-started, 'status': status, 'seconds': seconds})
            except Failure as error:
                samples.append({'at_seconds': time.monotonic()-started, 'error': str(error)})
            time.sleep(1)
        after = h.pods()
        removed = [p for uid, p in agg_before.items() if uid not in after]
        h.artifact(f'round-{repetition+1}-injection.json', {'agent_exit': code, 'removed_aggregators': removed,
                    'before': before, 'after_injection': after, 'readiness_samples': samples, 'leases_before': leases_before})
        h.artifact(f'round-{repetition+1}-daemon-logs.json', {'log': h.kube('-n', 'tester-agent', 'logs',
                    '-l', 'app=tester-agent', '--since=5m', '--prefix', '--tail=100', '--max-log-requests=3')})
        h.check(code == 0, 'Official kill-leader practice command succeeds')
        h.check(bool(removed), 'The Agent actually removed an existing Aggregator pod')
        recovery_started = time.monotonic()
        def recovered():
            current = [p for p in h.pods().values() if p['app'] == 'aggregator']
            return current if len(current) == len(agg_before) and all(p['ready'] for p in current) else None
        recovered_pods = h.wait(recovered, 'Aggregator replicas did not become Ready again')
        elapsed = time.monotonic()-started
        h.check(h.request('aggregator', '/ready')[0] == 200, 'The common entrypoint is Ready after recovery')
        h.artifact(f'round-{repetition+1}-recovery.json', {'elapsed_seconds': elapsed,
                    'readiness_recovery_wait_seconds': time.monotonic()-recovery_started, 'pods': recovered_pods,
                    'leases_after': h.sql("SELECT COALESCE(json_agg(row_to_json(l)),'[]'::json) FROM source_leases l")})
        h.note({'round': repetition+1, 'removed': [p['name'] for p in removed], 'elapsed_seconds': elapsed,
                'scope': 'Agent pod kill and replica readiness recovery; not proof of upstream exactly-once or the 60-second ingestion takeover criterion'})


if __name__ == '__main__':
    raise SystemExit(run('tester-agent/01-kill-leader', ['Tester Agent practice', 'Aggregator crash recovery'],
                        parser('Run official kill-leader practice repeatedly without manual recovery'), scenario))
