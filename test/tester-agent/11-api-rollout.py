import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    h.changes()
    h.auth('FIELD')
    for repetition in range(h.args.repeat):
        before = {uid for uid, p in h.pods().items() if p['app'] == 'client-api'}
        stop, samples = threading.Event(), []
        def observe():
            while not stop.is_set():
                try:
                    code, page, seconds = h.api('/events?limit=20', 'FIELD')
                    samples.append({'status': code, 'seconds': seconds,
                                    'valid': code == 200 and isinstance(page.get('events'), list)})
                except Exception as error:
                    samples.append({'valid': False, 'error': str(error)})
                stop.wait(.25)
        observer = threading.Thread(target=observe, daemon=True)
        observer.start()
        try:
            time.sleep(1)
            h.kube('rollout', 'restart', 'deployment/client-api')
            h.kube('rollout', 'status', 'deployment/client-api', '--timeout=180s')
            time.sleep(7)
        finally:
            stop.set()
            observer.join(timeout=30)
            h.artifact('round-'+str(repetition+1)+'.json', samples)
        h.check(not observer.is_alive(), 'Field observer joined before closing its tunnel')
        after = {uid for uid, p in h.pods().items() if p['app'] == 'client-api' and p['ready']}
        h.check(bool(after) and before.isdisjoint(after), 'The actual production Client API pod was replaced')
        failures = sum(not s['valid'] for s in samples)
        h.note({'round': repetition+1, 'samples': len(samples), 'failed_field_reads': failures})
        h.check(bool(samples) and failures == 0, 'Every Field read succeeded across the full Client API rollout')


if __name__ == '__main__':
    raise SystemExit(run('tester-agent/11-api-rollout', ['Client API rolling restart availability'],
                         parser('Observe Field reads while rolling the existing Client API without changing data or replicas'), scenario))
