from pathlib import Path
import datetime as dt
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Inconclusive, parser, run


def scenario(h):
    h.check(h.args.seconds > 0 and h.args.interval > 0, 'Observation duration and sampling interval are positive')
    h.artifact('session.json', h.session())
    end = time.monotonic() + h.args.seconds
    samples = []
    while True:
        state = h.satellite()
        samples.append({'observed_at': dt.datetime.now(dt.timezone.utc).isoformat(), **state})
        h.note(samples[-1])
        h.artifact('samples.json', samples)
        if time.monotonic() >= end:
            break
        time.sleep(min(h.args.interval, max(0, end - time.monotonic())))
    offsets = [s['committed_offset'] for s in samples]
    h.check(offsets == sorted(offsets), 'Durable checkpoint never went backwards during observation')
    if offsets[-1] == offsets[0]:
        raise Inconclusive('No committed-offset progress observed; inspect backlog, feed activity and structured logs')
    return 'observed'


if __name__ == '__main__':
    p = parser('Observe live satellite progress, lag and processing latency')
    p.add_argument('--seconds', type=int, default=180)
    p.add_argument('--interval', type=int, default=5)
    raise SystemExit(run('p4/01-live-stream', ['P4.4', 'P4.7'], p, scenario))
