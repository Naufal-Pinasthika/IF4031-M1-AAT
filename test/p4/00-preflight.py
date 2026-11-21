from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    session = h.session()
    state = h.satellite()
    h.artifact('session.json', session)
    h.artifact('stream-state.json', state)
    h.artifact('deployment.json', h.pods())
    h.check(isinstance(session.get('latest_offset'), int), 'Feed session exposes a numeric latest offset')
    h.check(isinstance(state.get('committed_offset'), int), 'Application exposes a durable stream checkpoint')
    h.note({'profile': session.get('profile'), 'speed': session.get('speed'), 'state': state})


if __name__ == '__main__':
    raise SystemExit(run('p4/00-preflight', [], parser('Check configured satellite-feed prerequisites'), scenario))
