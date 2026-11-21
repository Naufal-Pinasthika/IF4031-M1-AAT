from pathlib import Path
import sys
import urllib.parse
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Inconclusive, parser, run


def scenario(h):
    messages = h.unprocessed()
    message = next((m for m in messages if m['status'] == 'parked'), None)
    for _ in range(h.args.repeat):
        for role in ('FIELD', 'MEDIA'):
            for extra in ({}, {'X-Client-Role': 'ops', 'X-Client-ID': h.values['OPS_CLIENT_ID']}):
                h.check(h.api('/unprocessed', role, extra=extra)[0] == 403, role + ' cannot list failures, even with forged headers')
                h.check(h.api('/unprocessed/replay', role, {'source': 'PVMBG'}, extra)[0] == 403, role + ' cannot replay failures')
                if message:
                    path = '/unprocessed/' + urllib.parse.quote(message['message_id'], safe='')
                    h.check(h.api(path, role, extra=extra)[0] == 403, role + ' cannot inspect raw failed bytes')
        if message:
            h.raw_message(message['message_id'])
    h.check(h.request('client', '/unprocessed')[0] == 401, 'Unauthenticated inspection rejected')
    if not message:
        raise Inconclusive('No parked message exists to test raw-byte inspection. Run 02-poison.py first; list/replay denial checks are retained.')


if __name__ == '__main__':
    raise SystemExit(run('p3/03-ops-access', ['P3.5'], parser('Check Ops-only access to failed messages'), scenario))
