from pathlib import Path
import hashlib
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Inconclusive, parser, run


def capture(h, iteration):
    before = h.satellite()
    events = h.audit('SATELIT')
    h.artifact(f'round-{iteration}-canonical-events.json', events)
    if not events:
        raise Inconclusive('No canonical wildfire event exists in this snapshot')
    for event in events:
        h.check(event['source'] == 'SATELIT' and event['hazard_type'] == 'WILDFIRE', 'Canonical event has the correct source/type')
        attributes = event['attributes']
        keys = attributes.get('detection_keys', [])
        h.check(len(keys) >= 2 and len(keys) == len(set(keys)), 'A fire contains at least two distinct detection identities')
        h.check(attributes.get('detection_count') == len(keys), 'Detection count matches the retained identity list')
        h.check(all(len(key.split('|')) == 5 for key in keys), 'Detection-key components retain the contractual format')
        h.check(bool(event['source_ref_id']) and bool(event['area_name']), 'Stable fire identity and area are present')
    refs = sorted(e['source_ref_id'] for e in events)
    checksum = hashlib.sha256('\n'.join(refs).encode()).hexdigest()
    h.note({'iteration': iteration, 'snapshot_count': len(events), 'snapshot_identity_checksum': checksum,
            'before': before, 'after': h.satellite()})


def scenario(h):
    for iteration in range(h.args.repeat):
        capture(h, iteration)
    return 'observed'


if __name__ == '__main__':
    raise SystemExit(run('p4/03-audit', ['P4.1'], parser('Capture and inspect canonical wildfire audit output'), scenario))
