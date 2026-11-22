from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from tester_agent import practice


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Official storage faults run on the VPS lab')
    for repetition in range(h.args.repeat):
        result = practice(h, 'kill-store', repetition+1)
        h.check(any(re.search(r'pod .*role=store.*dihentikan', row.get('action', '')) for row in result['actions']),
                'Agent actually stopped a store pod, not merely queued a scenario')


if __name__ == '__main__':
    p = parser('Run official kill-store practice and observe reads plus automatic recovery')
    p.set_defaults(timeout=180)
    raise SystemExit(run('tester-agent/04-kill-store', ['Official store fault and recovery'], p, scenario))
