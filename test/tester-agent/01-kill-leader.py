from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from tester_agent import practice


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'This campaign runs from the laptop against the VPS cluster')
    for repetition in range(h.args.repeat):
        before = {uid: p for uid, p in h.pods().items() if p['app'] == 'aggregator'}
        h.check(len(before) >= 3 and all(p['ready'] for p in before.values()),
                'At least three Aggregators are Ready before fault injection')
        leases = h.sql("SELECT COALESCE(json_agg(row_to_json(l)),'[]'::json) FROM source_leases l")
        result = practice(h, 'kill-leader', repetition+1)
        removed = [p['name'] for uid, p in before.items() if uid not in result['after_pods']]
        h.artifact(f'round-{repetition+1}-leaders.json', {'leases_before': leases,
            'leases_after': h.sql("SELECT COALESCE(json_agg(row_to_json(l)),'[]'::json) FROM source_leases l"),
            'removed': removed})
        h.check(bool(removed), 'The official Agent actually replaced an existing Aggregator pod')
        h.note({'round': repetition+1, 'removed': removed,
                'scope': 'Official pod fault and observed read/recovery window; actual polling takeover is tested separately'})


if __name__ == '__main__':
    raise SystemExit(run('tester-agent/01-kill-leader', ['Official Aggregator crash and recovery'],
                        parser('Run official kill-leader practice with continuous read observation'), scenario))
