from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    for service, path in [('client', '/health'), ('auth', '/health'), ('aggregator', '/ready')]:
        h.check(h.request(service, path)[0] == 200, service + ' is ready')
    h.unprocessed()
    pods = h.pods()
    receivers = [p for p in pods.values() if p['app'] == 'aggregator' and p['ready']]
    h.check(len(receivers) >= 3, 'At least three Aggregators are Ready')
    h.check(len({p['node'] for p in receivers}) >= 3, 'Aggregators span distinct nodes in this three-node lab')
    config = json.loads(h.kube('get', 'configmap', 'aggregator-config', '-o', 'json'))['data']
    h.artifact('deployment.json', {'pods': pods, 'adapter': config.get('PVMBG_ADAPTER_VERSION', '1')})
    h.note({'adapter': config.get('PVMBG_ADAPTER_VERSION', '1'), 'ready_receivers': len(receivers)})


if __name__ == '__main__':
    raise SystemExit(run('p3/00-preflight', [], parser('Check P3 deployment prerequisites'), scenario))
