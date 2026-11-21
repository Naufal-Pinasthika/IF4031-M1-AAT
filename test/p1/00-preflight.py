from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


def scenario(h):
    h.check(h.request('aggregator', '/ready')[0] == 200, 'Aggregator common entrypoint is ready')
    nodes = json.loads(h.kube('get', 'nodes', '-o', 'json'))['items']
    h.check(len(nodes) >= 3, 'Current three-unit manifests have three distinct Kubernetes nodes')
    stores = [p for p in h.pods().values() if p['app'] in ('postgres-a', 'postgres-b', 'postgres-c')]
    h.check(len(stores) == 3 and all(p['ready'] for p in stores), 'All canonical units are ready')
    h.check(len({p['node'] for p in stores}) == 3, 'Storage replicas are placed on distinct nodes')
    for pod in stores:
        resources = next(iter(pod['resources'].values()))
        h.check(resources['requests']['memory'] == '512Mi' and resources['limits']['memory'] == '1Gi',
                'Storage memory matches specification resource table')
    claims = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
    volumes = json.loads(h.kube('get', 'pv', '-o', 'json'))['items']
    for volume in volumes:
        if volume['spec'].get('claimRef', {}).get('name') in {'data-postgres-' + c + '-0' for c in 'abc'}:
            h.check(bool(volume['spec'].get('local') or volume['spec'].get('hostPath')),
                    'Canonical data is node-local, not shared network storage')
    snapshot = {'nodes': nodes, 'stores': stores,
        'pvc_uids': {c['metadata']['name']: c['metadata']['uid'] for c in claims},
        'tester_agent_uid': json.loads(h.kube('-n', 'tester-agent', 'get', 'daemonset', 'tester-agent', '-o', 'json'))['metadata']['uid']}
    h.artifact('baseline.json', snapshot)
    if h.args.baseline:
        previous = json.loads(h.args.baseline.read_text())
        h.check(all(snapshot['pvc_uids'].get(name) == uid for name, uid in previous['pvc_uids'].items()),
                'All original application PVC identities were retained')
        h.check(snapshot['tester_agent_uid'] == previous['tester_agent_uid'], 'Official Tester Agent was not replaced')
        h.check(not json.loads(h.kube('get', 'deployments', '-l', 'aat-e2e', '-o', 'json'))['items'],
                'No temporary E2E applications remain')
        h.check(not any(c['metadata'].get('labels', {}).get('aat-e2e') for c in claims),
                'No temporary E2E storage claims remain')
    h.note({'dataset': 'Assistant catalog/checksum deferred to their release; practice data is explicitly labelled',
            'ready_storage_nodes': [p['node'] for p in stores]})


if __name__ == '__main__':
    p = parser('Check P1 topology, storage resources and audit prerequisites')
    p.add_argument('--baseline', type=Path, help='Earlier baseline.json to verify retained original volumes and cleanup')
    raise SystemExit(run('p1/00-preflight', [], p, scenario))
