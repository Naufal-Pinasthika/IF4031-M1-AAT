import argparse
import datetime as dt
import json
import re
import shlex
from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS


def main():
    p = argparse.ArgumentParser(description='Roll out staged critical-lane images without resetting source state, offsets or application volumes')
    p.add_argument('--aggregator-tag', required=True)
    p.add_argument('--notifier-tag', required=True)
    p.add_argument('--topic', default='hazard.critical.v1')
    p.add_argument('--apply', action='store_true')
    a = p.parse_args()
    for value in (a.aggregator_tag, a.notifier_tag, a.topic):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', value):
            p.error('invalid image tag/topic')
    if not a.apply:
        print(json.dumps({'action': 'plan', 'topic': a.topic, 'images': [a.aggregator_tag, a.notifier_tag],
                          'retains': ['all PVCs', 'general topic', 'consumer offsets', 'Sensor Pantai deployment and data']}))
        return
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    server = h.nodes[0]
    def kube(*args):
        return h.ssh(server, 'sudo -n k3s kubectl -n '+NS+' '+shlex.join(args), capture=True).decode()
    def read(kind):
        return json.loads(kube('get', kind, '-o', 'json'))
    def claims():
        return {item['metadata']['name']: item['metadata']['uid'] for item in read('pvc')['items']}
    folder = ROOT / '.e2e' / ('critical-lane-rollout-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    folder.mkdir(parents=True)
    before = {'aggregator': read('deployment/aggregator'), 'notifier': read('statefulset/notifier-consumer'),
              'source': read('deployment/sensor-pantai'), 'pvcs': claims(),
              'aggregator_config': read('configmap/aggregator-config'), 'notifier_config': read('configmap/notifier-consumer-config')}
    (folder / 'before.json').write_text(json.dumps(before, indent=2))
    if a.topic == before['aggregator_config']['data']['KAFKA_TOPIC']:
        raise RuntimeError('Critical topic cannot equal the general topic')
    kube('exec', 'kafka-0', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m',
         '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--create', '--if-not-exists',
         '--topic', a.topic, '--partitions', '3', '--replication-factor', '1')
    for config, data in [('notifier-consumer-config', {'KAFKA_CRITICAL_TOPIC': a.topic}),
                         ('aggregator-config', {'KAFKA_CRITICAL_TOPIC': a.topic, 'COASTAL_LEASE_TTL_MS': '4000'})]:
        kube('patch', 'configmap', config, '--type=merge', '-p', json.dumps({'data': data}))
    for workload, container, tag in [('statefulset/notifier-consumer', 'notifier-consumer', a.notifier_tag),
                                      ('deployment/aggregator', 'aggregator', a.aggregator_tag)]:
        kube('set', 'image', workload, container+'=aat/'+container+':'+tag)
        kube('rollout', 'status', workload, '--timeout=600s')
    result = {'aggregator_ready': read('deployment/aggregator')['status'].get('readyReplicas', 0),
              'notifier_ready': read('statefulset/notifier-consumer')['status'].get('readyReplicas', 0),
              'original_pvcs_retained': claims() == before['pvcs'],
              'source_spec_unchanged': read('deployment/sensor-pantai')['spec'] == before['source']['spec'],
              'topic': a.topic, 'images': [a.aggregator_tag, a.notifier_tag]}
    (folder / 'result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)
    print('Evidence:', folder.relative_to(ROOT), flush=True)
    if not result['original_pvcs_retained'] or not result['source_spec_unchanged'] or result['aggregator_ready'] < 3 or result['notifier_ready'] < 1:
        raise RuntimeError('Rollout invariants were not satisfied')


if __name__ == '__main__':
    main()
