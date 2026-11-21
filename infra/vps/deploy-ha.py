import argparse
import base64
import datetime as dt
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import time
from urllib.parse import quote

from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS

OPERATOR = 'https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.27/releases/cnpg-1.27.4.yaml'
KAFKA_IMAGE = 'apache/kafka:4.1.2'


class Rollout:
    def __init__(self, args):
        self.args = args
        self.h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
        self.server = self.h.nodes[0]
        self.folder = args.state_dir
        self.folder.mkdir(parents=True, exist_ok=True, mode=0o700)

    def kube(self, *args, payload=None, namespace=NS, timeout=180):
        cmd = ['sudo', '-n', 'k3s', 'kubectl']
        if namespace:
            cmd += ['-n', namespace]
        result = subprocess.run(self.h.command(self.server, shlex.join(cmd+list(args))),
                                input=payload, capture_output=True, timeout=timeout)
        if result.returncode:
            # Payloads can contain secrets. kubectl can echo rejected objects;
            # keep raw errors private instead of displaying their contents.
            (self.folder / 'last-error.log').write_bytes(result.stderr)
            raise RuntimeError('kubectl failed: '+shlex.join(list(args)[:3])+'. See private last-error.log')
        return result.stdout.decode()

    def read(self, name):
        return json.loads(self.kube('get', name, '-o', 'json'))

    def apply(self, items):
        self.kube('apply', '-f', '-', payload=json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': items}).encode())

    def patch(self, name, patch):
        self.kube('patch', name, '--type=merge', '-p', json.dumps(patch))

    def save(self, name, value):
        path = self.folder / name
        if not path.exists():
            path.write_text(json.dumps(value, indent=2))
            path.chmod(0o600)

    def mark(self, stage):
        (self.folder / (stage+'.done')).write_text(dt.datetime.now(dt.timezone.utc).isoformat())
        print(stage+': complete', flush=True)

    def completed(self, stage):
        return (self.folder / (stage+'.done')).exists()

    def prepare(self):
        self.save('original-pvcs.json', self.read('pvc'))
        self.save('original-sensor.json', self.read('deployment/sensor-pantai'))
        self.kube('apply', '--server-side', '-f', OPERATOR, namespace=None)
        self.kube('patch', 'deployment/cnpg-controller-manager', '--type=merge', '-p', json.dumps({'spec': {'template': {
            'metadata': {'labels': {'role': 'other'}}, 'spec': {
                'nodeSelector': {'node-role.kubernetes.io/control-plane': 'true'}}}}}), namespace='cnpg-system')
        self.kube('rollout', 'status', 'deployment/cnpg-controller-manager', '--timeout=180s', namespace='cnpg-system', timeout=200)
        self.kube('set', 'resources', 'deployment/cnpg-controller-manager', '--requests=memory=128Mi',
                  '--limits=memory=256Mi', namespace='cnpg-system')
        # Existing coordinator credentials remain unchanged. Notifier has its
        # own credentials and database; never connect it to Aggregator storage.
        old = self.read('secret/aggregator-coordinator-credentials')['data']
        password = base64.b64decode(old['POSTGRES_PASSWORD']).decode()
        for name, user, value in [('coordinator-ha-app', 'aggregator_coord', password),
                                  ('notifier-ha-app', 'notifier', secrets.token_urlsafe(36))]:
            found = json.loads(self.kube('get', 'secret', name, '--ignore-not-found', '-o', 'json') or '{}')
            if not found:
                self.apply([{'apiVersion': 'v1', 'kind': 'Secret', 'type': 'kubernetes.io/basic-auth',
                             'metadata': {'name': name, 'namespace': NS}, 'stringData': {'username': user, 'password': value}}])
        for filename in ('network-policies.yaml', 'databases.yaml'):
            data = (ROOT / 'k8s/ha' / filename).read_bytes()
            self.kube('apply', '--dry-run=server', '-f', '-', payload=data)
            self.kube('apply', '-f', '-', payload=data)
        for name in ('coordinator-ha', 'notifier-ha'):
            self.kube('wait', '--for=condition=Ready', 'cluster/'+name, '--timeout=600s', timeout=620)
        self.mark('prepare')

    def kafka_cli(self, pod, tool, *args, payload=None):
        return self.kube('exec', '-i', pod, '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m',
                         '/opt/kafka/bin/'+tool+'.sh', *args, payload=payload)

    def kafka(self):
        if self.completed('kafka'):
            print('kafka: already completed; use the HA verification test to inspect runtime', flush=True)
            return
        self.save('kafka-workload.json', self.read('statefulset/kafka'))
        self.save('kafka-config.json', self.read('configmap/kafka-config'))
        self.kube('set', 'image', 'statefulset/kafka', 'kafka='+KAFKA_IMAGE)
        self.kube('rollout', 'status', 'statefulset/kafka', '--timeout=300s', timeout=320)
        version = self.kafka_cli('kafka-0', 'kafka-features', '--bootstrap-server', 'localhost:9092', 'describe')
        if not re.search(r'kraft.version.*FinalizedVersionLevel:\s*1', version):
            # The static voter's address is persisted by the feature upgrade.
            # localhost would strand the other controllers on their own pods.
            address = '1@kafka-0.kafka.'+NS+'.svc.cluster.local:9093'
            self.patch('configmap/kafka-config', {'data': {'KAFKA_CONTROLLER_QUORUM_VOTERS': address}})
            self.kube('rollout', 'restart', 'statefulset/kafka')
            self.kube('rollout', 'status', 'statefulset/kafka', '--timeout=300s', timeout=320)
            self.kafka_cli('kafka-0', 'kafka-features', '--bootstrap-server', 'localhost:9092',
                           'upgrade', '--feature', 'kraft.version=1')
        cm = self.read('configmap/kafka-config')
        cm['data'].pop('KAFKA_CONTROLLER_QUORUM_VOTERS', None)
        bootstrap = ','.join('kafka-%d.kafka.%s.svc.cluster.local:9093' % (i, NS) for i in range(3))
        cm['data'].update(KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS=bootstrap,
                          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR='3', KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR='3',
                          KAFKA_TRANSACTION_STATE_LOG_MIN_ISR='2', KAFKA_DEFAULT_REPLICATION_FACTOR='3',
                          KAFKA_MIN_INSYNC_REPLICAS='2', KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE='false')
        cm['data'].pop('KAFKA_NODE_ID', None)
        cm['data'].pop('KAFKA_ADVERTISED_LISTENERS', None)
        self.apply([cm])
        launcher = (ROOT / 'k8s/ha/kafka-start.sh').read_text()
        self.apply([{'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'kafka-ha-start', 'namespace': NS},
                     'data': {'start.sh': launcher}}])
        workload = self.read('statefulset/kafka')
        spec = workload['spec']['template']['spec']
        spec['affinity'] = {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{
            'labelSelector': {'matchLabels': {'app': 'kafka'}}, 'topologyKey': 'kubernetes.io/hostname'}]}}
        spec['volumes'] = [{'name': 'start', 'configMap': {'name': 'kafka-ha-start', 'defaultMode': 365}}]
        c = spec['containers'][0]
        c['command'] = ['/bin/bash', '/ha/start.sh']
        c['volumeMounts'] = [m for m in c['volumeMounts'] if m['name'] != 'start'] + [
            {'name': 'start', 'mountPath': '/ha', 'readOnly': True}]
        c['env'] = [{'name': 'POD_NAME', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}]
        # Update ordinal zero before expanding. Its original PVC and node ID 1
        # stay intact; new controllers start as observers, not a new cluster.
        self.patch('statefulset/kafka', {'spec': {'template': {'spec': spec}}})
        self.kube('rollout', 'status', 'statefulset/kafka', '--timeout=300s', timeout=320)
        self.kube('scale', 'statefulset/kafka', '--replicas=3')
        self.kube('rollout', 'status', 'statefulset/kafka', '--timeout=600s', timeout=620)
        for pod in ('kafka-1', 'kafka-2'):
            status = self.kafka_cli('kafka-0', 'kafka-metadata-quorum', '--bootstrap-server', 'localhost:9092', 'describe', '--status')
            node_id = int(pod.rsplit('-', 1)[1])+1
            voters = status.split('CurrentVoters:', 1)[1].split('\n', 1)[0]
            if not re.search(r'"id"\s*:\s*'+str(node_id)+r'\b', voters):
                self.kafka_cli(pod, 'kafka-metadata-quorum', '--command-config', '/opt/kafka/config/server.properties',
                               '--bootstrap-server', 'localhost:9092', 'add-controller')
        description = self.kafka_cli('kafka-0', 'kafka-topics', '--bootstrap-server', 'localhost:9092', '--describe')
        plan = reassignment(description)
        if plan['partitions']:
            self.save('kafka-reassignment.json', plan)
            self.kube('exec', '-i', 'kafka-0', '--', 'sh', '-ec', 'umask 077; dd of=/tmp/aat-ha-reassignment.json status=none', payload=json.dumps(plan).encode())
            self.kafka_cli('kafka-0', 'kafka-reassign-partitions', '--bootstrap-server', 'localhost:9092',
                           '--reassignment-json-file', '/tmp/aat-ha-reassignment.json', '--execute')
            end = time.monotonic()+600
            while True:
                output = self.kafka_cli('kafka-0', 'kafka-reassign-partitions', '--bootstrap-server', 'localhost:9092',
                                        '--reassignment-json-file', '/tmp/aat-ha-reassignment.json', '--verify')
                if 'still in progress' not in output:
                    break
                if time.monotonic() > end:
                    raise RuntimeError('Kafka reassignment has not finished; no data/offsets were reset')
                time.sleep(2)
        topic = self.read('configmap/aggregator-config')['data']['KAFKA_TOPIC']
        for name in (topic, self.args.critical_topic):
            self.kafka_cli('kafka-0', 'kafka-topics', '--bootstrap-server', 'localhost:9092', '--create', '--if-not-exists',
                           '--topic', name, '--partitions', '3', '--replication-factor', '3')
            self.kafka_cli('kafka-0', 'kafka-configs', '--bootstrap-server', 'localhost:9092', '--alter',
                           '--entity-type', 'topics', '--entity-name', name, '--add-config', 'min.insync.replicas=2,unclean.leader.election.enable=false')
        brokers = ','.join('kafka-%d.kafka:9092' % i for i in range(3))
        for name in ('aggregator', 'notifier-consumer', 'dashboard-consumer'):
            self.patch('configmap/'+name+'-config', {'data': {'KAFKA_BROKERS': brokers}})
        self.save('kafka-quorum-after.json', {'status': self.kafka_cli('kafka-0', 'kafka-metadata-quorum',
                                                                    '--bootstrap-server', 'localhost:9092', 'describe', '--status')})
        self.mark('kafka')


def reassignment(description):
    partitions = []
    for line in description.splitlines():
        match = re.search(r'Topic:\s*(\S+)\s+Partition:\s*(\d+).*?Replicas:\s*([\d,]+)', line)
        if not match:
            continue
        topic, partition, replicas = match.groups()
        old = [int(v) for v in replicas.split(',')]
        if len(old) > 3 or any(v not in (1, 2, 3) for v in old):
            raise ValueError('unexpected existing Kafka replica assignment; refusing to overwrite')
        if len(old) < 3:
            # Rotate preferred leaders without changing offsets or topic IDs.
            first = int(partition) % 3 + 1
            order = [first, first % 3 + 1, (first + 1) % 3 + 1]
            partitions.append({'topic': topic, 'partition': int(partition), 'replicas': order})
    return {'version': 1, 'partitions': partitions}


def main():
    p = argparse.ArgumentParser(description='Stage warning-path HA without deleting legacy volumes, source state or Kafka offsets')
    p.add_argument('stage', choices=['prepare', 'kafka'])
    p.add_argument('--apply', action='store_true')
    p.add_argument('--state-dir', type=Path, default=ROOT / '.e2e/ha-rollout')
    p.add_argument('--critical-topic', default='hazard.critical.v1')
    args = p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', args.critical_topic):
        p.error('invalid critical topic')
    if not args.apply:
        print(json.dumps({'stage': args.stage, 'mode': 'plan', 'preserves': ['legacy PVCs', 'source state', 'topic identities and offsets'],
                          'changes': 'CNPG operator and two independent 3-instance databases' if args.stage == 'prepare' else
                          'Kafka 4.1.2, dynamic 3-controller quorum, 3 brokers and existing partition reassignment'}))
        return
    os.umask(0o077)
    rollout = Rollout(args)
    getattr(rollout, args.stage)()


if __name__ == '__main__':
    main()
