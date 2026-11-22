import base64
import json
from pathlib import Path
import re
import secrets
import socket
import subprocess
import time
import urllib.parse
import uuid
from common import NS, Failure


class Sandbox:
    def __init__(self, h, satellite=False):
        self.h, self.satellite = h, satellite
        self.suffix = uuid.uuid4().hex[:10]
        self.name = 'aat-e2e-' + self.suffix
        self.database = 'aat_e2e_' + self.suffix
        self.token = secrets.token_urlsafe(32)
        self.created_databases = []
        self.original_urls, self.original_sql = dict(h.urls), h.sql
        self.tunnel = None
        self.topic_created = False

    def apply(self, objects):
        payload = json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects})
        if self.h.hosts:
            self.h.hosts.ssh(self.h.server, f'sudo -n k3s kubectl -n {NS} apply -f -', payload.encode(), capture=True)
        else:
            command = ['kubectl']
            if self.h.args.kubeconfig:
                command += ['--kubeconfig', str(self.h.args.kubeconfig)]
            subprocess.run(command + ['-n', NS, 'apply', '-f', '-'], input=payload, text=True, check=True, capture_output=True)

    def resource(self, kind, name, **fields):
        return {'apiVersion': 'apps/v1' if kind == 'Deployment' else ('networking.k8s.io/v1' if kind == 'NetworkPolicy' else 'v1'),
                'kind': kind, 'metadata': {'name': name, 'namespace': NS, 'labels': {'aat-e2e': self.suffix}}, **fields}

    def deployment(self, name, role, image, port, env, **container_extra):
        labels = {'app': name, 'role': role, 'aat-e2e': self.suffix}
        memory = ('256Mi', '512Mi') if role == 'aggregator' else (('64Mi', '128Mi') if role == 'mock' else ('128Mi', '256Mi'))
        container = {'name': 'app', 'image': image, 'imagePullPolicy': 'IfNotPresent', 'env': env,
                     'resources': {'requests': {'memory': memory[0]}, 'limits': {'memory': memory[1]}},
                     'readinessProbe': {'httpGet': {'path': '/ready' if role == 'aggregator' else '/health', 'port': port},
                                        'periodSeconds': 2, 'timeoutSeconds': 2, 'failureThreshold': 30}, **container_extra}
        spec = {'automountServiceAccountToken': False, 'containers': [container]}
        if role == 'aggregator':
            spec['dnsConfig'] = {'options': [{'name': 'use-vc'}, {'name': 'timeout', 'value': '1'},
                                            {'name': 'attempts', 'value': '2'}]}
            spec['affinity'] = {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [
                {'labelSelector': {'matchLabels': {'app': name}}, 'topologyKey': 'kubernetes.io/hostname'}]}}
        deployment = self.resource('Deployment', name, spec={'replicas': 3 if role == 'aggregator' else 1,
            'selector': {'matchLabels': {'app': name}}, 'template': {'metadata': {'labels': labels}, 'spec': spec}})
        if role == 'aggregator':
            deployment['spec']['strategy'] = {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 0, 'maxUnavailable': 1}}
        service = self.resource('Service', name, spec={'selector': {'app': name}, 'ports': [{'port': port, 'targetPort': port}]})
        return deployment, service

    def sql_at(self, pod, user, database, sql):
        return self.h.kube('exec', pod, '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', user, '-d', database, '-At', '-c', sql)

    def database_targets(self, values):
        return [('aggregator-coordinator-0', 'aggregator_coord', 'COORDINATION_DATABASE_URL')] + [
            ('postgres-' + letter.lower() + '-0', 'postgres', 'P1_STORAGE_' + letter + '_URL') for letter in 'ABC']

    def configure(self, config):
        pass

    def configure_credentials(self, values):
        pass

    def source_script(self):
        return (Path(__file__).parent / 'source.py').read_text()

    def __enter__(self):
        h = self.h
        h.changes()
        h.check(bool(h.hosts), 'Isolated E2E workloads require --vps')
        existing = json.loads(h.kube('get', 'deployments', '-l', 'aat-e2e', '-o', 'json'))['items']
        h.check(not existing, 'Run only one isolated E2E campaign at a time; shared database capacity is not multiplied')
        try:
            config = json.loads(h.kube('get', 'configmap', 'aggregator-config', '-o', 'json'))['data']
            secret = json.loads(h.kube('get', 'secret', 'aggregator-credentials', '-o', 'json'))['data']
            values = {k: base64.b64decode(v).decode() for k, v in secret.items()}
            for pod, admin, key in self.database_targets(values):
                owner = urllib.parse.unquote(urllib.parse.urlsplit(values[key]).username)
                self.sql_at(pod, admin, 'postgres', f'CREATE DATABASE {self.database} OWNER "{owner.replace(chr(34), chr(34)*2)}"')
                self.created_databases.append((pod, admin))
                url = urllib.parse.urlsplit(values[key])
                values[key] = urllib.parse.urlunsplit(url._replace(path='/' + self.database))
            values.update(SATELLITE_TOKEN=self.token if self.satellite else '', PVMBG_TOKEN=self.token, PANTAI_AUTH_TOKEN='')
            self.configure_credentials(values)
            source_url = 'http://' + self.name + '-source.' + NS + '.svc.cluster.local:8080'
            config.update(PVMBG_URL=source_url, BMKG_URL=source_url, SATELLITE_URL=source_url,
                          AGGREGATOR_POLL_INTERVAL_MS='100', PVMBG_ADAPTER_VERSION='1',
                          AGGREGATOR_START_SINCE='1970-01-01T00:00:00Z', AGGREGATOR_READ_CACHE_TTL_MS='0',
                          AGGREGATOR_SHARED_READ_PROJECTION='false', P1_IMPORT_ONLY='false',
                          KAFKA_TOPIC=self.name, KAFKA_CRITICAL_TOPIC='', KAFKA_BROKERS='kafka:9092')
            self.configure(config)
            h.kube('exec', 'kafka-0', '--', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092',
                   '--create', '--topic', self.name, '--partitions', '3', '--replication-factor', '1')
            self.topic_created = True
            deployments = json.loads(h.kube('get', 'deployments', '-o', 'json'))['items']
            image = lambda app: next(d['spec']['template']['spec']['containers'][0]['image'] for d in deployments if d['metadata']['name'] == app)
            config_obj = self.resource('ConfigMap', self.name, data=config)
            secret_obj = self.resource('Secret', self.name, stringData={**values, 'SOURCE_TOKEN': self.token})
            script = self.resource('ConfigMap', self.name + '-source', data={'server.py': self.source_script()})
            source, source_svc = self.deployment(self.name + '-source', 'mock', image('observability'), 8080,
                [{'name': 'SOURCE_TOKEN', 'valueFrom': {'secretKeyRef': {'name': self.name, 'key': 'SOURCE_TOKEN'}}}],
                command=['python3', '-u', '/fixture/server.py'], volumeMounts=[{'name': 'script', 'mountPath': '/fixture', 'readOnly': True}])
            source['spec']['template']['spec']['volumes'] = [{'name': 'script', 'configMap': {'name': self.name + '-source'}}]
            agg_image = image('aggregator')
            if h.args.image_tag:
                h.check(bool(re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}', h.args.image_tag)), 'Test image tag is valid')
                agg_image = 'aat/aggregator:' + h.args.image_tag
            agg, agg_svc = self.deployment(self.name + '-agg', 'aggregator', agg_image, 8083,
                [{'name': 'AGGREGATOR_INSTANCE_ID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}],
                envFrom=[{'configMapRef': {'name': self.name}}, {'secretRef': {'name': self.name}}])
            client, client_svc = self.deployment(self.name + '-api', 'api', image('client-api'), 8080,
                [{'name': 'AGGREGATOR_URL', 'value': 'http://' + self.name + '-agg.' + NS + '.svc.cluster.local:8083'}],
                envFrom=[{'configMapRef': {'name': 'client-api-config'}}, {'secretRef': {'name': 'client-api-credentials'}}])
            policy = self.resource('NetworkPolicy', self.name, spec={'podSelector': {'matchExpressions': [
                {'key': 'app', 'operator': 'In', 'values': ['postgres-a', 'postgres-b', 'postgres-c', 'aggregator-coordinator']}]},
                'policyTypes': ['Ingress'], 'ingress': [{'from': [{'podSelector': {'matchLabels': {'app': self.name + '-agg'}}}],
                                                     'ports': [{'port': 5432, 'protocol': 'TCP'}]}]})
            self.apply([config_obj, secret_obj, script, policy, source, source_svc, agg, agg_svc, client, client_svc])
            for deployment in (self.name + '-source', self.name + '-agg', self.name + '-api'):
                h.kube('rollout', 'status', 'deployment/' + deployment, '--timeout=180s')
            services = json.loads(h.kube('get', 'services', '-l', 'aat-e2e=' + self.suffix, '-o', 'json'))['items']
            ips = {s['metadata']['name']: s['spec']['clusterIP'] for s in services}
            self.open_tunnel({'client': ips[self.name + '-api'] + ':8080',
                              'aggregator': ips[self.name + '-agg'] + ':8083', 'fixture': ips[self.name + '-source'] + ':8080'})
            h.sql = lambda sql: json.loads(self.sql_at('aggregator-coordinator-0', 'aggregator_coord', self.database, sql).strip() or 'null')
            h.artifact(self.name + '-deployment.json', {'name': self.name, 'database': self.database,
                'dns_config': agg['spec']['template']['spec']['dnsConfig'],
                'pods': {uid: p for uid, p in h.pods().items() if (p['app'] or '').startswith(self.name)}})
            h.complete_audit('SATELIT' if self.satellite else 'PVMBG')
            return self
        except BaseException:
            self.__exit__(*sys_exc())
            raise

    def open_tunnel(self, targets):
        h = self.h
        if not h.hosts:
            raise Failure('The isolated VPS E2E suite currently requires --vps')
        command = h.hosts.command(h.server, '', multiplex=False)[:-2]
        for name, target in targets.items():
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            command += ['-L', f'127.0.0.1:{port}:{target}']
            h.urls[name] = f'http://127.0.0.1:{port}'
        command += ['-o', 'ExitOnForwardFailure=yes', '-N', h.server['user'] + '@' + h.server['host']]
        self.tunnel = subprocess.Popen(command)
        time.sleep(.5)
        h.check(self.tunnel.poll() is None, 'Isolated E2E tunnel established')

    def control(self, path, body=None):
        code, data, _ = self.h.request('fixture', path, {'Authorization': 'Bearer ' + self.token}, body,
                                      'POST' if body is not None else 'GET')
        self.h.check(code == 200, 'Controlled source request succeeded')
        return data

    def adapter(self, version):
        self.h.kube('patch', 'configmap', self.name, '--type=merge', '-p', json.dumps({'data': {'PVMBG_ADAPTER_VERSION': str(version)}}))
        self.h.kube('rollout', 'restart', 'deployment/' + self.name + '-agg')
        self.h.kube('rollout', 'status', 'deployment/' + self.name + '-agg', '--timeout=180s')

    def kill_owner(self):
        owner = self.h.sql("SELECT to_json(holder_id) FROM source_leases WHERE source='SATELIT'")
        self.h.check(owner.startswith(self.name + '-agg-'), 'Only the isolated E2E owner is a fault target')
        self.h.kube('delete', 'pod', owner, '--grace-period=0', '--force', '--wait=false')

    def __exit__(self, *exc):
        h = self.h
        if exc and exc[0] is not None:
            try:
                logs = h.kube('logs', '-l', 'aat-e2e=' + self.suffix, '--all-containers=true', '--prefix=true',
                              '--tail=10000', '--max-log-requests=5')
                (h.folder / (self.name + '-failure.log')).write_text(logs)
                h.artifact(self.name + '-failure-pods.json', {uid: p for uid, p in h.pods().items()
                                                           if (p['app'] or '').startswith(self.name)})
            except Exception as error:
                h.note({'failure_log_collection_error': str(error)})
        if self.tunnel:
            self.tunnel.terminate()
            try:
                self.tunnel.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.tunnel.kill()
                self.tunnel.wait()
        h.urls, h.sql = self.original_urls, self.original_sql
        if not re.fullmatch(r'aat_e2e_[0-9a-f]{10}', self.database):
            raise Failure('Refusing cleanup of an unverified database name')
        h.kube('delete', 'deployment,daemonset,service,configmap,secret,networkpolicy', '-l', 'aat-e2e=' + self.suffix,
               '--ignore-not-found=true', '--wait=true', '--timeout=90s')
        for pod, admin in self.created_databases:
            self.sql_at(pod, admin, 'postgres', f'DROP DATABASE IF EXISTS {self.database} WITH (FORCE)')
        if self.topic_created:
            h.kube('exec', 'kafka-0', '--', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092',
                   '--delete', '--topic', self.name, '--if-exists')
        h.note({'cleaned_e2e_run': self.name, 'temporary_databases_removed': len(self.created_databases)})


def sys_exc():
    import sys
    return sys.exc_info()
