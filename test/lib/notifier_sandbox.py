import base64
import json
import urllib.parse

from coastal_sandbox import CoastalSandbox


class RedundantNotifierSandbox(CoastalSandbox):
    def __init__(self, h):
        super().__init__(h)
        self.notifier_database_created = False

    def primary(self):
        cluster = json.loads(self.h.kube('get', 'cluster/notifier-ha', '-o', 'json'))
        return cluster['status']['currentPrimary']

    def notifier_sql(self, query):
        return json.loads(self.sql_at(self.primary(), 'postgres', self.database, query).strip() or 'null')

    def __enter__(self):
        super().__enter__()
        h = self.h
        try:
            self.sql_at(self.primary(), 'postgres', 'postgres', f'CREATE DATABASE {self.database} OWNER notifier')
            self.notifier_database_created = True
            secret = json.loads(h.kube('get', 'secret/notifier-ha-app', '-o', 'json'))['data']
            user = base64.b64decode(secret['username']).decode()
            password = base64.b64decode(secret['password']).decode()
            url = 'postgresql://'+urllib.parse.quote(user, safe='')+':'+urllib.parse.quote(password, safe='')
            url += '@notifier-ha-rw:5432/'+self.database+'?sslmode=require'
            name = self.name+'-notifier'
            policy = self.resource('NetworkPolicy', name, spec={
                'podSelector': {'matchLabels': {'cnpg.io/cluster': 'notifier-ha'}},
                'policyTypes': ['Ingress'], 'ingress': [{'from': [{'podSelector': {'matchLabels': {'app': name}}}],
                    'ports': [{'port': 5432, 'protocol': 'TCP'}]}]})
            credentials = self.resource('Secret', name, stringData={'NOTIFIER_DATABASE_URL': url})
            old = json.loads(h.kube('get', 'deployment/'+name, '-o', 'json'))
            template = old['spec']['template']
            pod = template['spec']
            pod.pop('volumes', None)
            container = pod['containers'][0]
            container.pop('volumeMounts', None)
            container['envFrom'].append({'secretRef': {'name': name}})
            container['env'] = [{'name': 'NOTIFIER_REPLICA_ID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}]
            pod['affinity'] = {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{
                'labelSelector': {'matchLabels': {'app': name}}, 'topologyKey': 'kubernetes.io/hostname'}]}}
            workload = {'apiVersion': 'apps/v1', 'kind': 'StatefulSet',
                'metadata': {'name': name, 'namespace': old['metadata']['namespace'], 'labels': {'aat-e2e': self.suffix}},
                'spec': {'replicas': 3, 'serviceName': name, 'podManagementPolicy': 'Parallel',
                    'selector': old['spec']['selector'], 'template': template}}
            h.kube('delete', 'deployment/'+name, '--wait=true', '--timeout=90s')
            self.apply([credentials, policy, workload])
            h.kube('rollout', 'status', 'statefulset/'+name, '--timeout=180s')
            return self
        except BaseException:
            import sys
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *exc):
        try:
            if self.notifier_database_created:
                if exc and exc[0] is not None:
                    try:
                        logs = self.h.kube('logs', '-l', 'app='+self.name+'-notifier', '--prefix=true',
                                           '--tail=1000', '--max-log-requests=3')
                        self.h.artifact('notifier-failure.json', {'log': logs})
                    except Exception as error:
                        self.h.note({'notifier_failure_log_error': str(error)})
                self.h.kube('delete', 'statefulset/'+self.name+'-notifier', '--wait=true', '--timeout=90s', '--ignore-not-found=true')
            super().__exit__(*exc)
        finally:
            if self.notifier_database_created:
                self.sql_at(self.primary(), 'postgres', 'postgres', f'DROP DATABASE IF EXISTS {self.database} WITH (FORCE)')
                self.notifier_database_created = False
