import collections
import datetime as dt
import hashlib
import json
import time
import urllib.parse
from common import Failure, NS
from sandbox import Sandbox


class StorageSandbox(Sandbox):
    def __init__(self, h):
        super().__init__(h)
        self.stores = []
        self.node_observations = []

    def configure(self, config):
        config['P1_IMPORT_ONLY'] = 'true'

    def __enter__(self):
        super().__enter__()
        try:
            h = self.h
            backend = self.resource('Service', self.name + '-backends', spec={'clusterIP': 'None',
                'selector': {'app': self.name + '-agg'}, 'ports': [{'port': 8083, 'targetPort': 8083}]})
            images = json.loads(h.kube('get', 'daemonset', 'aggregator-gateway', '-o', 'json'))
            image = images['spec']['template']['spec']['containers'][0]['image']
            gateway, service = self.deployment(self.name + '-gateway', 'api', image, 8083,
                [{'name': 'AGGREGATOR_BACKEND_DNS', 'value': self.name + '-backends.' + NS + '.svc.cluster.local'},
                 {'name': 'GATEWAY_REPLICAS', 'value': '2'}], command=['/app/gateway'],
                envFrom=[{'secretRef': {'name': self.name}}])
            gateway['spec']['replicas'] = 2
            pod = gateway['spec']['template']['spec']
            pod['containers'][0]['readinessProbe']['httpGet']['path'] = '/ready'
            pod['dnsConfig'] = {'options': [{'name': 'use-vc'}, {'name': 'timeout', 'value': '1'},
                                            {'name': 'attempts', 'value': '2'}]}
            pod['affinity'] = {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [
                {'labelSelector': {'matchLabels': {'app': self.name + '-gateway'}},
                 'topologyKey': 'kubernetes.io/hostname'}]}, 'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {'nodeSelectorTerms': [{'matchExpressions': [
                    {'key': 'kubernetes.io/hostname', 'operator': 'In',
                     'values': [h.hosts.nodes[0]['name'], h.hosts.nodes[2]['name']]}]}]}}}
            self.apply([backend, gateway, service])
            h.kube('rollout', 'status', 'deployment/' + self.name + '-gateway', '--timeout=180s')
            h.kube('patch', 'deployment', self.name + '-api', '--type=strategic', '-p', json.dumps({
                'spec': {'template': {'spec': {'nodeSelector': {'kubernetes.io/hostname': h.hosts.nodes[0]['name']},
                    'containers': [{'name': 'app', 'env': [{'name': 'AGGREGATOR_URL',
                        'value': 'http://' + self.name + '-gateway:8083'}]}]}}}}))
            h.kube('rollout', 'status', 'deployment/' + self.name + '-api', '--timeout=180s')
            self.tunnel.terminate()
            self.tunnel.wait(timeout=10)
            ips = {name: h.kube('get', 'service', self.name + '-' + name, '-o', 'jsonpath={.spec.clusterIP}').strip()
                   for name in ('gateway', 'api', 'source')}
            self.open_tunnel({'aggregator': ips['gateway'] + ':8083', 'client': ips['api'] + ':8080',
                              'fixture': ips['source'] + ':8080'})
            return self
        except BaseException:
            from sandbox import sys_exc
            self.__exit__(*sys_exc())
            raise

    def database_targets(self, values):
        h = self.h
        for index, letter in enumerate('abc'):
            name = self.name + '-store-' + letter
            original = json.loads(h.kube('get', 'statefulset', 'postgres-' + letter, '-o', 'json'))
            spec = original['spec']
            labels = {'app': name, 'role': 'store', 'aat-e2e': self.suffix}
            spec['serviceName'] = name
            spec['selector'] = {'matchLabels': {'app': name}}
            spec['template']['metadata'] = {'labels': labels}
            pod = spec['template']['spec']
            pod['nodeSelector'] = {'kubernetes.io/hostname': h.hosts.nodes[index]['name']}
            pod.pop('affinity', None)
            for claim in spec['volumeClaimTemplates']:
                claim['metadata'] = {'name': claim['metadata']['name'], 'labels': {'aat-e2e': self.suffix}}
                claim.pop('status', None)
            statefulset = self.resource('StatefulSet', name, spec=spec)
            statefulset['apiVersion'] = 'apps/v1'
            service = self.resource('Service', name, spec={'selector': {'app': name},
                'ports': [{'port': 5432, 'targetPort': 5432}]})
            policy = self.resource('NetworkPolicy', name, spec={'podSelector': {'matchLabels': {'app': name}},
                'policyTypes': ['Ingress'], 'ingress': [
                    {'from': [{'podSelector': {'matchLabels': {'app': self.name + '-agg'}}}],
                     'ports': [{'port': 5432, 'protocol': 'TCP'}]},
                    {'from': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'tester-agent'}}}]}]})
            self.stores.append(name)
            self.apply([statefulset, service, policy])
            h.kube('rollout', 'status', 'statefulset/' + name, '--timeout=180s')
            key = 'P1_STORAGE_' + letter.upper() + '_URL'
            url = urllib.parse.urlsplit(values[key])
            authority = url.netloc.rsplit('@', 1)[0] + '@' + name + ':5432'
            values[key] = urllib.parse.urlunsplit(url._replace(netloc=authority, path='/aggregator'))
        h.artifact('temporary-storage.json', {'statefulsets': self.stores, 'nodes': [n['name'] for n in h.hosts.nodes],
                                             'data_policy': 'Only run-owned PVCs may be erased'})
        return [('aggregator-coordinator-0', 'aggregator_coord', 'COORDINATION_DATABASE_URL')]

    def __exit__(self, *exc):
        if exc and exc[0] is not None:
            for name in self.stores:
                try:
                    raw = self.sql_at(name + '-0', 'postgres', 'aggregator',
                        "SELECT COALESCE(json_agg(r),'[]'::json) FROM (SELECT pid,state,wait_event_type,wait_event,"
                        "extract(epoch FROM clock_timestamp()-xact_start) AS transaction_age_seconds,"
                        "pg_blocking_pids(pid) AS blocking_pids FROM pg_stat_activity WHERE datname=current_database()) r")
                    self.h.artifact(name + '-failure-transactions.json', json.loads(raw))
                except Exception as error:
                    self.h.note({'transaction_diagnostics_unavailable': name, 'error': str(error)})
        try:
            super().__exit__(*exc)
        finally:
            if self.stores:
                h = self.h
                claims = json.loads(h.kube('get', 'pvc', '-l', 'aat-e2e=' + self.suffix, '-o', 'json'))['items']
                h.check(all(p['metadata']['name'] in {'data-' + n + '-0' for n in self.stores} for p in claims),
                        'Cleanup targets only this run\'s temporary storage claims')
                h.kube('delete', 'statefulset', *self.stores, '--ignore-not-found=true', '--wait=true', '--timeout=90s')
                h.kube('delete', 'pvc', '-l', 'aat-e2e=' + self.suffix, '--wait=true', '--timeout=90s')
                h.note({'removed_temporary_storage': self.stores, 'data_recoverable': False,
                        'original_application_volumes_deleted': False})

    def import_events(self, events, require_success=True):
        results = []
        for start in range(0, len(events), 20):
            batch = events[start:start + 20]
            code, body, elapsed = self.h.request('aggregator', '/internal/import/events',
                {'X-Aggregator-Key': self.h.values['AGGREGATOR_API_KEY']}, batch, 'POST')
            self.h.check(code in (200, 503), 'Import responds with success or explicit unavailability, never HTTP 500')
            if require_success:
                self.h.check(code == 200 and body == {'accepted': len(batch), 'failed': 0},
                             'Every imported event has a successful acknowledgement')
            results.append({'status': code, 'body': body, 'elapsed': elapsed})
        return results

    def stats(self):
        code, body, _ = self.h.request('aggregator', '/internal/storage-stats',
                                     {'X-Aggregator-Key': self.h.values['AGGREGATOR_API_KEY']})
        self.h.check(code == 200, 'Physical storage placement is inspectable')
        return body

    def ready(self):
        state = self.stats()
        return state if len(state['units']) == 3 and all(u['status'] == 'ready' for u in state['units']) else None

    def checksum(self, source):
        code, body, _ = self.h.request('aggregator', '/internal/audit/checksum?' + urllib.parse.urlencode({'source': source}),
                                     {'Authorization': 'Bearer ' + self.h.values['P1_AUDIT_TOKEN']})
        self.h.check(code == 200, 'Audit checksum responds without an internal error')
        return body

    def available_audit(self):
        h, events, cursor, seen = self.h, [], None, set()
        for _ in range(1000):
            query = urllib.parse.urlencode({'limit': 1000, **({'cursor': cursor} if cursor else {})})
            code, page, _ = h.request('aggregator', '/internal/audit/events?' + query,
                                     {'Authorization': 'Bearer ' + h.values['P1_AUDIT_TOKEN']})
            h.check(code == 200, 'Degraded Audit API remains accessible')
            h.check(not page['partial'] or bool(page['unavailable_units']), 'Degraded audit labels unavailable units')
            events += page['items']
            cursor = page.get('next_cursor')
            if cursor is None:
                h.check(len({e['hazard_id'] for e in events}) == len(events), 'Degraded pagination contains no duplicate identity')
                return events
            h.check(cursor not in seen, 'Degraded audit cursor advances')
            seen.add(cursor)
        raise Failure('Degraded audit exceeded 1000 pages')

    def physical(self):
        rows = {}
        for name in self.stores:
            raw = self.sql_at(name + '-0', 'postgres', 'aggregator',
                "SELECT COALESCE(json_agg(r),'[]'::json) FROM (SELECT source,source_ref_id,hazard_id,partition_id,"
                "ingestion_version,revision FROM hazard_events ORDER BY source,source_ref_id) r")
            rows[name] = json.loads(raw)
        return rows

    def erase(self, index):
        h, name = self.h, self.stores[index]
        claim_name = 'data-' + name + '-0'
        claim = json.loads(h.kube('get', 'pvc', claim_name, '-o', 'json'))
        h.check(claim['metadata']['labels'].get('aat-e2e') == self.suffix,
                'Disk-loss target belongs exclusively to this test run')
        self.old_claim = claim['metadata']['uid']
        h.artifact(name + '-erased-claim.json', {'name': claim_name, 'uid': self.old_claim,
                    'volume': claim['spec']['volumeName'], 'node': h.hosts.nodes[index]['name']})
        h.kube('scale', 'statefulset/' + name, '--replicas=0')
        h.kube('wait', '--for=delete', 'pod/' + name + '-0', '--timeout=90s')
        h.kube('delete', 'pvc', claim_name, '--wait=true', '--timeout=90s')
        h.note({'erased_temporary_volume': claim_name, 'recoverable': False,
                'original_application_volumes_deleted': False})

    def replace(self, index):
        h, name = self.h, self.stores[index]
        h.kube('scale', 'statefulset/' + name, '--replicas=1')
        h.kube('rollout', 'status', 'statefulset/' + name, '--timeout=180s')
        claim = json.loads(h.kube('get', 'pvc', 'data-' + name + '-0', '-o', 'json'))
        h.check(claim['metadata']['uid'] != self.old_claim, 'Replacement has a genuinely new empty disk claim')

    def stop_worker(self, index):
        h, node = self.h, self.h.hosts.nodes[index]
        h.check(index > 0 and node['role'] == 'agent', 'Only a non-control-plane worker may be stopped')
        unit = self.name + '-restore'
        command = ('sudo -n systemd-run --unit=' + unit + ' --on-active=70s --timer-property=AccuracySec=1s '
                   '/usr/bin/systemctl start k3s-agent && '
                   'sudo -n systemctl stop k3s-agent && sudo -n /usr/local/bin/k3s-killall.sh')
        output = h.hosts.ssh(node, '{ ' + command + '; } 2>&1', capture=True)
        (h.folder / (unit + '.log')).write_bytes(output)
        h.note({'fault': 'worker_k3s_stopped_and_containers_killed', 'node': node['name'],
                'automatic_restart_seconds': 70, 'restore_unit': unit})

    def worker_ready(self, index):
        node = json.loads(self.h.kube('get', 'node', self.h.hosts.nodes[index]['name'], '-o', 'json'))
        self.node_observations.append({'observed_at': time.time(), 'node': node['metadata']['name'],
                                       'conditions': node['status']['conditions']})
        self.h.artifact('worker-state-history.json', self.node_observations)
        return any(c['type'] == 'Ready' and c['status'] == 'True' for c in node['status']['conditions'])

    def load(self, label):
        h = self.h
        snapshot = {'stats': self.stats(), 'cpu_memory': h.kube('top', 'pod', '-l', 'aat-e2e=' + self.suffix),
                    'nodes': h.kube('top', 'nodes')}
        h.artifact(label + '-load.json', snapshot)
        return snapshot

    def assert_dataset(self, events):
        h = self.h
        actual = h.complete_audit('')
        wanted = {(e['source'], e['source_ref_id']): e for e in events}
        h.check({(e['source'], e['source_ref_id']) for e in actual} == set(wanted),
                'Audit contains exactly the independent input identities')
        for event in actual:
            expected = wanted[(event['source'], event['source_ref_id'])]
            for key in ('hazard_type', 'severity', 'area_name', 'latitude', 'longitude', 'occurred_at', 'attributes'):
                h.check(event[key] == expected[key], 'Canonical field matches input: ' + key)
        for source in sorted({e['source'] for e in events}):
            refs = sorted(e['source_ref_id'].encode() for e in events if e['source'] == source)
            result = self.checksum(source)
            h.check(result['count'] == len(refs) and result['checksum'] == hashlib.sha256(b'\n'.join(refs)).hexdigest(),
                    'Checksum matches independent byte-order SHA-256 for ' + source)
        return actual


def fixture(count=300, start=0, skew=False):
    kinds = [('BMKG', 'SEISMIC'), ('BMKG', 'TSUNAMI'), ('PVMBG', 'VOLCANIC'),
             ('SATELIT', 'WILDFIRE'), ('SENSOR_PANTAI', 'COASTAL')]
    result = []
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    for i in range(start, start + count):
        source, hazard = ('BMKG', 'SEISMIC') if skew else kinds[i % len(kinds)]
        occurred = base + dt.timedelta(hours=(i // 2) % (31 * 24))
        result.append({'source': source, 'source_ref_id': f'p1-practice-{i:07d}', 'hazard_type': hazard,
            'severity': ['NORMAL', 'WASPADA', 'SIAGA', 'AWAS'][i % 4],
            'area_name': 'Sulawesi Tengah' if skew or i % 20 else 'Jawa Barat',
            'latitude': -1.0, 'longitude': 120.0, 'occurred_at': occurred.isoformat().replace('+00:00', 'Z'),
            'attributes': {'practice': True, 'sequence': i}})
    return result


def verify_analytics(h, events, query=''):
    code, data, _ = h.api('/analytics' + ('?' + query if query else ''))
    h.check(code == 200, 'Authorized analytical dashboard endpoint responds')
    for field, key in [('by_hazard', 'hazard_type'), ('by_severity', 'severity'), ('by_area', 'area_name')]:
        expected = collections.Counter(e[key] for e in events)
        h.check({k: v for k, v in data[field].items() if v} == dict(expected), 'Analytical ' + field + ' equals direct audit counting')
    daily = {}
    for event in events:
        daily.setdefault(event['occurred_at'][:10], collections.Counter())[event['hazard_type']] += 1
    actual = {d['date']: {k: v for k, v in d['counts'].items() if v} for d in data['daily']}
    h.check(actual == {k: dict(v) for k, v in daily.items()} and data['total'] == len(events),
            'Time series and total equal the independent audit aggregation')
    return data
