import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Failure, NS, parser, run
from sandbox import Sandbox


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'This isolated gateway experiment requires --vps')
    s = Sandbox(h)
    try:
        deployments = json.loads(h.kube('get', 'deployments', '-o', 'json'))['items']
        python_image = next(d['spec']['template']['spec']['containers'][0]['image']
                            for d in deployments if d['metadata']['name'] == 'observability')
        script = s.resource('ConfigMap', s.name, data={'server.py':
            (Path(__file__).resolve().parents[1] / 'lib/gateway_source.py').read_text()})
        objects = [script]
        gateways = []
        for number in range(2):
            name = s.name + '-gateway-' + str(number)
            gateways.append(name)
            deployment, service = s.deployment(name, 'other', python_image, 8083, [],
                command=['python3', '-u', '/fixture/server.py'],
                volumeMounts=[{'name': 'script', 'mountPath': '/fixture', 'readOnly': True}])
            pod = deployment['spec']['template']
            pod['metadata']['labels']['test-gateway'] = s.suffix
            pod['spec']['nodeSelector'] = {'kubernetes.io/hostname': h.server['name']}
            pod['spec']['terminationGracePeriodSeconds'] = 2
            pod['spec']['volumes'] = [{'name': 'script', 'configMap': {'name': s.name}}]
            objects += [deployment, service]
        headless = s.resource('Service', s.name+'-gateways', spec={'clusterIP': 'None',
            'selector': {'test-gateway': s.suffix}, 'ports': [{'port': 8083, 'targetPort': 8083}]})
        client, service = s.deployment(s.name+'-api', 'api', 'aat/client-api:'+h.args.client_image_tag, 8080,
            [{'name': 'AGGREGATOR_URL', 'value': 'http://'+s.name+'-gateways.'+NS+'.svc.cluster.local:8083'}],
            envFrom=[{'configMapRef': {'name': 'client-api-config'}}, {'secretRef': {'name': 'client-api-credentials'}}])
        client['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': h.server['name']}
        client['spec']['template']['spec']['terminationGracePeriodSeconds'] = 2
        objects += [headless, client, service]
        s.apply(objects)
        for name in gateways+[s.name+'-api']:
            h.kube('rollout', 'status', 'deployment/'+name, '--timeout=120s')
        services = json.loads(h.kube('get', 'services', '-l', 'aat-e2e='+s.suffix, '-o', 'json'))['items']
        ips = {v['metadata']['name']: v['spec']['clusterIP'] for v in services}
        s.open_tunnel({'client': ips[s.name+'-api']+':8080', **{
            'gateway'+str(i): ips[name]+':8083' for i, name in enumerate(gateways)}})
        for repetition in range(h.args.repeat):
            # A new API process establishes a new pool only between fault windows.
            if repetition:
                h.kube('rollout', 'restart', 'deployment/'+s.name+'-api')
                h.kube('rollout', 'status', 'deployment/'+s.name+'-api', '--timeout=120s')
            def reachable():
                try:
                    return h.request('client', '/health')[0] == 200
                except Failure as error:
                    h.note({'between_round_endpoint_wait': str(error)})
                    return False
            h.wait(reachable, 'The temporary API endpoint converges before the fault window')
            before = [h.request('gateway'+str(i), '/state')[1]['calls'] for i in range(2)]
            code, page, _ = h.api('/events?limit=20', 'FIELD')
            h.check(code == 200 and isinstance(page.get('events'), list), 'Warm read uses real canonical events')
            after = [h.request('gateway'+str(i), '/state')[1]['calls'] for i in range(2)]
            owners = [i for i in range(2) if after[i] > before[i]]
            h.check(len(owners) == 1, 'The actual keep-alive gateway is identified before the fault')
            owner = owners[0]
            h.check(h.request('gateway'+str(owner), '/freeze', method='POST')[0] == 200,
                    'Only the run-owned gateway listener is frozen')
            # Allow the run-owned listener shutdown to finish, not a source clock.
            time.sleep(.6)
            code, page, seconds = h.api('/events?limit=20', 'FIELD')
            h.check(code == 200 and isinstance(page.get('events'), list) and seconds < 3,
                    'Field read survives a silent pooled gateway within three seconds')
            logs = h.kube('logs', 'deployment/'+s.name+'-api', '--tail=100')
            h.artifact('round-'+str(repetition+1)+'.json', {'seconds': seconds, 'owner': owner, 'client_log': logs})
            h.check('client_gateway_read_failover' in logs, 'The measured request exercised client failover')
            h.note({'round': repetition+1, 'field_seconds': seconds, 'frozen_gateway': owner})
            h.kube('delete', 'pod', '-l', 'app='+gateways[owner], '--wait=true', '--timeout=60s')
            h.kube('rollout', 'status', 'deployment/'+gateways[owner], '--timeout=120s')
    finally:
        s.__exit__(*sys.exc_info())


if __name__ == '__main__':
    p = parser('Measure safe Client API reads through a failed run-owned gateway and real canonical services')
    p.add_argument('--client-image-tag', required=True)
    raise SystemExit(run('tester-agent/09-gateway-failover', ['Client API read continuity during gateway loss'], p, scenario))
