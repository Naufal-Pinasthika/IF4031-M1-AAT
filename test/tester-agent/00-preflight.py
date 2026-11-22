from pathlib import Path
import json
import shlex
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run

IMAGE = 'sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec'


def scenario(h):
    if h.args.vps:
        tools = []
        probe = '''import json,os,pathlib,subprocess
p=pathlib.Path('/usr/sbin/iptables')
link=os.readlink(p) if p.is_symlink() else None
print(json.dumps({'path':str(p),'resolved':str(p.resolve(strict=True)),'link':link,
 'host_mount_visible':not link or not link.startswith('/'),
 'version':subprocess.check_output([str(p),'--version'],text=True).strip()}))
'''
        for node in h.hosts.nodes:
            tool = json.loads(h.hosts.ssh(node, 'python3 -c '+shlex.quote(probe), capture=True))
            tools.append({'node': node['name'], **tool})
            h.check(tool['host_mount_visible'], 'Host iptables discovery does not escape the Agent host mount: '+node['name'])
        h.artifact('host-tools.json', tools)
    ds = json.loads(h.kube('-n', 'tester-agent', 'get', 'daemonset/tester-agent', '-o', 'json'))
    nodes = json.loads(h.kube('get', 'nodes', '-o', 'json'))['items']
    pods = json.loads(h.kube('-n', 'tester-agent', 'get', 'pods', '-l', 'app=tester-agent', '-o', 'json'))['items']
    h.check(ds['spec']['template']['spec']['containers'][0]['image'] == IMAGE, 'Tester Agent uses the specification image digest')
    ready_nodes = {p['spec'].get('nodeName') for p in pods if any(
        c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', []))}
    h.check(ready_nodes == {n['metadata']['name'] for n in nodes}, 'Exactly the cluster nodes have a Ready Tester Agent')
    scenarios = h.kube('-n', 'tester-agent', 'exec', 'daemonset/tester-agent', '--', '/app', 'list').splitlines()
    h.check(set(scenarios) == {'clock-skew', 'kill-leader', 'kill-store', 'partition', 'schema', 'wipe-node'}, 'Official practice scenarios are discoverable')
    for p in pods:
        version = h.kube('-n', 'tester-agent', 'exec', p['metadata']['name'], '--', '/host/usr/sbin/iptables', '--version')
        h.check('iptables v' in version, 'Host iptables actually executes from the unchanged Agent: '+p['spec']['nodeName'])
        h.artifact(p['metadata']['name']+'-log.json', {'log': h.kube('-n', 'tester-agent', 'logs', p['metadata']['name'], '--tail=100')})
    h.artifact('baseline.json', {'daemonset': ds, 'nodes': nodes, 'agent_pods': pods, 'scenarios': scenarios,
                                'application_pods': h.pods(),
                                'agent_leases': json.loads(h.kube('-n', 'tester-agent', 'get', 'leases', '-o', 'json'))})
    application = json.loads(h.kube('get', 'pods', '-o', 'json'))['items']
    h.note({'scope': 'Agent installation and practice discovery only; this does not prove application fault tolerance',
            'known_unready_apps': [p['metadata']['name'] for p in application if p['status'].get('phase') not in ('Succeeded', 'Failed')
                and not any(c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', []))]})


if __name__ == '__main__':
    raise SystemExit(run('tester-agent/00-preflight', ['Tester Agent environment contract'], parser('Check official Tester Agent on all VPS nodes'), scenario))
