import argparse
import datetime as dt
import json
import os
import re
import shlex

from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS


def main():
    p = argparse.ArgumentParser(description='Expose only the PVMBG test-control listener to the official Tester Agent; retain Bearer protection on the data listener')
    p.add_argument('--image-tag', required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', args.image_tag):
        p.error('invalid image tag')
    if not args.apply:
        print('Plan: apply the admin-port NetworkPolicy first, create pvmbg-admin Service, enable :8089, roll only PVMBG. No Agent changes.')
        return
    os.umask(0o077)
    h = Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    folder = ROOT/'.e2e'/('tester-admin-rollout-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    folder.mkdir()
    def kube(*parts, payload=None):
        return h.ssh(h.nodes[0], 'sudo -n k3s kubectl -n '+NS+' '+shlex.join(parts), payload, capture=True).decode()
    before = {'deployment': json.loads(kube('get', 'deployment/pvmbg', '-o', 'json')),
              'config': json.loads(kube('get', 'configmap/pvmbg-config', '-o', 'json')),
              'service': json.loads(kube('get', 'service/pvmbg', '-o', 'json'))}
    (folder/'before.json').write_text(json.dumps(before, indent=2))
    for filename, kind, name in [('k8s/base/platform/network-policies.yaml', 'NetworkPolicy', 'pvmbg-admin-tester-only'),
                                  ('k8s/base/apps/workloads.yaml', 'Service', 'pvmbg-admin')]:
        raw = kube('create', '--dry-run=client', '-f', '-', '-o', 'json', payload=(ROOT/filename).read_bytes()).lstrip()
        objects = []
        decoder = json.JSONDecoder()
        while raw:
            obj, end = decoder.raw_decode(raw)
            objects.extend(obj.get('items', [obj]))
            raw = raw[end:].lstrip()
        chosen = next(o for o in objects if o['kind'] == kind and o['metadata']['name'] == name)
        kube('apply', '-f', '-', payload=json.dumps(chosen).encode())
    # Undo only the discovery label added during diagnosis on the data Service.
    # The new admin Service is the discovery target for this control API.
    if before['service']['metadata'].get('labels', {}).get('role') == 'mock':
        kube('label', 'service/pvmbg', 'role-')
    kube('patch', 'configmap/pvmbg-config', '--type=merge', '-p', json.dumps({'data': {'PVMBG_ADMIN_LISTEN_ADDRESS': ':8089'}}))
    kube('set', 'image', 'deployment/pvmbg', 'pvmbg=aat/pvmbg:'+args.image_tag)
    kube('rollout', 'status', 'deployment/pvmbg', '--timeout=180s')
    after = json.loads(kube('get', 'deployment/pvmbg', '-o', 'json'))
    (folder/'after.json').write_text(json.dumps(after, indent=2))
    print(json.dumps({'pvmbg_ready': after['status'].get('readyReplicas', 0), 'admin_port': 8089,
                      'data_port': 8082, 'agent_changed': False, 'evidence': str(folder.relative_to(ROOT))}))


if __name__ == '__main__':
    main()
