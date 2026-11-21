#!/usr/bin/env python3
"""Build/update only Aggregator; optional coordinated schema cutover, no PVC deletes.

Schema-v2 workers must be stopped before v3 admission/delivery starts: old
workers do not understand per-target acknowledgements. This one-off cutover is
NOT evidence of a zero-downtime compatible-version rolling update.
"""
import argparse
import concurrent.futures
import datetime as dt
import json
import re
import shlex
import subprocess
from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS
from deploy import import_worker



def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tag', required=True)
    p.add_argument('--schema-cutover', action='store_true')
    p.add_argument('--build-only', action='store_true', help='Stage the image on all nodes without changing the deployment')
    args = p.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}', args.tag):
        raise ValueError('invalid image tag')
    h = Hosts(inventory(ROOT/'infra/vps/inventory.json')); server = h.nodes[0]
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory = ROOT/'.e2e'/('replication-update-'+stamp); directory.mkdir(parents=True)
    def kube(*items):
        return h.ssh(server, 'sudo -n k3s kubectl -n '+NS+' '+shlex.join(items), capture=True).decode()
    before = json.loads(kube('get', 'deployment/aggregator', '-o', 'json'))
    replicas = before['spec']['replicas']
    (directory/'deployment-before.json').write_text(json.dumps(before, indent=2))
    ssh = shlex.join(h.command(server, '')[:-2])
    # --delete is confined to the disposable source mirror, never runtime data.
    target = '/opt/aat/repo/services/aggregator'
    h.ssh(server, 'test "$(sudo -n readlink -f '+target+')" = '+target)
    subprocess.run(['rsync', '-az', '--delete', '--rsync-path=sudo -n rsync', '-e', ssh,
                    str(ROOT/'services/aggregator')+'/', server['user']+'@'+server['host']+':'+target+'/'], check=True)
    image = 'aat/aggregator:'+args.tag
    h.ssh(server, 'sudo -n docker build --build-arg BUILD_VERSION='+args.tag+' -t '+image+' '+target)
    h.ssh(server, 'sudo -n docker save -o /opt/aat/images/'+args.tag+'.tar '+image)
    h.ssh(server, 'sudo -n k3s ctr images import /opt/aat/images/'+args.tag+'.tar')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda n: import_worker(h,n,args.tag), h.nodes[1:]))
    if args.build_only:
        result = dict(image=image, staged_nodes=[n['name'] for n in h.nodes], deployment_changed=False)
        (directory/'result.json').write_text(json.dumps(result, indent=2))
        print(json.dumps(result), flush=True)
        print('Evidence:', directory.relative_to(ROOT), flush=True)
        return
    if args.schema_cutover:
        print('Starting one-off schema cutover: stopping old Aggregator workers only.', flush=True)
        kube('scale', 'deployment/aggregator', '--replicas=0')
        kube('wait', '--for=delete', 'pods', '-l', 'app=aggregator', '--timeout=120s')
    # Never roll back automatically to schema-v2 after the new workers start.
    kube('set', 'image', 'deployment/aggregator', 'aggregator='+image)
    kube('scale', 'deployment/aggregator', '--replicas='+str(replicas))
    kube('rollout', 'status', 'deployment/aggregator', '--timeout=240s')
    after = json.loads(kube('get', 'deployment/aggregator', '-o', 'json'))
    (directory/'deployment-after.json').write_text(json.dumps(after, indent=2))
    result = dict(image=image, replicas=replicas, schema_cutover=args.schema_cutover,
                  ready=after['status'].get('readyReplicas',0), other_services_changed=False)
    (directory/'result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)
    print('Evidence:', directory.relative_to(ROOT), flush=True)


if __name__ == '__main__':
    main()
