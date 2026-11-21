import argparse
import concurrent.futures
import datetime as dt
import json
import re
import shlex
import subprocess
from pathlib import Path
from provision import Hosts, ROOT, inventory
from deploy import import_worker
from vps_config import NAMESPACE as NS


def main():
    p = argparse.ArgumentParser(description='Build and roll out one selected service, retaining existing data, credentials and offsets')
    p.add_argument('--service', choices=['notifier-consumer','auth','aggregator','pvmbg'],required=True)
    p.add_argument('--tag', required=True)
    p.add_argument('--build-only', action='store_true')
    p.add_argument('--source-dir', type=Path, help='Explicit staged service source directory; avoids deploying unrelated worktree changes')
    args = p.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}', args.tag):
        p.error('invalid image tag')
    h = Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    source = (args.source_dir or ROOT/'services'/args.service).resolve()
    if not (source/'Dockerfile').is_file() or not (source/'go.mod').is_file():
        p.error('service source must contain Dockerfile and go.mod')
    server = h.nodes[0]
    folder = ROOT/'.e2e'/(args.service+'-update-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    folder.mkdir(parents=True)

    def kube(*args):
        return h.ssh(server, 'sudo -n k3s kubectl -n '+NS+' '+shlex.join(args), capture=True).decode()

    workload=('statefulset/' if args.service=='notifier-consumer' else 'deployment/')+args.service
    before = json.loads(kube('get', workload, '-o', 'json'))
    claims = json.loads(kube('get', 'pvc', '-o', 'json'))
    (folder/'before.json').write_text(json.dumps(before, indent=2))
    target = '/opt/aat/repo/services/'+args.service
    h.ssh(server, 'test "$(sudo -n readlink -f '+target+')" = '+target)
    ssh = shlex.join(h.command(server, '')[:-2])
    subprocess.run(['rsync', '-az', '--delete', '--rsync-path=sudo -n rsync', '-e', ssh,
                    str(source)+'/',server['user']+'@'+server['host']+':'+target+'/'],check=True)
    image = 'aat/'+args.service+':'+args.tag
    h.ssh(server, 'sudo -n docker build --build-arg BUILD_VERSION='+args.tag+' -t '+image+' '+target)
    transfer_id = args.service+'-'+args.tag
    archive = '/opt/aat/images/'+transfer_id+'.tar'
    h.ssh(server, 'sudo -n docker save -o '+archive+' '+image)
    h.ssh(server, 'sudo -n k3s ctr images import '+archive)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda node: import_worker(h,node,transfer_id),h.nodes[1:]))
    if args.build_only:
        print('Candidate staged on all nodes; workloads unchanged.',flush=True)
        return
    kube('set','image',workload,args.service+'='+image)
    kube('rollout','status',workload,'--timeout=600s')
    after = json.loads(kube('get',workload,'-o','json'))
    retained = {p['metadata']['name']:p['metadata']['uid'] for p in json.loads(kube('get','pvc','-o','json'))['items']}
    unchanged = all(retained.get(p['metadata']['name']) == p['metadata']['uid'] for p in claims['items'])
    result = {'image':image,'ready':after['status'].get('readyReplicas',0),'original_pvcs_retained':unchanged,
              'other_workloads_changed':False}
    (folder/'result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    print('Evidence:',folder.relative_to(ROOT),flush=True)
    if not unchanged:
        raise RuntimeError('An original PVC changed unexpectedly')


if __name__ == '__main__':
    main()
