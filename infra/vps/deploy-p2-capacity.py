#!/usr/bin/env python3
"""Build only the P2 image and roll out its read path, retaining all PVCs.

Secrets, canonical databases, source images and tester agents are not changed.
Common-entry selectors switch only after gateway readiness; failure restores
the previous image/selectors/config values. Run regression tests first.
"""
import argparse
import datetime as dt
import fcntl
import json
import shlex
import subprocess
import re
import yaml

from provision import Hosts,ROOT,inventory
from deploy import import_worker
from vps_config import NAMESPACE as NS


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resume',metavar='DEPLOYMENT',help='Resume a retained p2-capacity-deploy-UTC directory after interruption')
    args=parser.parse_args()
    (ROOT/'.vps').mkdir(exist_ok=True)
    lock=(ROOT/'.vps/p2-test.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if args.resume and not re.fullmatch(r'p2-capacity-deploy-\d{8}T\d{6}Z',args.resume):
        parser.error('--resume must name a retained deployment directory, not a path')
    stamp=args.resume.removeprefix('p2-capacity-deploy-') if args.resume else dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    tag='m2-vps-'+stamp+'-read-capacity'
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));a=h.nodes[0]
    out=ROOT/'.e2e'/('p2-capacity-deploy-'+stamp)
    if args.resume:
        state=json.loads((out/'before.json').read_text())
        if state['image']!='aat/aggregator:'+tag or (out/'result.json').exists():
            raise RuntimeError('Resume requires an unfinished deployment with the matching image')
    else:out.mkdir(parents=True)
    def kube(*args,payload=None):
        return h.ssh(a,'sudo -n k3s kubectl --request-timeout=180s -n '+NS+' '+shlex.join(args),payload,capture=True).decode()
    old=json.loads(kube('get','deployment/aggregator','-o','json'))
    image=old['spec']['template']['spec']['containers'][0]['image']
    gateway_raw=kube('get','deployment/aggregator-gateway','--ignore-not-found','-o','json').strip()
    previous_gateway=json.loads(gateway_raw) if gateway_raw else None
    services={name:json.loads(kube('get','service/'+name,'-o','json'))['spec']['selector'] for name in ('aggregator','aggregator-vps')}
    config=json.loads(kube('get','configmap/aggregator-config','-o','json'))['data']
    keys={'AGGREGATOR_RESPONSE_SLOTS':'128','AGGREGATOR_SHARED_READ_PROJECTION':'true'}
    if args.resume:
        allowed={state['previous_image'],state['image']}
        if image not in allowed or (previous_gateway and previous_gateway['spec']['template']['spec']['containers'][0]['image'] not in allowed):
            raise RuntimeError('Live image changed outside the retained deployment; refusing resume')
        image=state['previous_image'];services=state['previous_selectors']
        state['resumed']=True
    else:
        state=dict(previous_image=image,image='aat/aggregator:'+tag,previous_selectors=services,
                   previous_config={k:config.get(k) for k in keys},previous_gateway_image=previous_gateway['spec']['template']['spec']['containers'][0]['image'] if previous_gateway else None,passed=False)
        (out/'before.json').write_text(json.dumps(state,indent=2))
    context='/opt/aat/capacity-build-'+stamp
    if not args.resume:
        h.ssh(a,'sudo -n install -d -m 700 '+context)
        ssh=shlex.join(h.command(a,'')[:-2])
        subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,
                        str(ROOT/'services/aggregator')+'/',a['user']+'@'+a['host']+':'+context+'/'],check=True)
        build='sudo -n docker build --build-arg BUILD_VERSION='+tag+' -t aat/aggregator:'+tag+' '+context
        with (out/'build.log').open('w') as log:
            process=subprocess.run(h.command(a,build),stdout=log,stderr=subprocess.STDOUT)
        if process.returncode:raise RuntimeError('Image build failed; see retained build.log')
    else:
        h.ssh(a,'sudo -n docker image inspect aat/aggregator:'+tag+' >/dev/null')
    h.ssh(a,'sudo -n docker save -o /opt/aat/images/'+tag+'.tar aat/aggregator:'+tag)
    h.ssh(a,'sudo -n k3s ctr images import /opt/aat/images/'+tag+'.tar')
    for node in h.nodes[1:]:import_worker(h,node,tag)
    # Separate apply preserves the official sensor/tester and all other workloads.
    resources=list(yaml.safe_load_all((ROOT/'k8s/base/apps/gateway.yaml').read_text()))
    for obj in resources:
        obj['metadata']['namespace']=NS
        if obj['kind']=='Deployment':obj['spec']['template']['spec']['containers'][0]['image']='aat/aggregator:'+tag
    try:
        kube('patch','configmap/aggregator-config','--type=merge','-p',json.dumps({'data':keys}))
        kube('set','image','deployment/aggregator','aggregator=aat/aggregator:'+tag)
        print(kube('rollout','status','deployment/aggregator','--timeout=180s'),flush=True)
        kube('apply','-f','-',payload=json.dumps({'apiVersion':'v1','kind':'List','items':resources}).encode())
        print(kube('rollout','status','deployment/aggregator-gateway','--timeout=180s'),flush=True)
        for name in services:
            kube('patch','service/'+name,'--type=merge','-p',json.dumps({'spec':{'selector':{'app':'aggregator-gateway'}}}))
        state['passed']=True
        (out/'pods-after.json').write_text(kube('get','pods','-o','json'))
    except BaseException:
        for name,selector in services.items():kube('patch','service/'+name,'--type=merge','-p',json.dumps({'spec':{'selector':selector}}))
        kube('patch','configmap/aggregator-config','--type=merge','-p',json.dumps({'data':state['previous_config']}))
        kube('set','image','deployment/aggregator','aggregator='+image)
        print(kube('rollout','status','deployment/aggregator','--timeout=180s'),flush=True)
        if previous_gateway:
            kube('set','image','deployment/aggregator-gateway','aggregator-gateway='+state['previous_gateway_image'])
            print(kube('rollout','status','deployment/aggregator-gateway','--timeout=180s'),flush=True)
        state['rolled_back']=True
        raise
    finally:
        (out/'result.json').write_text(json.dumps(state,indent=2))
        print('Evidence:',out.relative_to(ROOT),flush=True)
    print('P2 image:',state['image'],flush=True)


if __name__=='__main__':main()
