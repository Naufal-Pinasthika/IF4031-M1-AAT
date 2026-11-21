#!/usr/bin/env python3
"""Add gateway metrics/traces without restarting Prometheus or losing history."""
import datetime as dt
import fcntl
import json
import shlex
import subprocess
import time
from provision import Hosts,ROOT,inventory
from deploy import import_worker
from vps_config import NAMESPACE as NS


def main():
    lock=(ROOT/'.vps/p2-test.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));a=h.nodes[0]
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ');tag='p2-obs-'+stamp
    directory=ROOT/'.e2e'/tag;directory.mkdir()
    def kube(*args):return h.ssh(a,'sudo -n k3s kubectl -n '+NS+' '+shlex.join(args),capture=True).decode()
    config=json.loads(kube('get','configmap/prometheus-config','-o','json'))['data']['prometheus.yml']
    old_image=json.loads(kube('get','deployment/observability','-o','json'))['spec']['template']['spec']['containers'][0]['image']
    prometheus_pod=json.loads(kube('get','pods','-l','app=prometheus','-o','json'))['items'][0]
    state=dict(previous_image=old_image,previous_prometheus_config=config,prometheus_uid_before=prometheus_pod['metadata']['uid'],passed=False)
    (directory/'before.json').write_text(json.dumps(state,indent=2))
    context='/opt/aat/'+tag
    h.ssh(a,'sudo -n install -d -m 700 '+context)
    ssh=shlex.join(h.command(a,'')[:-2])
    subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,str(ROOT/'services/observability')+'/',a['user']+'@'+a['host']+':'+context+'/'],check=True)
    h.ssh(a,'sudo -n docker build -t aat/observability:'+tag+' '+context)
    h.ssh(a,'sudo -n docker save -o /opt/aat/images/'+tag+'.tar aat/observability:'+tag)
    h.ssh(a,'sudo -n k3s ctr images import /opt/aat/images/'+tag+'.tar')
    for node in h.nodes[1:]:import_worker(h,node,tag)
    if 'aggregator-gateway' not in config:
        old='regex: aggregator|auth|'
        if old not in config:raise RuntimeError('Unexpected Prometheus discovery config; refusing a blind replacement')
        updated=config.replace(old,'regex: aggregator|aggregator-gateway|auth|')
    else:updated=config
    try:
        kube('set','image','deployment/observability','observability=aat/observability:'+tag)
        print(kube('rollout','status','deployment/observability','--timeout=180s'),flush=True)
        kube('patch','configmap/prometheus-config','--type=merge','-p',json.dumps({'data':{'prometheus.yml':updated}}))
        for attempt in range(60):
            mounted=kube('exec','deployment/prometheus','--','cat','/etc/prometheus/prometheus.yml')
            if 'aggregator-gateway' in mounted:break
            time.sleep(2)
        else:raise RuntimeError('Projected ConfigMap did not update')
        kube('exec','deployment/prometheus','--','/bin/kill','-HUP','1')
        after=json.loads(kube('get','pods','-l','app=prometheus','-o','json'))['items'][0]
        state.update(image='aat/observability:'+tag,prometheus_uid_after=after['metadata']['uid'])
        state['passed']=state['prometheus_uid_before']==state['prometheus_uid_after']
        if not state['passed']:raise RuntimeError('Prometheus pod changed unexpectedly; check retained history')
    except BaseException:
        kube('set','image','deployment/observability','observability='+old_image)
        kube('patch','configmap/prometheus-config','--type=merge','-p',json.dumps({'data':{'prometheus.yml':config}}))
        raise
    finally:
        (directory/'result.json').write_text(json.dumps(state,indent=2));print('Evidence:',directory.relative_to(ROOT),flush=True)


if __name__=='__main__':main()
