#!/usr/bin/env python3
"""Bounded read-only pod/entry resource samples alongside an existing run."""
import argparse
import datetime as dt
import json
import shlex
import time
from provision import Hosts,ROOT,inventory

REMOTE=r'''
import json,pathlib,subprocess,time,sys
ns='sistem-informasi-kebencanaan-bnpb'
metrics=json.loads(subprocess.check_output(['k3s','kubectl','get','--raw','/apis/metrics.k8s.io/v1beta1/namespaces/'+ns+'/pods']))
pods=[p for p in metrics['items'] if p['metadata']['name'].startswith(('aggregator-','postgres-'))]
accounting={}
if len(sys.argv)>1 and sys.argv[1]=='cpu-accounting':
 for p in pods:
  name=p['metadata']['name']
  if not name.startswith('aggregator-') or name.startswith('aggregator-coordinator-'):continue
  try:
   raw=subprocess.check_output(['k3s','kubectl','--request-timeout=5s','-n',ns,'exec',name,'--','cat','/sys/fs/cgroup/cpu.stat'],timeout=8,text=True)
   accounting[name]=dict((k,int(v)) for k,v in (line.split() for line in raw.splitlines()))
  except Exception as exc:accounting[name]={'error':str(exc)}
print(json.dumps(dict(time=time.time(),pods=pods,cgroup_cpu=accounting,net_dev=pathlib.Path('/proc/net/dev').read_text(),tcp=pathlib.Path('/proc/net/snmp').read_text(),cpu=pathlib.Path('/proc/stat').read_text().splitlines()[0])))
'''


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('run');p.add_argument('--seconds',type=int,default=360,choices=range(1,601))
    p.add_argument('--cpu-accounting',action='store_true',help='Diagnostic Kubernetes exec sampling of CPU quota counters; additional observer overhead, separately labelled output')
    args=p.parse_args()
    directory=(ROOT/args.run).resolve()
    if not directory.is_relative_to(ROOT/'.e2e') or not directory.is_dir():p.error('Expected an existing local .e2e run directory')
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));samples=[];began=time.monotonic()
    path=directory/('capacity-cpu-accounting.json' if args.cpu_accounting else 'capacity-resources.json')
    while time.monotonic()-began<args.seconds:
        try:sample=json.loads(h.ssh(h.nodes[0],'sudo -n python3 -c '+shlex.quote(REMOTE)+(' cpu-accounting' if args.cpu_accounting else ''),capture=True))
        except Exception as exc:sample=dict(error=str(exc))
        sample['observer_monotonic_seconds']=time.monotonic()-began;samples.append(sample)
        path.write_text(json.dumps(dict(samples=samples,cpu_accounting_requested=args.cpu_accounting,scope='Read-only metrics-server sampled CPU/RSS and entry host counters, not per-request causal attribution. Optional cgroup exec sampling adds diagnostic overhead; compare deltas only for matching pod identities.'),indent=2))
        result=directory/'result.json'
        if result.exists() and json.loads(result.read_text()).get('restored_three'):break
        time.sleep(10)
    print('Evidence:',path.relative_to(ROOT),flush=True)


if __name__=='__main__':main()
