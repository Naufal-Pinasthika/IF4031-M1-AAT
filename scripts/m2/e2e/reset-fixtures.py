#!/usr/bin/env python3
"""Delete disposable BNPB fixtures ONLY in the dedicated three-QEMU-node lab.

Never use against a shared/demo cluster containing data that must be retained.
Tester Agent, namespace/RBAC, Secrets and ConfigMaps are not deleted.
"""
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys

import smoke
from resilience import kube

assert sys.argv[1:] == ['--confirm-disposable-lab-data'], 'Explicit disposable-data confirmation flag required'
config = json.loads(subprocess.check_output(['kubectl','config','view','--minify','-o','json'],text=True))
assert config['clusters'][0]['cluster']['server'] == 'https://192.168.93.10:6443', 'Not the isolated lab endpoint'
nodes = json.loads(subprocess.check_output(['kubectl','get','nodes','-o','json'],text=True))
assert {x['metadata']['name'] for x in nodes['items']} == {'aat-node-0','aat-node-1','aat-node-2'}, 'Unexpected cluster topology'
deployments = ['aggregator','auth','bmkg','client-api','pvmbg','sensor-pantai']
statefulsets = ['aggregator-coordinator','auth-postgres','dashboard-consumer','kafka','notifier-consumer','postgres-a','postgres-b','postgres-c']
pvcs = ['data-'+name+'-0' for name in statefulsets]
for kind, expected in [('deployment',deployments),('statefulset',statefulsets),('pvc',pvcs),('job',['kafka-init'])]:
    actual = json.loads(kube('get',kind,'-o','json'))
    assert {x['metadata']['name'] for x in actual['items']} == set(expected), f'Unexpected {kind}; refusing reset'
archive = smoke.ROOT/'.e2e'/('before-reset-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
archive.mkdir()
for evidence in (smoke.ROOT/'.e2e').glob('*results.json'):
    shutil.copy2(evidence,archive/evidence.name)
print(f'Archived prior results to {archive.name}',flush=True)
targets = ['deployment/'+x for x in deployments]+['statefulset/'+x for x in statefulsets]+['job/kafka-init']
print(kube('delete',*targets,'--wait=true','--timeout=180s',timeout=200),flush=True)
print(kube('delete',*['pvc/'+x for x in pvcs],'--wait=true','--timeout=90s',timeout=110),flush=True)
print('Disposable application fixtures removed; Tester Agent and credentials retained.',flush=True)
