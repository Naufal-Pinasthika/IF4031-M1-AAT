#!/usr/bin/env python3
"""Repeated real canonical-replica pauses; autonomous resume and durable catch-up.

Pauses ONLY an application PostgreSQL container, never its VM, official agents,
coordinator or Kafka. No row updates/deletes, source resets or manual repairs.
"""
import datetime as dt
import json
import shlex
import time
import uuid
from provision import Hosts, ROOT, inventory

NS = 'sistem-informasi-kebencanaan-bnpb'


def main():
    h = Hosts(inventory(ROOT/'infra/vps/inventory.json')); server = h.nodes[0]
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory = ROOT/'.e2e'/('replication-fault-'+stamp); directory.mkdir(parents=True)
    def kube(*items):
        return h.ssh(server, 'sudo -n k3s kubectl --request-timeout=10s -n '+NS+' '+shlex.join(items), capture=True).decode()
    def sql(query):
        return kube('exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',query).strip()
    results = []
    for letter in ('a','b','c'):
        pod = json.loads(kube('get','pod','postgres-'+letter+'-0','-o','json'))
        node = next(n for n in h.nodes if n['name']==pod['spec']['nodeName'])
        cid = pod['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
        if not cid.isalnum(): raise ValueError('invalid observed container ID')
        lower = int(sql('SELECT COALESCE(max(version),0) FROM ingestion_outbox'))
        unit = 'aat-replica-fault-'+uuid.uuid4().hex[:10]
        helper = r'''
import json,signal,subprocess,time
def run(action):subprocess.run(['k3s','ctr','-n','k8s.io','tasks',action,CID],check=True)
def stop(*args):raise SystemExit(1)
signal.signal(signal.SIGTERM,stop)
try:
 run('pause');print(json.dumps({'paused':True}),flush=True);time.sleep(35)
finally:
 run('resume');print(json.dumps({'resumed':True}),flush=True)
'''.replace('CID',repr(cid))
        result = dict(store='store-'+letter, lower_version=lower, scope='35s PostgreSQL process pause; not node/disk loss')
        journal = ''
        try:
            h.ssh(node, 'sudo -n systemd-run --quiet --unit='+unit+' --property=RuntimeMaxSec=55 /usr/bin/python3 -u -c '+shlex.quote(helper))
            time.sleep(27)
            upper = int(sql('SELECT COALESCE(max(version),0) FROM ingestion_outbox'))
            result['upper_version'] = upper
            target = 'canonical:store-'+letter
            q = "SELECT json_build_object('pending_replica',count(*) FILTER(WHERE target='"+target+"' AND delivered_at IS NULL),'broker_delivered',count(*) FILTER(WHERE target='broker' AND delivered_at IS NOT NULL),'other_replica_delivered',count(*) FILTER(WHERE target LIKE 'canonical:%' AND target<>'"+target+"' AND delivered_at IS NOT NULL),'admitted_versions',count(DISTINCT version)) FROM ingestion_projections WHERE version>"+str(lower)+' AND version<='+str(upper)
            result['during_pause'] = json.loads(sql(q))
            print(json.dumps(dict(stage='during-pause',**result)),flush=True)
            time.sleep(12)
            journal = h.ssh(node,'sudo -n journalctl -u '+unit+' --no-pager -o cat',capture=True).decode()
            result['resumed_automatically'] = '"resumed": true' in journal
            deadline = time.monotonic()+120
            started = time.monotonic()
            while True:
                pending = int(sql('SELECT count(*) FROM ingestion_projections WHERE delivered_at IS NULL AND version>'+str(lower)+' AND version<='+str(upper)))
                if not pending or time.monotonic()>=deadline: break
                time.sleep(2)
            result.update(pending_after_recovery=pending,seconds_to_observed_drain=round(time.monotonic()-started,3))
            during = result['during_pause']
            result['passed'] = result['resumed_automatically'] and during['pending_replica']>0 and during['broker_delivered']>0 and during['other_replica_delivered']>0 and pending==0
        finally:
            (directory/(letter+'-fault.log')).write_text(journal)
            # Cleanup is NOT part of a pass: normally helper has already resumed.
            h.ssh(node,'sudo -n k3s ctr -n k8s.io tasks resume '+cid+' 2>/dev/null || true')
            results.append(result)
            (directory/'result.json').write_text(json.dumps(dict(passed=all(x.get('passed',False) for x in results),cases=results),indent=2))
            print(json.dumps(result),flush=True)
    print('Evidence:', directory.relative_to(ROOT),flush=True)
    raise SystemExit(0 if all(x.get('passed',False) for x in results) else 1)


if __name__ == '__main__':
    main()
