#!/usr/bin/env python3
"""Bounded P2 control-path checks on the existing VPS cluster; never deletes PVCs.

Worker clock changes restore NTP and wall time automatically. Freeze is a
container/process pause, explicitly NOT a hypervisor/whole-VM suspend test.
"""
import datetime as dt
import fcntl
import json
from pathlib import Path
import shlex
import subprocess
import time
import uuid
from provision import Hosts, ROOT, inventory
from p2_log_capture import PodLogCapture

NS = 'sistem-informasi-kebencanaan-bnpb'
SOURCES = ('BMKG', 'PVMBG', 'SENSOR_PANTAI')


def literal(value):
    return "'"+value.replace("'", "''")+"'"


def task_identity(pod):
    runtime=next((x for x in pod.get('status',{}).get('containerStatuses',[]) if x['name']=='aggregator'),{})
    return dict(container_id=runtime.get('containerID'),restarts=runtime.get('restartCount'))


class Test:
    def __init__(self):
        self.h = Hosts(inventory(ROOT/'infra/vps/inventory.json'))
        self.server = self.h.nodes[0]
        self.directory = ROOT/'.e2e'/('p2-control-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
        self.directory.mkdir(parents=True)
        self.results = []
        self.capture = PodLogCapture(self.h, self.server, NS, self.directory)

    def kube(self, *args):
        return self.h.ssh(self.server, 'sudo -n k3s kubectl --request-timeout=10s -n '+NS+' '+shlex.join(args), capture=True).decode()

    def sql(self, query):
        return self.kube('exec', 'aggregator-coordinator-0', '--', 'psql', '-U', 'aggregator_coord', '-d', 'coordination', '-At', '-c', query).strip()

    def leases(self):
        return json.loads(self.sql("SELECT json_agg(t) FROM (SELECT source,holder_id,fencing_token,lease_until,clock_timestamp() AS database_now FROM source_leases WHERE source IN ('BMKG','PVMBG','SENSOR_PANTAI')) t"))

    def pods(self):
        pods = [p for p in json.loads(self.kube('get','pods','-l','app=aggregator','-o','json'))['items'] if not p['metadata'].get('deletionTimestamp')]
        self.capture.observe(pods)
        return pods

    def worker_owner(self):
        """Exercise a worker owner without editing lease rows or source state.

        Scaling can leave every lease on A. In that case, record a real owner
        loss first; the other existing pods must take over normally. This is
        explicit setup evidence, not a forced lease assignment or a skipped test.
        """
        topology = {p['metadata']['name']: p for p in self.pods()}
        leases = self.leases()
        if any(topology[x['holder_id']]['spec']['nodeName'] != self.server['name'] for x in leases):
            return
        before = next(x for x in leases if x['source'] == 'BMKG')
        started = time.monotonic()
        self.kube('delete','pod',before['holder_id'],'--grace-period=0','--force','--wait=false')
        result = self.wait_successor(before, started)
        self.mark(dict(case='worker-owner-setup',source=before['source'],passed=True,**result))
        self.kube('rollout','status','deployment/aggregator','--timeout=180s')
        self.pods()

    def save(self, name, value):
        (self.directory/name).write_text(json.dumps(value,indent=2))

    def mark(self, result):
        self.results.append(result)
        self.save('result.json',dict(passed=all(x['passed'] for x in self.results),cases=self.results))
        print(json.dumps(result),flush=True)

    def wait_successor(self, before, started):
        while time.monotonic()-started < 60:
            current=next(x for x in self.leases() if x['source']==before['source'])
            if current['holder_id']!=before['holder_id'] and current['fencing_token']>before['fencing_token']:
                n=int(self.sql("SELECT count(*) FROM poll_attempts WHERE source="+literal(before['source'])+" AND holder_id="+literal(current['holder_id'])+" AND fencing_token="+str(current['fencing_token'])+" AND status='completed'"))
                if n:
                    return dict(seconds_to_completed_poll=round(time.monotonic()-started,3),before=before,after=current)
            time.sleep(.5)
        raise RuntimeError('No completed successor poll within 60 seconds')

    def backend_service(self):
        front=json.loads(self.kube('get','service/aggregator','-o','json'))
        return 'aggregator-backends' if front['spec']['selector'].get('app')=='aggregator-gateway' else 'aggregator'

    def topology(self):
        pods=self.pods()
        backend_service=self.backend_service()
        slices=json.loads(self.kube('get','endpointslices','-l','kubernetes.io/service-name='+backend_service,'-o','json'))['items']
        ready={p['metadata']['name'] for p in pods if any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[]))}
        targets={e['targetRef']['name'] for s in slices for e in s['endpoints'] if e.get('conditions',{}).get('ready') is True}
        self.save('topology.json',dict(pods=pods,slices=slices))
        front_ok=True
        front_targets=[]
        if backend_service=='aggregator-backends':
            gateways=json.loads(self.kube('get','pods','-l','app=aggregator-gateway','-o','json'))['items']
            gateway_ready={p['metadata']['name'] for p in gateways if not p['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[]))}
            front_slices=json.loads(self.kube('get','endpointslices','-l','kubernetes.io/service-name=aggregator','-o','json'))['items']
            front_targets=sorted({e['targetRef']['name'] for s in front_slices for e in s['endpoints'] if e.get('conditions',{}).get('ready') is True})
            front_ok=bool(gateway_ready) and set(front_targets)==gateway_ready
            self.save('front-topology.json',dict(pods=gateways,slices=front_slices))
        self.mark(dict(case='topology',backend_service=backend_service,front_targets=front_targets,front_ready=front_ok,ready_pods=len(ready),nodes=sorted({p['spec']['nodeName'] for p in pods}),ready_service_targets=sorted(targets),passed=front_ok and len(ready)==3 and len({p['spec']['nodeName'] for p in pods})==3 and ready==targets))

    def clock(self):
        self.worker_owner()
        topology={p['metadata']['name']:p for p in self.pods()}
        eligible=[x for x in self.leases() if topology[x['holder_id']]['spec']['nodeName']!=self.server['name']]
        if not eligible:
            self.mark(dict(case='worker-clock',passed=False,error='No source owner on a worker; no leadership was forced'))
            return
        before=eligible[0];pod=topology[before['holder_id']]
        node=next(n for n in self.h.nodes if n['name']==pod['spec']['nodeName'])
        unit='aat-p2-clock-'+uuid.uuid4().hex[:10]
        start=self.sql('SELECT clock_timestamp()')
        code=r'''
import json,signal,subprocess,time
base=time.time();mono=time.monotonic()
ntp=subprocess.check_output(['timedatectl','show','-p','NTP','--value'],text=True).strip()
def stop(*args):raise SystemExit(130)
signal.signal(signal.SIGTERM,stop)
try:
 subprocess.run(['timedatectl','set-ntp','false'],check=True)
 for shift in (45,-30):
  expected=base+time.monotonic()-mono
  time.clock_settime(time.CLOCK_REALTIME,expected+shift)
  print(json.dumps({'offset_seconds':time.time()-(base+time.monotonic()-mono),'requested_shift':shift}),flush=True)
  time.sleep(18)
finally:
 time.clock_settime(time.CLOCK_REALTIME,base+time.monotonic()-mono)
 subprocess.run(['timedatectl','set-ntp','true' if ntp=='yes' else 'false'],check=True)
 print(json.dumps({'restored':True,'ntp_original':ntp}),flush=True)
'''
        self.h.ssh(node,'sudo -n systemd-run --quiet --unit='+unit+' --property=RuntimeMaxSec=60 /usr/bin/python3 -u -c '+shlex.quote(code))
        samples=[]
        deadline=time.monotonic()+41
        while time.monotonic()<deadline:
            samples.append(self.leases());time.sleep(2)
        log=self.h.ssh(node,'sudo -n journalctl -u '+unit+' --no-pager -o cat',capture=True).decode()
        (self.directory/'clock.log').write_text(log)
        rows=[]
        for line in log.splitlines():
            try:rows.append(json.loads(line))
            except ValueError:pass
        violations=int(self.sql("SELECT count(*) FROM ingestion_outbox a JOIN ingestion_outbox b ON a.source=b.source AND a.fencing_token<b.fencing_token AND a.accepted_at>b.accepted_at WHERE a.accepted_at>"+literal(start)+"::timestamptz AND b.accepted_at>"+literal(start)+"::timestamptz"))
        self.save('clock-lease-samples.json',samples)
        offsets=[x.get('offset_seconds',0) for x in rows if 'offset_seconds' in x]
        progress=int(self.sql("SELECT count(*) FROM ingestion_outbox WHERE accepted_at>"+literal(start)+"::timestamptz"))
        self.mark(dict(case='worker-clock',node=node['name'],offsets_seconds=offsets,restored=any(x.get('restored') for x in rows),epoch_order_violations=violations,admitted_during_window=progress,scope='Worker clock only; coordinator clock unchanged; observed admission epoch ordering',passed=len(offsets)==2 and offsets[0]>40 and offsets[1]<-25 and any(x.get('restored') for x in rows) and violations==0 and progress>0))

    def freeze(self):
        self.worker_owner()
        topology={p['metadata']['name']:p for p in self.pods()}
        eligible=[x for x in self.leases() if topology[x['holder_id']]['spec']['nodeName']!=self.server['name']]
        if not eligible:
            self.mark(dict(case='process-freeze',passed=False,error='No source owner on a worker; no leadership was forced'))
            return
        before=eligible[0];pod=topology[before['holder_id']]
        node=next(n for n in self.h.nodes if n['name']==pod['spec']['nodeName'])
        self.save('frozen-pod-before.json',pod)
        original_task=task_identity(pod)
        cid=pod['status']['containerStatuses'][0]['containerID'].split('://')[1]
        unit='aat-p2-freeze-'+uuid.uuid4().hex[:10]
        code='''import json,subprocess,time,signal
cid=CID
def run(action):return subprocess.run(['k3s','ctr','-n','k8s.io','tasks',action,cid],check=True)
def stop(*args):raise SystemExit(130)
signal.signal(signal.SIGTERM,stop)
try:
 run('pause');print(json.dumps({'paused':True}),flush=True);time.sleep(28)
finally:
 run('resume');print(json.dumps({'resumed':True}),flush=True)
'''.replace('CID',repr(cid))
        started=time.monotonic()
        self.h.ssh(node,'sudo -n systemd-run --quiet --unit='+unit+' --property=RuntimeMaxSec=45 /usr/bin/python3 -u -c '+shlex.quote(code))
        try:
            result=self.wait_successor(before,started)
            self.save('frozen-endpoints.json',json.loads(self.kube('get','endpointslices','-l','kubernetes.io/service-name='+self.backend_service(),'-o','json')))
            time.sleep(max(0,36-(time.monotonic()-started)))
            log=self.h.ssh(node,'sudo -n journalctl -u '+unit+' --no-pager -o cat',capture=True).decode()
            (self.directory/'freeze.log').write_text(log)
            stale=int(self.sql("SELECT count(*) FROM ingestion_outbox WHERE source="+literal(before['source'])+" AND holder_id="+literal(before['holder_id'])+" AND fencing_token="+str(before['fencing_token'])+" AND accepted_at>"+literal(before['lease_until'])+"::timestamptz"))
            endpoints=json.loads((self.directory/'frozen-endpoints.json').read_text())['items']
            routed=any(e.get('targetRef',{}).get('name')==before['holder_id'] and e.get('conditions',{}).get('ready') is True for s in endpoints for e in s['endpoints'])
            after_pod=next((p for p in self.pods() if p['metadata']['name']==before['holder_id']),{})
            self.save('frozen-pod-after.json',after_pod)
            self.save('frozen-pod-events.json',json.loads(self.kube('get','events',
                '--field-selector=involvedObject.name='+before['holder_id'],'-o','json')))
            same_task=bool(original_task['container_id']) and original_task==task_identity(after_pod)
            available=stale==0 and not routed and '"resumed": true' in log
            self.mark(dict(case='process-freeze',node=node['name'],source=before['source'],pause_seconds=28,stale_epoch_admissions=stale,unhealthy_target_still_ready=routed,resumed='"resumed": true' in log,handoff=result,same_container_survived=same_task,availability_checks_passed=available,scope='Aggregator process/container pause; not a whole-VM freeze. Stale-resume exercise also requires the original container to survive without restart.',passed=available and same_task))
        finally:
            # Cleanup only: do not leave a task paused if evidence collection fails.
            self.h.ssh(node,'sudo -n systemctl stop '+unit+'; sudo -n k3s ctr -n k8s.io tasks ls')
        self.kube('rollout','status','deployment/aggregator','--timeout=180s')

    def failover(self):
        for source in SOURCES:
            self.pods()
            before=next(x for x in self.leases() if x['source']==source)
            started=time.monotonic()
            self.kube('delete','pod',before['holder_id'],'--grace-period=0','--force','--wait=false')
            result=self.wait_successor(before,started)
            self.mark(dict(case='hard-owner-loss',source=source,passed=result['seconds_to_completed_poll']<=60,**result))
            self.kube('rollout','status','deployment/aggregator','--timeout=180s')
            self.pods()

    def run(self):
        (ROOT/'.vps').mkdir(exist_ok=True)
        lock=(ROOT/'.vps/p2-test.lock').open('a')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.save('window-start.json',dict(poll_id=int(self.sql('SELECT COALESCE(max(poll_id),0) FROM poll_attempts')),database_time=self.sql('SELECT clock_timestamp()')))
        try:
            self.topology();self.clock();self.freeze();self.failover();self.topology()
        except Exception as exc:
            self.mark(dict(case='controller-error',passed=False,error=type(exc).__name__+': '+str(exc)))
            raise
        finally:
            self.capture.close()
            print('Evidence:',self.directory.relative_to(ROOT),flush=True)
        raise SystemExit(0 if all(x['passed'] for x in self.results) else 1)


if __name__=='__main__':
    Test().run()
