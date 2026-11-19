#!/usr/bin/env python3
"""P2 k6 checks on bounded host C; preserves workloads/PVCs and restores input image.

Generator shares a VPS with application workloads. This is not a dedicated
generator capacity proof. Existing k6 workloads/thresholds are not relaxed.
"""
import argparse
import datetime as dt
import fcntl
import json
import math
from pathlib import Path
import shlex
import subprocess
import time
from provision import Hosts, ROOT, inventory
from p2_log_capture import PodLogCapture
from p2_profiles import PodProfiles

NS='sistem-informasi-kebencanaan-bnpb'


def threshold_ok(value):
    # Legacy --summary-export stores threshold FAILED booleans, whereas
    # handleSummary stores {ok: boolean}. Preserve both formats unchanged.
    return not value if isinstance(value,bool) else value['ok']


def metric_values(metric):
    return metric.get('values',metric)


def timing_integrity(metrics):
    invalid={}
    for name,metric in metrics.items():
        if not (name.startswith('http_req_') or name=='iteration_duration'):
            continue
        value=metric_values(metric).get('min')
        if value is not None and (not math.isfinite(value) or value<0):
            invalid[name]=value
    return dict(valid=not invalid and 'http_req_duration' in metrics,invalid_minima=invalid)


def rollout_observed(snapshots, image, minimum_replicas=3):
    return any(len(s.get('pods',[])) == s['desired'] and s['desired'] >= minimum_replicas and
               all(p['ready'] and p['image'] == image and p['image_id'] for p in s['pods'])
               for s in snapshots)


def gateways_stable(snapshots, baseline):
    return bool(snapshots) and all(len(s.get('gateways',[]))==2 and all(
        g['uid'] in baseline and sum(c.get('restartCount',0) for c in g['containers'])==baseline[g['uid']]
        for g in s['gateways']) for s in snapshots)


def non_rejected_error_rate(metrics):
    # Controlled rejections must not dilute failure of accepted/non-429 work.
    count=lambda name:metric_values(metrics.get(name,{})).get('count',0)
    failures=sum(count(name) for name in ('responses_partial','responses_malformed','responses_other'))
    total=count('responses_complete')+failures
    return failures/total if total else 1.0

RUNNER=r'''
import json,os,signal,subprocess,sys,time,urllib.request
from pathlib import Path
cfg=json.load(sys.stdin);env=os.environ.copy();env.update(cfg['env'])
base=Path(cfg['directory']);base.mkdir(parents=True,exist_ok=True,mode=0o700)
unit=cfg['unit'];started=time.monotonic();execution=None;peak=0;minfree=1e9;peakcpu=0;previous=None
samples=[];reason=None;process=None;pid=None
def interrupt():
 if pid:
  try:os.kill(pid,signal.SIGINT)
  except ProcessLookupError:pass
def stopping(*args):raise KeyboardInterrupt()
signal.signal(signal.SIGTERM,stopping)
try:
 with (base/'k6.log').open('w') as log:
  process=subprocess.Popen(['systemd-run','--quiet','--scope','--unit='+unit,'-p','MemoryMax=5G','-p','CPUQuota=600%',
    '/opt/aat/tools/k6','run','--quiet','--address=127.0.0.1:6566','--summary-export',str(base/'summary.json'),'-'],
    env=env,stdin=subprocess.PIPE,stdout=log,stderr=subprocess.STDOUT)
  process.stdin.write(cfg['script'].encode());process.stdin.close()
  while process.poll() is None:
   now=time.monotonic()
   if pid is None:
    for p in Path('/proc').iterdir():
     if not p.name.isdigit():continue
     try:
      cmd=(p/'cmdline').read_bytes()
      own_scope=('/'+unit+'.scope') in (p/'cgroup').read_text()
     except OSError:continue
     if own_scope and cmd.startswith(b'/opt/aat/tools/k6\x00'):pid=int(p.name);break
   mem=dict(x.split(':',1) for x in Path('/proc/meminfo').read_text().splitlines())
   free=int(mem['MemAvailable'].split()[0])/1024;minfree=min(free,minfree)
   rss=0;cpu=0
   if pid:
    try:
     status=Path('/proc/'+str(pid)+'/status').read_text()
     rss=next((int(x.split()[1])/1024 for x in status.splitlines() if x.startswith('VmRSS:')),0)
     st=Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split();ticks=int(st[11])+int(st[12])
     if previous:cpu=100*(ticks-previous[1])/os.sysconf('SC_CLK_TCK')/max(.001,now-previous[0])
     previous=(now,ticks)
    except OSError:pass
   peak=max(peak,rss);peakcpu=max(peakcpu,cpu)
   if free<768 or rss>4608:reason='Generator memory/headroom safety stop';interrupt()
   if now-started>cfg['timeout']:reason='Generator overall timeout';interrupt()
   try:
    with urllib.request.urlopen('http://127.0.0.1:6566/v1/status',timeout=.5) as r:state=json.load(r)['data']['attributes']
    if state['running'] and execution is None:execution=now
    sample={'type':'status','elapsed':round(now-execution,3) if execution else None,'vus':state['vus'],'running':state['running'],
      'rss_mib':round(rss,2),'cpu_percent':round(cpu,2),'available_mib':round(free,2)}
    samples.append(sample);print(json.dumps(sample),flush=True)
   except OSError:pass
   time.sleep(1)
  code=process.wait()
  summary=json.loads((base/'summary.json').read_text()) if (base/'summary.json').exists() else None
  report={'type':'final','exit_code':code,'safety_stop':reason,'peak_rss_mib':peak,'peak_cpu_percent':peakcpu,'lowest_available_mib':minfree,'samples':samples,'summary':summary}
  (base/'generator.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
finally:
 if process and process.poll() is None:
  interrupt()
  try:process.wait(timeout=15)
  except subprocess.TimeoutExpired:subprocess.run(['systemctl','stop',unit+'.scope'],check=False)
'''


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=['scale','throughput','rolling'])
    p.add_argument('--previous-image', help='Schema-compatible rolling comparison image (required for rolling)')
    p.add_argument('--rolling-target',choices=['aggregator','aggregator-gateway'],default='aggregator',help='Roll either the three Aggregators or both common-entry gateways; same strict workload')
    p.add_argument('--cpu-profile', action='store_true', help='Collect bounded private CPU profiles at 35s; requires explicit profiling opt-in on pods')
    args=p.parse_args()
    if args.mode!='rolling' and args.rolling_target!='aggregator':p.error('--rolling-target applies only to rolling')
    if args.mode=='rolling' and not args.previous_image:
        p.error('rolling requires an explicitly schema-compatible --previous-image; do not use pre-v3 images')
    (ROOT/'.vps').mkdir(exist_ok=True)
    lock=(ROOT/'.vps/p2-test.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));server=h.nodes[0];generator=h.nodes[2]
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory=ROOT/'.e2e'/('p2-'+args.mode+('-gateway' if args.rolling_target=='aggregator-gateway' else '')+'-'+stamp);directory.mkdir(parents=True)
    env={}
    for line in (ROOT/'.env').read_text().splitlines():
        k,sep,v=line.partition('=')
        if sep and k=='AGGREGATOR_API_KEY':env[k]=v.strip().strip('"\'')
    env.update(AGGREGATOR_BASE_URL='http://10.77.0.1:30083',GOMAXPROCS='6',K6_NO_USAGE_REPORT='true')
    def kube(*items):return h.ssh(server,'sudo -n k3s kubectl --request-timeout=10s -n '+NS+' '+shlex.join(items),capture=True).decode()
    target=args.rolling_target if args.mode=='rolling' else 'aggregator'
    target_before=json.loads(kube('get','deployment/'+target,'-o','json'))
    CURRENT=target_before['spec']['template']['spec']['containers'][0]['image']
    gateway_before=json.loads(kube('get','pods','-l','app=aggregator-gateway','-o','json'))['items']
    gateway_restarts={p['metadata']['uid']:sum(c.get('restartCount',0) for c in p['status'].get('containerStatuses',[])) for p in gateway_before}
    (directory/'gateway-before.json').write_text(json.dumps(gateway_before,indent=2))
    # Separate control/evidence directory: collecting source identities must not
    # overwrite this load run's result.json or erase a failed load verdict.
    control=ROOT/'.e2e'/('p2-source-control-'+stamp);control.mkdir()
    window=kube('exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',
        "SELECT json_build_object('poll_id',COALESCE(max(poll_id),0),'database_time',clock_timestamp()) FROM poll_attempts")
    (control/'window-start.json').write_text(json.dumps(json.loads(window),indent=2))
    PREVIOUS=args.previous_image
    if args.mode=='rolling' and PREVIOUS==CURRENT:
        p.error('rolling comparison must use a different image, not a no-op update')
    def ready(count):
        kube('scale','deployment/aggregator','--replicas='+str(count))
        kube('rollout','status','deployment/aggregator','--timeout=180s')
    results=[]
    capture=PodLogCapture(h,server,NS,directory)
    profiles=PodProfiles(h,server,NS,directory)
    def run(name,script,extra):
        remote='/opt/aat/repo/.e2e/'+directory.name+'/'+name
        cfg=dict(env=dict(env,**extra),script=(ROOT/script).read_text(),directory=remote,unit='aat-'+directory.name+'-'+name,timeout=350)
        process=subprocess.Popen(h.command(generator,'sudo -n python3 -c '+shlex.quote(RUNNER)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
        process.stdin.write(json.dumps(cfg));process.stdin.close()
        statuses=[];snapshots=[];report=None;up=down=old=new=False;last=-5;full=None;span=0;profiled=False
        try:
            for line in process.stdout:
                value=json.loads(line)
                if value['type']=='final':report=value;continue
                statuses.append(value);elapsed=value['elapsed']
                (directory/(name+'-live.json')).write_text(json.dumps(dict(statuses=statuses,snapshots=snapshots),indent=2))
                if elapsed is None:continue
                if args.cpu_profile and elapsed>=35 and not profiled:
                    profiles.start(json.loads(kube('get','pods','-l','app='+target,'-o','json'))['items'])
                    profiled=True
                if value['vus']>=10000 and value['running']:
                    if full is None:full=elapsed
                    span=max(span,elapsed-full)
                else:full=None
                if args.mode=='scale':
                    if elapsed>=60 and not up:kube('scale','deployment/aggregator','--replicas=5');up=True
                    if elapsed>=110 and not down:kube('scale','deployment/aggregator','--replicas=2');down=True
                if args.mode=='rolling':
                    if elapsed>=20 and not old:
                        kube('set','image','deployment/'+target,target+'='+PREVIOUS)
                        old=True
                    if elapsed>=70 and not new:
                        kube('set','image','deployment/'+target,target+'='+CURRENT)
                        new=True
                if elapsed-last>=5:
                    d=json.loads(kube('get','deployment/'+target,'-o','json'))
                    pod_items=json.loads(kube('get','pods','-l','app='+target,'-o','json'))['items']
                    capture.observe(pod_items)
                    pods=[]
                    if args.mode=='rolling':
                        for pod in pod_items:
                            if pod['metadata'].get('deletionTimestamp'):continue
                            container=next(c for c in pod['spec']['containers'] if c['name']==target)
                            runtime=next((c for c in pod['status'].get('containerStatuses',[]) if c['name']==target),{})
                            pods.append(dict(name=pod['metadata']['name'],node=pod['spec'].get('nodeName',''),image=container['image'],image_id=runtime.get('imageID',''),ready=runtime.get('ready',False)))
                    snapshots.append(dict(elapsed=elapsed,observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),desired=d['spec']['replicas'],ready=d['status'].get('readyReplicas',0),image=d['spec']['template']['spec']['containers'][0]['image'],pods=pods))
                    if args.mode=='scale':
                        gateways=json.loads(kube('get','pods','-l','app=aggregator-gateway','-o','json'))['items']
                        snapshots[-1]['gateways']=[dict(pod=p['metadata']['name'],uid=p['metadata']['uid'],node=p['spec'].get('nodeName'),
                            containers=[{k:c.get(k) for k in ('ready','restartCount','lastState')} for c in p['status'].get('containerStatuses',[])]) for p in gateways if not p['metadata'].get('deletionTimestamp')]
                    last=elapsed
                    if int(elapsed)%20<5:print(json.dumps(dict(case=name,elapsed=elapsed,vus=value['vus'],ready=snapshots[-1]['ready'],rss_mib=value['rss_mib'])),flush=True)
            code=process.wait()
        finally:
            if process.poll() is None:process.terminate();process.wait(timeout=30)
        result=dict(case=name,passed=False,report=report,snapshots=snapshots,statuses=statuses,longest_full_vu_seconds=span)
        if report and report['summary']:
            metrics=report['summary']['metrics']
            result['timing_integrity']=timing_integrity(metrics)
            result['passed']=code==0 and report['exit_code']==0 and not report['safety_stop'] and all(
                threshold_ok(t) for m in metrics.values() for t in m.get('thresholds',{}).values()) and result['timing_integrity']['valid']
            if args.mode=='scale':
                result['passed'] &= span>=120 and any(x['desired']==5 and x['ready']==5 for x in snapshots) and any(x['desired']==2 and x['ready']==2 for x in snapshots)
                result['passed'] &= metric_values(metrics.get('responses_malformed',{})).get('count',0)==0
                result['gateway_restarts_unchanged']=gateways_stable(snapshots,gateway_restarts)
                result['passed'] &= result['gateway_restarts_unchanged']
                result['non_rejected_semantic_error_rate']=non_rejected_error_rate(metrics)
                result['passed'] &= result['non_rejected_semantic_error_rate']<.01
            if args.mode=='rolling':
                result['rollout_images_observed']={image:rollout_observed(snapshots,image,2 if target=='aggregator-gateway' else 3) for image in (CURRENT,PREVIOUS)}
                digests={p['image_id'] for s in snapshots for p in s['pods'] if p['ready']}
                result['distinct_serving_image_ids']=sorted(digests)
                result['passed'] &= old and new and all(result['rollout_images_observed'].values()) and len(digests)>=2
        results.append(result)
        (directory/(name+'.json')).write_text(json.dumps(result,indent=2))
        print(json.dumps({'case':name,'passed':result['passed'],'peak_rss_mib':report.get('peak_rss_mib') if report else None,'longest_full_vu_seconds':span}),flush=True)
        return result
    try:
        capture.observe(json.loads(kube('get','pods','-l','app='+target,'-o','json'))['items'])
        if args.mode=='throughput':
            # Reverse the order in the second pair; same workload and generator.
            for index,count in enumerate((1,3,3,1)):
                ready(count);time.sleep(10)
                run('replicas-'+str(count)+'-'+str(index+1),'scripts/m2/p2/throughput.js',dict(VUS='1000',DURATION='60s'))
            rates=[metric_values(r['report']['summary']['metrics']['successful_requests'])['rate'] for r in results]
            ratios=[rates[1]/rates[0],rates[2]/rates[3]]
            summary=dict(passed=all(r['passed'] for r in results) and all(r>=2 for r in ratios),successful_rps=rates,ratios=ratios)
        else:
            ready(3)
            run(args.mode,'scripts/m2/p2/'+('load.js' if args.mode=='scale' else 'rolling.js'),
                dict(VUS='10000',HOLD_DURATION='130s') if args.mode=='scale' else dict(VUS='50',DURATION='120s'))
            summary=dict(passed=all(r['passed'] for r in results))
        summary.update(generator='VPS C host, colocated with workloads',rolling_target=target if args.mode=='rolling' else None,cpu_profile_requested=args.cpu_profile,
                       generator_bounds=dict(cpu_cores=6,memory_limit_mib=5120,rss_guard_mib=4608,min_available_mib=768))
        (directory/'result.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
    finally:
        try:
            if args.mode=='rolling':
                kube('set','image','deployment/'+target,target+'='+CURRENT)
                kube('rollout','status','deployment/'+target,'--timeout=180s')
            ready(3)
        finally:
            capture.close()
            profiles.close()
        ssh=shlex.join(h.command(generator,'')[:-2])
        subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,generator['user']+'@'+generator['host']+':/opt/aat/repo/.e2e/'+directory.name+'/',str(directory)+'/'],check=False)
        print('Evidence:',directory.relative_to(ROOT),flush=True)
    raise SystemExit(0 if summary['passed'] else 1)


if __name__=='__main__':main()
