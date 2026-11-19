#!/usr/bin/env python3
"""P2 k6 checks on bounded host C; preserves workloads/PVCs and restores input image.

Generator shares a VPS with application workloads. This is not a dedicated
generator capacity proof. Existing k6 workloads/thresholds are not relaxed.
"""
import argparse
import datetime as dt
import fcntl
import json
from pathlib import Path
import shlex
import subprocess
import time
from provision import Hosts, ROOT, inventory

NS='sistem-informasi-kebencanaan-bnpb'


def threshold_ok(value):
    # Legacy --summary-export stores threshold FAILED booleans, whereas
    # handleSummary stores {ok: boolean}. Preserve both formats unchanged.
    return not value if isinstance(value,bool) else value['ok']


def metric_values(metric):
    return metric.get('values',metric)


def rollout_observed(snapshots, image):
    return any(len(s.get('pods',[])) == s['desired'] and s['desired'] >= 3 and
               all(p['ready'] and p['image'] == image and p['image_id'] for p in s['pods'])
               for s in snapshots)

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
     try:cmd=(p/'cmdline').read_bytes()
     except OSError:continue
     if cmd.startswith(b'/opt/aat/tools/k6\x00'):pid=int(p.name);break
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
    args=p.parse_args()
    if args.mode=='rolling' and not args.previous_image:
        p.error('rolling requires an explicitly schema-compatible --previous-image; do not use pre-v3 images')
    (ROOT/'.vps').mkdir(exist_ok=True)
    lock=(ROOT/'.vps/p2-test.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));server=h.nodes[0];generator=h.nodes[2]
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory=ROOT/'.e2e'/('p2-'+args.mode+'-'+stamp);directory.mkdir(parents=True)
    env={}
    for line in (ROOT/'.env').read_text().splitlines():
        k,sep,v=line.partition('=')
        if sep and k=='AGGREGATOR_API_KEY':env[k]=v.strip().strip('"\'')
    env.update(AGGREGATOR_BASE_URL='http://10.77.0.1:30083',GOMAXPROCS='6',K6_NO_USAGE_REPORT='true')
    def kube(*items):return h.ssh(server,'sudo -n k3s kubectl --request-timeout=10s -n '+NS+' '+shlex.join(items),capture=True).decode()
    CURRENT=json.loads(kube('get','deployment/aggregator','-o','json'))['spec']['template']['spec']['containers'][0]['image']
    PREVIOUS=args.previous_image
    if args.mode=='rolling' and PREVIOUS==CURRENT:
        p.error('rolling comparison must use a different image, not a no-op update')
    def ready(count):
        kube('scale','deployment/aggregator','--replicas='+str(count))
        kube('rollout','status','deployment/aggregator','--timeout=180s')
    results=[]
    def run(name,script,extra):
        remote='/opt/aat/repo/.e2e/'+directory.name+'/'+name
        cfg=dict(env=dict(env,**extra),script=(ROOT/script).read_text(),directory=remote,unit='aat-'+directory.name+'-'+name,timeout=350)
        process=subprocess.Popen(h.command(generator,'sudo -n python3 -c '+shlex.quote(RUNNER)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
        process.stdin.write(json.dumps(cfg));process.stdin.close()
        statuses=[];snapshots=[];report=None;up=down=old=new=False;last=-5;full=None;span=0
        try:
            for line in process.stdout:
                value=json.loads(line)
                if value['type']=='final':report=value;continue
                statuses.append(value);elapsed=value['elapsed']
                (directory/(name+'-live.json')).write_text(json.dumps(dict(statuses=statuses,snapshots=snapshots),indent=2))
                if elapsed is None:continue
                if value['vus']>=10000 and value['running']:
                    if full is None:full=elapsed
                    span=max(span,elapsed-full)
                else:full=None
                if args.mode=='scale':
                    if elapsed>=60 and not up:kube('scale','deployment/aggregator','--replicas=5');up=True
                    if elapsed>=110 and not down:kube('scale','deployment/aggregator','--replicas=2');down=True
                if args.mode=='rolling':
                    if elapsed>=20 and not old:
                        kube('set','image','deployment/aggregator','aggregator='+PREVIOUS)
                        old=True
                    if elapsed>=70 and not new:
                        kube('set','image','deployment/aggregator','aggregator='+CURRENT)
                        new=True
                if elapsed-last>=5:
                    d=json.loads(kube('get','deployment/aggregator','-o','json'))
                    pods=[]
                    if args.mode=='rolling':
                        for pod in json.loads(kube('get','pods','-l','app=aggregator','-o','json'))['items']:
                            if pod['metadata'].get('deletionTimestamp'):continue
                            container=next(c for c in pod['spec']['containers'] if c['name']=='aggregator')
                            runtime=next((c for c in pod['status'].get('containerStatuses',[]) if c['name']=='aggregator'),{})
                            pods.append(dict(name=pod['metadata']['name'],node=pod['spec'].get('nodeName',''),image=container['image'],image_id=runtime.get('imageID',''),ready=runtime.get('ready',False)))
                    snapshots.append(dict(elapsed=elapsed,observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),desired=d['spec']['replicas'],ready=d['status'].get('readyReplicas',0),image=d['spec']['template']['spec']['containers'][0]['image'],pods=pods))
                    last=elapsed
                    if int(elapsed)%20<5:print(json.dumps(dict(case=name,elapsed=elapsed,vus=value['vus'],ready=snapshots[-1]['ready'],rss_mib=value['rss_mib'])),flush=True)
            code=process.wait()
        finally:
            if process.poll() is None:process.terminate();process.wait(timeout=30)
        result=dict(case=name,passed=False,report=report,snapshots=snapshots,statuses=statuses,longest_full_vu_seconds=span)
        if report and report['summary']:
            metrics=report['summary']['metrics']
            result['passed']=code==0 and report['exit_code']==0 and not report['safety_stop'] and all(
                threshold_ok(t) for m in metrics.values() for t in m.get('thresholds',{}).values()) and metric_values(metrics['http_req_duration'])['min']>=0
            if args.mode=='scale':
                result['passed'] &= span>=120 and any(x['desired']==5 and x['ready']==5 for x in snapshots) and any(x['desired']==2 and x['ready']==2 for x in snapshots)
                result['passed'] &= metric_values(metrics.get('responses_malformed',{})).get('count',0)==0
            if args.mode=='rolling':
                result['rollout_images_observed']={image:rollout_observed(snapshots,image) for image in (CURRENT,PREVIOUS)}
                digests={p['image_id'] for s in snapshots for p in s['pods'] if p['ready']}
                result['distinct_serving_image_ids']=sorted(digests)
                result['passed'] &= old and new and all(result['rollout_images_observed'].values()) and len(digests)>=2
        results.append(result)
        (directory/(name+'.json')).write_text(json.dumps(result,indent=2))
        print(json.dumps({'case':name,'passed':result['passed'],'peak_rss_mib':report.get('peak_rss_mib') if report else None,'longest_full_vu_seconds':span}),flush=True)
        return result
    try:
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
        summary.update(generator='VPS C host, colocated with workloads',generator_bounds=dict(cpu_cores=6,memory_limit_mib=5120,rss_guard_mib=4608,min_available_mib=768))
        (directory/'result.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
    finally:
        kube('set','image','deployment/aggregator','aggregator='+CURRENT);ready(3)
        ssh=shlex.join(h.command(generator,'')[:-2])
        subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,generator['user']+'@'+generator['host']+':/opt/aat/repo/.e2e/'+directory.name+'/',str(directory)+'/'],check=False)
        print('Evidence:',directory.relative_to(ROOT),flush=True)
    raise SystemExit(0 if summary['passed'] else 1)


if __name__=='__main__':main()
