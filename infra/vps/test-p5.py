#!/usr/bin/env python3
"""Repeated P5 load/fault runs from WSL over the private operator mesh.

The official sensor and Tester Agent are never changed. Network faults target
only current Aggregator netns -> the Sensor Service TCP/8080. Rule cleanup
is on the remote shell's EXIT trap and each fault has a bounded duration.
"""
import argparse
import concurrent.futures
import datetime
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import random
import re
import shlex
import subprocess
import threading
import time
import uuid
import urllib.request
from provision import Hosts, ROOT, inventory

NS = 'sistem-informasi-kebencanaan-bnpb'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['isolation','priority'], default='isolation')
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--seconds', type=int, default=75)
    p.add_argument('--k6', default='/mnt/d/aat-m2-e2e/k6/k6-v2.4.0-linux-amd64/k6')
    p.add_argument('--faults', action='store_true')
    p.add_argument('--pvmbg-outage', action='store_true', help='Combine the sensor fault with a bounded PVMBG admin outage')
    p.add_argument('--verify-source-health', action='store_true', help='Require consistent PVMBG freshness markers on every Aggregator during outage and recovery')
    p.add_argument('--media-vus', type=int, default=300)
    p.add_argument('--generator', choices=['local','node-b'], default='local')
    args = p.parse_args()
    if not 1 <= args.rounds <= 20 or not 60 <= args.seconds <= 600 or not 1 <= args.media_vus <= 1000:
        p.error('invalid bounded test parameters')
    if args.pvmbg_outage and not args.faults:
        p.error('--pvmbg-outage requires --faults')
    if args.verify_source_health and not args.pvmbg_outage:
        p.error('--verify-source-health requires --pvmbg-outage')
    h = Hosts(inventory(ROOT/'infra/vps/inventory.json')); server=h.nodes[0]
    # Serialize controllers: overlapping loads must be explicitly requested as
    # a separate combined scenario, not accidentally contaminate clean rounds.
    (ROOT/'.vps').mkdir(exist_ok=True)
    run_lock=(ROOT/'.vps/p5-test.lock').open('a')
    try:
        fcntl.flock(run_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Another P5 load controller is running; refusing accidental overlap')
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory=ROOT/'.e2e'/('p5-'+args.mode+'-'+stamp)
    directory.mkdir(parents=True,exist_ok=False)
    values={}
    for line in (ROOT/'.env').read_text().splitlines():
        key,sep,value=line.partition('=')
        if sep and not key.startswith('#'): values[key]=value.strip().strip('"\'')
    env=os.environ.copy()
    for kind in ('MEDIA','FIELD'):
        for suffix in ('CLIENT_ID','CLIENT_SECRET'): env[kind+'_'+suffix]=values[kind+'_'+suffix]
    env.update(CLIENT_API_BASE_URL='http://10.77.0.1:30080',AUTH_BASE_URL='http://10.77.0.1:30084',
               DURATION=str(args.seconds)+'s',MODE=args.mode,MEDIA_VUS=str(args.media_vus),K6_NO_USAGE_REPORT='true',GOMAXPROCS='4')

    def kube(*items):
        return h.ssh(server,'sudo -n k3s kubectl -n '+NS+' '+shlex.join(items),capture=True).decode()

    outcomes=[]
    for index in range(args.rounds):
        result=dict(round=index+1,mode=args.mode,passed=False,transport='WireGuard from operator WSL' if args.generator=='local' else 'VPS B host -> private NodePort on A; generator outside app pods, colocated with B workloads',faults=[],fault_errors=[])
        result['started_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        summary=directory/f'round-{index+1}.json'
        env['SUMMARY_PATH']=str(summary)
        workers=[]
        def inject():
            try:
                delay=random.SystemRandom().uniform(10,16)
                duration=random.SystemRandom().uniform(12,18)
                time.sleep(delay)
                pods=json.loads(kube('get','pods','-o','json'))['items']
                sensor=next(x for x in pods if x['metadata'].get('labels',{}).get('app')=='sensor-pantai' and not x['metadata'].get('deletionTimestamp'))
                target=str(ipaddress.ip_address(json.loads(kube('get','service','sensor-pantai','-o','json'))['spec']['clusterIP']))
                aggregators=[x for x in pods if x['metadata'].get('labels',{}).get('app')=='aggregator' and not x['metadata'].get('deletionTimestamp')]
                if len(aggregators)!=3 or not ipaddress.ip_address(target).is_private:
                    raise RuntimeError('expected exactly three Aggregators and a private Sensor Service')
                marker='aat-p5-'+uuid.uuid4().hex[:12]
                def block(pod):
                    node=next(n for n in h.nodes if n['name']==pod['spec']['nodeName'])
                    container=pod['status']['containerStatuses'][0]['containerID'].removeprefix('containerd://')
                    if not re.fullmatch('[0-9a-f]{64}',container):
                        raise RuntimeError('invalid runtime container identifier')
                    script='''set -euo pipefail
pid=$(k3s crictl inspect CONTAINER | python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["pid"])')
[[ "$pid" =~ ^[0-9]+$ ]] && [[ "$pid" -gt 1 ]]
added=false
cleanup() {
  if [[ "$added" == true ]] && [[ -e /proc/$pid/ns/net ]]; then
    nsenter -t "$pid" -n iptables -w -D OUTPUT -d TARGET -p tcp --dport 8080 -m comment --comment MARKER -j DROP
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP
nsenter -t "$pid" -n iptables -w -I OUTPUT 1 -d TARGET -p tcp --dport 8080 -m comment --comment MARKER -j DROP
added=true
echo 'Sensor fault applied in Aggregator network namespace'
sleep DURATION
nsenter -t "$pid" -n iptables -w -nvxL OUTPUT | grep -F MARKER
'''.replace('TARGET',target).replace('MARKER',marker).replace('CONTAINER',container).replace('DURATION',str(round(duration,2)))
                    return node['name']+'\n'+h.ssh(node,'sudo -n bash -s',script.encode(),capture=True).decode()
                result['faults'].append(dict(type='sensor TCP blackhole at each Aggregator netns',at=time.time(),seconds=duration,
                                             target=target,marker=marker,pods=[p['metadata']['name'] for p in aggregators]))
                def pvmbg_fault():
                    script='''import base64,json,subprocess,time,urllib.request
ns='sistem-informasi-kebencanaan-bnpb'
def obj(kind,name):
 return json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get',kind,name,'-o','json']))
token=base64.b64decode(obj('secret','pvmbg-credentials')['data']['PVMBG_TOKEN']).decode()
address=obj('service','pvmbg')['spec']['clusterIP']
def toggle():
 req=urllib.request.Request('http://'+address+':8082/admin/outage',method='POST',headers={'Authorization':'Bearer '+token,'X-Correlation-ID':'p5-scheduled-outage'})
 with urllib.request.urlopen(req,timeout=5) as response: return json.load(response)['outage_enabled']
enabled=toggle()
if not enabled:
 toggle()
 raise RuntimeError('PVMBG already in outage; restored its pre-test state and aborted')
print('PVMBG outage enabled',flush=True)
try:
 if VERIFY:
  time.sleep(8)
  key=base64.b64decode(obj('secret','aggregator-credentials')['data']['AGGREGATOR_API_KEY']).decode()
  pods=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get','pods','-l','app=aggregator','-o','json']))['items']
  states=[]
  for pod in pods:
   if pod['metadata'].get('deletionTimestamp'): continue
   req=urllib.request.Request('http://'+pod['status']['podIP']+':8083/internal/events?source=PVMBG&limit=1',headers={'X-Aggregator-Key':key})
   with urllib.request.urlopen(req,timeout=3) as response: page=json.load(response)
   state=page['sources']['PVMBG'];states.append({'pod':pod['metadata']['name'],'state':state,'partial':page['partial']})
   assert state['state'] in ('STALE','UNAVAILABLE') and state.get('stale_since'), 'Missing outage freshness marker'
  assert len(states)==3, 'Expected three read replicas'
  print('PVMBG per-replica freshness: '+json.dumps(states),flush=True)
  time.sleep(max(0,DURATION-8))
 else: time.sleep(DURATION)
finally:
 if toggle(): raise RuntimeError('PVMBG outage release did not restore normal state')
print('PVMBG outage automatically released',flush=True)
'''.replace('DURATION',str(round(duration,2))).replace('VERIFY',str(args.verify_source_health))
                    return h.ssh(server,'sudo -n python3 -',script.encode(),capture=True).decode()
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    extra=pool.submit(pvmbg_fault) if args.pvmbg_outage else None
                    result['fault_packet_counters']='\n'.join(pool.map(block,aggregators))
                    if extra:
                        result['pvmbg_fault_log']=extra.result()
                result['faults'].append(dict(type='sensor rules automatically removed',at=time.time()))
            except Exception as exc:
                result['fault_errors'].append(type(exc).__name__+': '+str(exc))
        log=directory/f'round-{index+1}.log'
        with log.open('w') as stream:
            remote_summary='/opt/aat/regression/p5-'+stamp+'/'+f'{args.mode}-{index+1}.json'
            if args.generator=='node-b':
                code='''import json,sys,os,subprocess
from pathlib import Path
cfg=json.load(sys.stdin)
env=os.environ.copy();env.update(cfg['env'])
Path(env['SUMMARY_PATH']).parent.mkdir(parents=True,exist_ok=True,mode=0o700)
proc=subprocess.run(['systemd-run','--quiet','--scope','-p','MemoryMax=1536M','-p','CPUQuota=400%',
 '/opt/aat/tools/k6','run','--quiet','-'],input=cfg['script'].encode(),env=env)
raise SystemExit(proc.returncode)
'''
                private={key:value for key,value in env.items() if key in ('MEDIA_CLIENT_ID','MEDIA_CLIENT_SECRET','FIELD_CLIENT_ID','FIELD_CLIENT_SECRET','CLIENT_API_BASE_URL','AUTH_BASE_URL','DURATION','MODE','MEDIA_VUS','K6_NO_USAGE_REPORT','GOMAXPROCS')}
                private['SUMMARY_PATH']=remote_summary
                payload=json.dumps(dict(env=private,script=(ROOT/'scripts/m2/p5/07-isolation-load.js').read_text())).encode()
                process=subprocess.Popen(h.command(h.nodes[1],'sudo -n python3 -c '+shlex.quote(code)),stdin=subprocess.PIPE,stdout=stream,stderr=subprocess.STDOUT)
                process.stdin.write(payload);process.stdin.close()
                result['generator_bounds']={'memory_mib':1536,'cpu_cores':4}
            else:
                process=subprocess.Popen([args.k6,'run','--quiet',str(ROOT/'scripts/m2/p5/07-isolation-load.js')],env=env,stdout=stream,stderr=subprocess.STDOUT)
            if args.faults:
                worker=threading.Thread(target=inject);worker.start();workers.append(worker)
            peak=0
            while process.poll() is None:
                mem=dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
                available=int(mem['MemAvailable'].split()[0])/1024
                try:
                    status=Path(f'/proc/{process.pid}/status').read_text()
                    rss=next((int(x.split()[1])/1024 for x in status.splitlines() if x.startswith('VmRSS:')),0)
                    peak=max(peak,rss)
                except FileNotFoundError: break
                if available<512 or rss>2048:
                    result['safety_stop']='generator memory guard'
                    process.terminate()
                    break
                time.sleep(1)
            try: code=process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill();code=process.wait()
            for worker in workers: worker.join(timeout=60)
        result.update(k6_exit=code,peak_launcher_rss_mib=peak)
        if args.generator=='node-b':
            try:
                summary.write_bytes(h.ssh(h.nodes[1],'sudo -n cat '+shlex.quote(remote_summary),capture=True))
            except Exception as exc:
                result['fault_errors'].append('missing generator summary: '+str(exc))
        if summary.exists():
            metrics=json.loads(summary.read_text())['metrics']
            result['metrics']={key:value for key,value in metrics.items() if key in ('bmkg_complete_latency_ms','sensor_read_complete_latency_ms','sensor_read_failures','complete_latency_ms','complete_responses','unexpected_errors','field_failures','media_429','invalid_timing','media_privacy_failures','dropped_iterations','http_reqs','http_req_failed','auth_responses','business_status')}
        try:
            request=urllib.request.Request('http://10.77.0.1:30083/internal/polling?source=SENSOR_PANTAI&limit=1000',headers={'X-Aggregator-Key':values['AGGREGATOR_API_KEY']})
            with urllib.request.urlopen(request,timeout=5) as response: polls=json.load(response)['items']
            polls=[x for x in polls if x['started_at']>=result['started_at']]
            (directory/f'polls-{index+1}.json').write_text(json.dumps(polls,indent=2))
            result['source_poll_statuses']={status:sum(x['status']==status for x in polls) for status in ('completed','uncertain','in_flight')}
            result['source_poll_errors']=list({x.get('last_error') for x in polls if x.get('last_error')})
            def epoch(value):
                # Python 3.10 accepts 3/6 fractional digits, Go emits 0..9.
                value=re.sub(r'\.(\d+)(?=Z|[+-])',lambda m:'.'+m[1][:6].ljust(6,'0'),value)
                return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
            if args.faults and len(result['faults']) >= 2:
                applied=result['faults'][0]['at']; released=result['faults'][-1]['at']
                actual=[x for x in polls if x['status']=='completed' or (x['status']=='uncertain' and x.get('last_error')!='coastal circuit is open')]
                before=[x for x in actual if epoch(x['started_at'])<applied]
                during=[x for x in actual if applied<=epoch(x['started_at'])<=released]
                after=[x for x in polls if x['status']=='completed' and epoch(x['started_at'])>released]
                result['source_actual_cycle_rate_before']=len(before)/max(1,applied-epoch(result['started_at']))
                result['source_actual_cycle_rate_during']=len(during)/max(1,released-applied)
                result['source_recovery_seconds']=max(0,min(epoch(x['finished_at']) for x in after)-released) if after else None
                result['source_request_rate_scope']='Cycle proxy from ledger, not physical HTTP counter; official sensor request logs unavailable'
                if not after:
                    result['fault_errors'].append('no successful sensor cycle after automatic fault release')
                if not any(int(line.split()[0])>0 for line in result.get('fault_packet_counters','').splitlines() if ' DROP ' in line):
                    result['fault_errors'].append('fault had no observed dropped packets')
        except Exception as exc:
            result['fault_errors'].append('poll evidence unavailable: '+str(exc))
        result['passed']=code==0 and not result['fault_errors'] and not result.get('safety_stop') and all(not w.is_alive() for w in workers)
        result['pods_after']=kube('get','pods','-o','wide')
        if args.verify_source_health:
            code='''import base64,json,subprocess,urllib.request
ns='sistem-informasi-kebencanaan-bnpb'
data=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get','secret','aggregator-credentials','-o','json']))
key=base64.b64decode(data['data']['AGGREGATOR_API_KEY']).decode()
pods=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get','pods','-l','app=aggregator','-o','json']))['items']
states=[]
for pod in pods:
 if pod['metadata'].get('deletionTimestamp'): continue
 req=urllib.request.Request('http://'+pod['status']['podIP']+':8083/internal/events?source=PVMBG&limit=1',headers={'X-Aggregator-Key':key})
 with urllib.request.urlopen(req,timeout=3) as response: page=json.load(response)
 state=page['sources']['PVMBG'];states.append({'pod':pod['metadata']['name'],'state':state})
 assert state['state']=='UP' and not state.get('stale_since'), 'Source did not recover on a read replica'
assert len(states)==3
print(json.dumps(states))
'''
            try:
                result['source_health_recovered']=json.loads(h.ssh(server,'sudo -n python3 -',code.encode(),capture=True))
            except Exception as exc:
                result['fault_errors'].append('shared freshness recovery failed: '+str(exc))
                result['passed']=False
        outcomes.append(result)
        (directory/'result.json').write_text(json.dumps(dict(passed=all(r['passed'] for r in outcomes),rounds=outcomes),indent=2))
        print(json.dumps({key:result[key] for key in ('round','mode','passed','k6_exit','fault_errors','metrics') if key in result}),flush=True)
    print('Evidence:',directory.relative_to(ROOT),flush=True)
    raise SystemExit(0 if all(r['passed'] for r in outcomes) else 1)


if __name__=='__main__':main()
