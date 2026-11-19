#!/usr/bin/env python3
"""Collect bounded upstream/ledger and identity evidence after P2 experiments.

Operator source-reference requests are explicitly tagged and retained separately
from application polling. No source or application configuration is modified.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import re
import shlex
import subprocess
from provision import Hosts,ROOT,inventory

COLLECT=r'''
import base64,datetime as dt,importlib.util,json,re,subprocess,time,urllib.parse,urllib.request,uuid
from pathlib import Path
ns='sistem-informasi-kebencanaan-bnpb';directory=Path(DIRECTORY);directory.mkdir(parents=True,exist_ok=True)
def kube(*args):return subprocess.check_output(['k3s','kubectl','--request-timeout=15s','-n',ns,*args],text=True,timeout=30)
def sql(q):return kube('exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',q).strip()
def secret(name):return {k:base64.b64decode(v).decode() for k,v in json.loads(kube('get','secret',name,'-o','json'))['data'].items()}
keys=secret('aggregator-credentials');reference_calls=[]
def req(base,path,headers):
 id='p2-reference-'+uuid.uuid4().hex;headers=dict(headers,**{'X-Correlation-ID':id})
 with urllib.request.urlopen(urllib.request.Request(base+path,headers=headers),timeout=10) as r:
  data=json.load(r);reference_calls.append(dict(correlation_id=id,path=path,status=r.status));return data
def service(name,port):return 'http://'+json.loads(kube('get','service',name,'-o','json'))['spec']['clusterIP']+':'+str(port)
expected={}
bmkg=service('bmkg',8081)
quakes=req(bmkg,'/seismic-events?since=1970-01-01T00:00:00Z',{'X-BMKG-Key':keys['BMKG_API_KEY']})
warnings=req(bmkg,'/tsunami-warnings?since=1970-01-01T00:00:00Z',{'X-BMKG-Key':keys['BMKG_API_KEY']})
expected['BMKG']={x['event_id'] for x in quakes}|{x['warning_id'] for x in warnings}
reports=req(service('pvmbg',8082),'/volcanic-reports?since=1970-01-01T00:00:00Z',{'Authorization':'Bearer '+keys['PVMBG_TOKEN']})
expected['PVMBG']={x['report_id'] for x in reports}
spec=importlib.util.spec_from_file_location('observer','/opt/aat/repo/scripts/m2/p5/06-reliability.py')
observer=importlib.util.module_from_spec(spec);spec.loader.exec_module(observer)
sensor=service('sensor-pantai',8080);auth={'Authorization':'Bearer '+keys['PANTAI_AUTH_TOKEN']}
stations={x['station_id'] for x in req(sensor,'/v1/stations',auth)['items']}
cursor='';seen=set();expected['SENSOR_PANTAI']=set();invalid=0;tail=False
for _ in range(4096):
 page=req(sensor,'/v1/observations?limit=1000'+('&cursor='+urllib.parse.quote(cursor,safe='') if cursor else ''),auth)
 for item in page['items']:
  if observer.valid(item,stations):expected['SENSOR_PANTAI'].add(item['observation_id'])
  else:invalid+=1
 if not page['items']:tail=True;break
 next_cursor=page.get('next_cursor')
 if not next_cursor or next_cursor in seen:raise RuntimeError('reference sensor did not advance to empty tail')
 seen.add(next_cursor);cursor=next_cursor
if not tail:raise RuntimeError('reference bound exceeded')
for source,ids in expected.items():(directory/('expected-'+source+'.json')).write_text(json.dumps(sorted(ids)))
checks=[]
for source,target in expected.items():
 deadline=time.monotonic()+60
 partial_attempts=0
 while True:
  cursor='';events=[];seen=set();incomplete=False
  while True:
   page=req('http://10.77.0.1:30083','/internal/audit/events?source='+source+'&limit=1000'+('&cursor='+urllib.parse.quote(cursor,safe='') if cursor else ''),{'Authorization':'Bearer '+keys['P1_AUDIT_TOKEN']})
   if page['partial']:incomplete=True;break
   events.extend(page['items']);cursor=page.get('next_cursor')
   if not cursor:break
   if cursor in seen:raise RuntimeError('Audit cursor repeated')
   seen.add(cursor)
  if incomplete:
   partial_attempts+=1
   if time.monotonic()>=deadline:raise RuntimeError('Audit remained partial beyond drain deadline')
   time.sleep(2);continue
  refs=[x['source_ref_id'] for x in events];missing=target-set(refs)
  if not missing or time.monotonic()>=deadline:break
  time.sleep(2)
 check=dict(source=source,reference_ids=len(target),canonical_ids=len(refs),duplicate_identities=len(refs)-len(set(refs)),missing_ids=sorted(missing),partial_audit_attempts=partial_attempts,passed=bool(target) and not missing and len(refs)==len(set(refs)))
 checks.append(check)
 (directory/('canonical-'+source+'.json')).write_text(json.dumps(events))
 print(json.dumps(check),flush=True)
upper=int(sql('SELECT COALESCE(max(poll_id),0) FROM poll_attempts'))
ledger=json.loads(sql("SELECT COALESCE(json_agg(t),'[]') FROM (SELECT poll_id,source,holder_id,fencing_token,previous_poll_id,status,started_at,finished_at,last_error,checkpoint FROM poll_attempts WHERE poll_id>LOWER AND poll_id<="+str(upper)+" ORDER BY poll_id) t"))
(directory/'ledger-all.json').write_text(json.dumps({'items':ledger},indent=2))
mock_ledger=[x for x in ledger if x['source'] in ('BMKG','PVMBG')]
(directory/'ledger-mocks.json').write_text(json.dumps({'items':mock_ledger},indent=2))
records=[];invalid_logs=0;operator=[];poll_pattern=re.compile(r'^poll-(BMKG|PVMBG|SENSOR_PANTAI)-(\d+)$')
start=dt.datetime.fromisoformat(START.replace('Z','+00:00'))
for source in ('bmkg','pvmbg'):
 raw=kube('logs','-l','app='+source,'--all-containers=true','--prefix=false','--tail=-1')
 (directory/('upstream-'+source+'-all.jsonl')).write_text(raw)
 window=[]
 for line in raw.splitlines():
  try:item=json.loads(line)
  except ValueError:invalid_logs+=1;continue
  id=item.get('correlation_id','');match=poll_pattern.fullmatch(id)
  if match and LOWER<int(match[2])<=upper:window.append(item);continue
  if id.startswith('p2-reference-'):operator.append(item);continue
  if not match and item.get('msg')=='http_request' and item.get('path') in ('/seismic-events','/tsunami-warnings','/volcanic-reports'):
   stamp=re.sub(r'\.(\d{6})\d+(?=Z|[+-])',r'.\1',item['time'])
   t=dt.datetime.fromisoformat(stamp.replace('Z','+00:00'))
   if t>=start:window.append(item)
 records.extend(window)
 (directory/('upstream-'+source+'-window.jsonl')).write_text('\n'.join(json.dumps(x) for x in window)+'\n')
spec=importlib.util.spec_from_file_location('checker','/opt/aat/repo/scripts/m2/p2/check_polling.py')
checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
polling=checker.check([{'items':mock_ledger}],records)
polling['non_json_lines']=invalid_logs
if invalid_logs:polling['passed']=False
(directory/'polling-check.json').write_text(json.dumps(polling,indent=2))
same_epoch=int(sql("SELECT count(*) FROM (SELECT source,fencing_token FROM ingestion_outbox WHERE accepted_at>='START'::timestamptz GROUP BY source,fencing_token HAVING count(DISTINCT holder_id)>1) t"))
cadence=[]
for source,ms in [('BMKG',5000),('PVMBG',5000),('SENSOR_PANTAI',500)]:
 rows=sorted((dt.datetime.fromisoformat(x['started_at'].replace('Z','+00:00')) for x in ledger if x['source']==source))
 gaps=[(b-a).total_seconds()*1000 for a,b in zip(rows,rows[1:])]
 cadence.append(dict(source=source,attempts=len(rows),minimum_start_gap_ms=min(gaps) if gaps else None,required_gap_ms=ms,passed=bool(gaps) and min(gaps)>=ms))
result=dict(passed=all(x['passed'] for x in checks) and polling['passed'] and same_epoch==0 and all(x['passed'] for x in cadence),identities=checks,polling=polling,cadence=cadence,same_epoch_multiple_holders=same_epoch,reference_invalid_items=invalid,known_operator_upstream_requests=len(operator),start_poll_id=LOWER,end_poll_id=upper,
 scope='Bounded source snapshots vs Audit identities; mock upstream correlation/cadence. Not official checksum/payload oracle or absolute physical HTTP exactly-once. Official sensor per-request correlated logs unavailable.')
(directory/'operator-reference-calls.json').write_text(json.dumps(reference_calls,indent=2))
(directory/'operator-upstream-logs.json').write_text(json.dumps(operator,indent=2))
(directory/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
raise SystemExit(0 if result['passed'] else 1)
'''


def render(start,remote):
    stamp=start['database_time'].replace('Z','+00:00')
    if re.search(r'[+-]\d{2}$',stamp):stamp+=':00'
    dt.datetime.fromisoformat(stamp)
    code=COLLECT.replace('DIRECTORY',repr(remote)).replace('LOWER',str(int(start['poll_id'])))
    code=code.replace('START.replace',repr(stamp)+'.replace')
    return code.replace("'START'",repr(stamp))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--start',type=Path,required=True)
    args=p.parse_args();start=json.loads(args.start.read_text())
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));server=h.nodes[0]
    name=args.start.parent.name.replace('control','evidence')
    directory=ROOT/'.e2e'/name;directory.mkdir(exist_ok=True)
    remote='/opt/aat/repo/.e2e/'+name
    code=render(start,remote)
    result=subprocess.run(h.command(server,'sudo -n python3 -'),input=code.encode())
    ssh=shlex.join(h.command(server,'')[:-2])
    subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,server['user']+'@'+server['host']+':'+remote+'/',str(directory)+'/'],check=True)
    print('Evidence:',directory.relative_to(ROOT));raise SystemExit(result.returncode)


if __name__=='__main__':main()
