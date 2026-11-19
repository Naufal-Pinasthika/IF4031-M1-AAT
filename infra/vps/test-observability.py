#!/usr/bin/env python3
"""Read-only dashboard checks plus one real authenticated request workflow.

Run access.py observe first. Tokens remain inside the VPS process; only IDs,
HTTP status, timing, service names and coverage metadata leave it.
"""
import argparse
import datetime as dt
import json
import urllib.request
import uuid
from provision import Hosts,ROOT,inventory


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--require-async',action='store_true',help='Also require observed broker/consumer steps and durations')
    args=parser.parse_args()
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));n=h.nodes[0]
    correlation='p5-ui-proof-'+uuid.uuid4().hex
    code=r'''
import base64,json,subprocess,time,urllib.request
NS='sistem-informasi-kebencanaan-bnpb'
def k(kind,name):return json.loads(subprocess.check_output(['k3s','kubectl','-n',NS,'get',kind,name,'-o','json']))['data']
c=k('configmap','auth-config');s=k('secret','auth-credentials')
id=CORRELATION
def req(port,path,token=None,body=None):
 headers={'X-Correlation-ID':id,'Content-Type':'application/json'}
 if token:headers['Authorization']='Bearer '+token
 if port==30084:headers['Authorization']='Basic '+base64.b64encode((c['FIELD_CLIENT_ID']+':'+base64.b64decode(s['FIELD_CLIENT_SECRET']).decode()).encode()).decode()
 start=time.monotonic()
 with urllib.request.urlopen(urllib.request.Request('http://10.77.0.1:'+str(port)+path,headers=headers,data=json.dumps(body).encode() if body else None),timeout=8) as r:
  data=json.load(r);return r.status,data,(time.monotonic()-start)*1000
status,tokens,_=req(30084,'/v1/token',body={'grant_type':'client_credentials'})
status,page,ms=req(30080,'/events?source=BMKG&limit=37',tokens['access_token'])
assert status==200 and page.get('events') and not page.get('partial')
pods=json.loads(subprocess.check_output(['k3s','kubectl','-n',NS,'get','pods','-l','app=aggregator','-o','json']))['items']
poll=None
for p in pods:
 raw=subprocess.check_output(['k3s','kubectl','-n',NS,'logs',p['metadata']['name'],'--tail=300'],text=True)
 for line in raw.splitlines():
  try:record=json.loads(line)
  except ValueError:continue
  if record.get('msg')=='coastal_batch_received' and record.get('events',0)>0:poll=record['correlation_id']
print(json.dumps({'correlation_id':id,'http_status':status,'client_total_ms':round(ms,3),'poll_correlation':poll}))
'''.replace('CORRELATION',repr(correlation))
    proof=json.loads(h.ssh(n,'sudo -n python3 -',code.encode(),capture=True))
    def fetch(path):
        with urllib.request.urlopen('http://localhost:8090'+path,timeout=60) as r:return json.load(r)
    metrics=fetch('/api/query?query=up')['data']['result']
    apps={x['metric']['app'] for x in metrics if x['value'][1]=='1'}
    required={'aggregator','auth','client-api','bmkg','pvmbg','dashboard-consumer','notifier-consumer'}
    trace=fetch('/api/trace?id='+correlation)
    services={x['service'] for x in trace['steps']}
    if not {'auth','client-api','aggregator'}<=services:raise RuntimeError('fresh workflow trace lacks a traversed application service')
    if not all('latency_ms' in x for x in trace['steps'] if x.get('msg')=='http_request'):raise RuntimeError('HTTP trace duration missing')
    event_trace=fetch('/api/trace?id='+proof['poll_correlation']) if proof['poll_correlation'] else None
    async_steps=[s for s in (event_trace or {}).get('steps',[]) if s.get('msg') in {
        'hazard_event_published','kafka_fetch_completed','kafka_commit_completed',
        'notification_delivered','dashboard_event_applied'}]
    async_complete=({'aggregator','dashboard-consumer','notifier-consumer'} <= {s['service'] for s in async_steps}
        and {'hazard_event_published','kafka_fetch_completed','kafka_commit_completed','notification_delivered','dashboard_event_applied'} <= {s['msg'] for s in async_steps}
        and all(isinstance(s.get('latency_ms'),(int,float)) and s['latency_ms']>=0 for s in async_steps))
    proof.update(scraped_services=sorted(apps),all_seven_services_scraped=required<=apps,
        workflow_trace_services=sorted(services),workflow_trace_steps=len(trace['steps']),
        event_trace_services=sorted({x['service'] for x in event_trace['steps']}) if event_trace else [],
        event_trace_messages=sorted({x.get('msg','') for x in event_trace['steps']}) if event_trace else [],
        async_trace_observed=async_complete,async_required=args.require_async,
        async_step_durations=[{k:s[k] for k in ('service','msg','latency_ms') if k in s} for s in async_steps],
        trace_coverage=trace['coverage'],scope=trace['scope'],passed=required<=apps and (not args.require_async or async_complete))
    output=ROOT/'.e2e'/('p5-observability-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.json')
    output.parent.mkdir(exist_ok=True);output.write_text(json.dumps(proof,indent=2))
    output.with_suffix('.traces.json').write_text(json.dumps({'workflow':trace,'event':event_trace},indent=2))
    print(json.dumps(proof,indent=2));print('Evidence:',output.relative_to(ROOT))
    if not proof['passed']:raise SystemExit(1)


if __name__=='__main__':main()
