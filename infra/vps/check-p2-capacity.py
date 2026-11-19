#!/usr/bin/env python3
"""Read-only deployed P2 topology, negotiation, auth and bounded-view checks."""
import argparse
import datetime as dt
import json
import shlex

from provision import Hosts,ROOT,inventory

REMOTE=r'''
import gzip,json,subprocess,time,urllib.request,urllib.error,uuid
ns='sistem-informasi-kebencanaan-bnpb'
def kube(*args):return json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,*args]))
key=None
for line in open('/opt/aat/repo/.env'):
 name,sep,value=line.strip().partition('=')
 if name=='AGGREGATOR_API_KEY' and sep:key=value.strip('"\'')
if not key:raise RuntimeError('Missing existing operator credential')
rows=[]
for encoding in ['gzip','identity']*5:
 correlation='capacity-contract-'+uuid.uuid4().hex
 req=urllib.request.Request('http://10.77.0.1:30083/internal/events?limit=20',headers={'X-Aggregator-Key':key,'X-Correlation-ID':correlation,'Accept-Encoding':encoding})
 began=time.monotonic()
 with urllib.request.urlopen(req,timeout=5) as response:
  wire=response.read();actual=response.headers.get('Content-Encoding','identity')
  payload=gzip.decompress(wire) if actual=='gzip' else wire
  data=json.loads(payload)
  rows.append(dict(requested_encoding=encoding,actual_encoding=actual,wire_bytes=len(wire),decoded_bytes=len(payload),partial=data.get('partial'),events=len(data.get('events',[])),gateway=response.headers.get('X-Aggregator-Gateway'),correlation_id=correlation,latency_ms=(time.monotonic()-began)*1000))
 try:
  bad=urllib.request.Request(req.full_url,headers={'X-Aggregator-Key':'wrong','X-Request-Priority':'field'})
  urllib.request.urlopen(bad,timeout=3)
  raise RuntimeError('Wrong credential was accepted')
 except urllib.error.HTTPError as err:
  if err.code!=401:raise
pods=kube('get','pods','-l','app=aggregator','-o','json')['items']
gateways=kube('get','pods','-l','app=aggregator-gateway','-o','json')['items']
def snapshot(items):return [dict(pod=p['metadata']['name'],node=p['spec'].get('nodeName'),containers=p['status'].get('containerStatuses',[]),resources=p['spec']['containers'][0]['resources']) for p in items if not p['metadata'].get('deletionTimestamp')]
services={name:kube('get','service/'+name,'-o','json')['spec'] for name in ['aggregator','aggregator-vps','aggregator-backends']}
metrics={}
for p in pods:
 with urllib.request.urlopen('http://'+p['status']['podIP']+':8083/metrics',timeout=3) as response:
  metrics[p['metadata']['name']]=[line for line in response.read().decode().splitlines() if line.startswith(('aggregator_read_','aggregator_projection_'))]
sql="SELECT json_build_object('rows',count(*),'payload_bytes',coalesce(sum(octet_length(payload)),0),'largest_page',coalesce(max(octet_length(payload)),0)) FROM read_page_projection"
view=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',sql]))
result=dict(passed=all(r['partial'] is False and r['gateway']=='least-active' and (r['requested_encoding']!='gzip' or r['actual_encoding']=='gzip') for r in rows) and view['rows']<=32 and view['largest_page']<=1048576,requests=rows,aggregators=snapshot(pods),gateways=snapshot(gateways),services=services,projection=view,metrics=metrics,wrong_credential_status=401)
try:
 code="import urllib.request,json;d=json.load(urllib.request.urlopen('http://prometheus:9090/api/v1/targets',timeout=5));print(json.dumps([dict(pod=t['labels'].get('pod'),health=t['health'],error=t['lastError']) for t in d['data']['activeTargets'] if t['labels'].get('app')=='aggregator-gateway']))"
 result['gateway_prometheus_targets']=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'exec','deployment/observability','--','python3','-c',code],timeout=15))
 result['gateway_observability_ready']=len(result['gateway_prometheus_targets'])==len(snapshot(gateways)) and all(t['health']=='up' for t in result['gateway_prometheus_targets'])
except Exception as err:
 result['gateway_observability_ready']=False
 result['gateway_observability_error']=str(err)
# Observability is reported separately from the HTTP/data contract.
if with_trace:
 try:
  # The private dashboard deliberately denies node-host ingress. Verify over
  # authenticated Kubernetes exec/loopback, without relaxing its NetworkPolicy.
  code="import urllib.request;print(urllib.request.urlopen("+repr('http://127.0.0.1:8090/api/trace?id='+rows[-1]['correlation_id'])+",timeout=20).read().decode())"
  trace=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'exec','deployment/observability','--','python3','-c',code],timeout=30))
  services_seen=sorted({step['service'] for step in trace['steps']})
  result['correlation_trace']={'correlation_id':rows[-1]['correlation_id'],'services_seen':services_seen,'scope':trace['scope'],'complete':trace['complete']}
  result['gateway_trace_ready']={'aggregator','aggregator-gateway'}.issubset(services_seen)
 except Exception as err:
  result['gateway_trace_ready']=False
  result['gateway_trace_error']=str(err)
print(json.dumps(result))
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trace',action='store_true',help='Also scan the bounded operations-dashboard log trace; do not run during a clean capacity measurement')
    args=parser.parse_args()
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    raw=h.ssh(h.nodes[0],'sudo -n python3 -c '+shlex.quote('with_trace='+repr(args.trace)+'\n'+REMOTE),capture=True)
    result=json.loads(raw)
    result['requested_checks_passed']=result['passed'] and (not args.trace or (result.get('gateway_trace_ready',False) and result['gateway_observability_ready']))
    path=ROOT/'.e2e'/('p2-capacity-contract-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.json')
    path.write_text(json.dumps(result,indent=2))
    print(json.dumps(dict(passed=result['passed'],projection=result['projection'],gateway_observability_ready=result['gateway_observability_ready'],
                         requests=[{k:r[k] for k in ('requested_encoding','actual_encoding','wire_bytes','decoded_bytes','partial','latency_ms')} for r in result['requests']])),flush=True)
    print('Evidence:',path.relative_to(ROOT),flush=True)
    if args.trace:print('Gateway correlation trace:',result.get('gateway_trace_ready',False),flush=True)
    raise SystemExit(0 if result['requested_checks_passed'] else 1)


if __name__=='__main__':main()
