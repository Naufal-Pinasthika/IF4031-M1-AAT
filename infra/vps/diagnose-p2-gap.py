#!/usr/bin/env python3
"""Read-only diagnosis of source identities missing from a P2 Audit snapshot."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
from provision import Hosts,ROOT,inventory


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('evidence',type=Path);args=p.parse_args()
    result=json.loads((args.evidence/'result.json').read_text())
    refs=next(x['missing_ids'] for x in result['identities'] if x['source']=='SENSOR_PANTAI')
    if not refs:raise RuntimeError('No missing sensor identities to diagnose')
    ids=['hz-'+hashlib.sha256(json.dumps(['SENSOR_PANTAI',r],separators=(',',':')).encode()).hexdigest() for r in refs]
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'));n=h.nodes[0];ns='sistem-informasi-kebencanaan-bnpb'
    def kube(*items):return h.ssh(n,'sudo -n k3s kubectl -n '+ns+' '+shlex.join(items),capture=True).decode()
    names=','.join("'"+x+"'" for x in ids)
    stores={}
    for pod in ('postgres-a-0','postgres-b-0','postgres-c-0'):
        query="SELECT json_build_object('metadata',(SELECT row_to_json(t) FROM (SELECT unit_name,status FROM p1_metadata WHERE singleton=true)t),'events',(SELECT COALESCE(json_agg(t),'[]') FROM (SELECT hazard_id,source_ref_id,partition_id,revision,ingestion_version,attributes->>'critical' AS critical FROM hazard_events WHERE hazard_id IN ("+names+"))t))"
        stores[pod]=json.loads(kube('exec',pod,'--','sh','-c','exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "$1"','sh',query))
    query="SELECT COALESCE(json_agg(t),'[]') FROM (SELECT hazard_id,version,holder_id,fencing_token,accepted_at,delivered_at,attempts,last_error FROM ingestion_outbox WHERE hazard_id IN ("+names+"))t"
    intents=json.loads(kube('exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',query))
    code=r'''
import base64,json,subprocess,urllib.request,urllib.error
ns='sistem-informasi-kebencanaan-bnpb'
key=base64.b64decode(json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get','secret','aggregator-credentials','-o','json']))['data']['AGGREGATOR_API_KEY']).decode()
pods=json.loads(subprocess.check_output(['k3s','kubectl','-n',ns,'get','pods','-l','app=aggregator','-o','json']))['items'];out=[]
for p in pods:
 if p['metadata'].get('deletionTimestamp'):continue
 for id in IDS:
  req=urllib.request.Request('http://'+p['status']['podIP']+':8083/internal/events/'+id,headers={'X-Aggregator-Key':key,'X-Correlation-ID':'p2-gap-diagnosis'})
  try:r=urllib.request.urlopen(req,timeout=5)
  except urllib.error.HTTPError as e:r=e
  with r:
   body=json.load(r)
   out.append({'pod':p['metadata']['name'],'hazard_id':id,'status':r.status,'event_present':body.get('event',{}).get('hazard_id')==id if body.get('event') else False,'partial':body.get('partial')})
print(json.dumps(out))
'''.replace('IDS',repr(ids))
    reads=json.loads(h.ssh(n,'sudo -n python3 -',code.encode(),capture=True))
    evidence=dict(missing_refs=refs,stores=stores,intents=intents,per_pod_reads=reads,scope='Read-only observation; no replay, repair, row edits, pod restart or configuration change')
    (args.evidence/'gap-diagnosis.json').write_text(json.dumps(evidence,indent=2));print(json.dumps(evidence,indent=2))


if __name__=='__main__':main()
