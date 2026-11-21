import argparse
import datetime as dt
import json
import shlex
from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS

INDEX = 'p1_source_occurred'
DEFINITION = "ON hazard_events(partition_id,source,occurred_at,hazard_id) WHERE COALESCE(attributes->>'_retracted','false')<>'true'"


def main():
    p=argparse.ArgumentParser(description='Add a source-filtered pagination index, without changing rows, query budgets or PVCs')
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    if not args.apply:
        print(json.dumps({'action':'plan','index':INDEX,'definition':DEFINITION,'units':['postgres-'+c for c in 'abc'],'data_deleted':False}))
        return
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    folder=ROOT/'.e2e'/('source-index-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    folder.mkdir(parents=True)
    def kube(*args):
        return h.ssh(h.nodes[0],'sudo -n k3s kubectl -n '+NS+' '+shlex.join(args),capture=True).decode()
    def sql(pod,*statements):
        args=[]
        for statement in statements:
            args+=['-c',statement]
        return kube('exec',pod,'--','psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','aggregator','-qAt',*args)
    claims=lambda:{p['metadata']['name']:p['metadata']['uid'] for p in json.loads(kube('get','pvc','-o','json'))['items']}
    before=claims()
    results=[]
    for letter in 'abc':
        pod='postgres-'+letter+'-0'
        invalid=sql(pod,"SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='"+INDEX+"' AND i.indrelid='hazard_events'::regclass AND NOT i.indisvalid").strip()
        if invalid=='1':
            sql(pod,"SET lock_timeout='2s'","SET statement_timeout='120s'",'DROP INDEX CONCURRENTLY '+INDEX)
        sql(pod,"SET lock_timeout='2s'","SET statement_timeout='120s'",'CREATE INDEX CONCURRENTLY IF NOT EXISTS '+INDEX+' '+DEFINITION)
        sql(pod,"SET statement_timeout='30s'",'ANALYZE hazard_events')
        state=json.loads(sql(pod,"SELECT row_to_json(r) FROM (SELECT i.indisvalid,i.indisready,pg_get_indexdef(i.indexrelid) AS definition,pg_relation_size(i.indexrelid) AS bytes FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='"+INDEX+"' AND i.indrelid='hazard_events'::regclass) r"))
        if not state['indisvalid'] or not state['indisready'] or '(partition_id, source, occurred_at, hazard_id)' not in state['definition']:
            raise RuntimeError('Source pagination index is invalid or incompatible')
        results.append({'unit':pod,**state})
        print(json.dumps(results[-1]),flush=True)
    retained=before==claims()
    (folder/'result.json').write_text(json.dumps({'indexes':results,'pvcs_retained':retained,'canonical_rows_deleted':False},indent=2))
    if not retained:
        raise RuntimeError('PVC identities changed unexpectedly')
    print('Evidence:',folder.relative_to(ROOT),flush=True)


if __name__=='__main__':
    main()
