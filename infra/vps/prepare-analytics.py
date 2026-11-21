import argparse
import datetime as dt
import json
import shlex
from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE


def main():
    p = argparse.ArgumentParser(description='Prepare additive analytics covering indexes before an Aggregator rollout; no data or PVC deletion')
    p.add_argument('--apply', action='store_true', help='Build missing indexes concurrently, one storage unit at a time')
    p.add_argument('--rebuild', action='store_true', help='Rebuild only the two named analytics indexes concurrently; canonical rows are retained')
    args = p.parse_args()
    indexes = {
        'p1_analytics_live': "COALESCE(attributes->'_retracted','false'::jsonb)<>'true'::jsonb",
        'p1_analytics_retracted': "COALESCE(attributes->'_retracted','false'::jsonb)='true'::jsonb",
    }
    if not args.apply:
        print(json.dumps({'action': 'plan', 'units': ['postgres-'+c for c in 'abc'],
                          'indexes': indexes, 'data_deleted': False, 'rebuild': args.rebuild, 'apply_flag_required': True}, indent=2))
        return
    hosts = Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    server = hosts.nodes[0]
    folder = ROOT/'.e2e'/('analytics-indexes-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    folder.mkdir(parents=True)
    def kube(*args):
        return hosts.ssh(server,'sudo -n k3s kubectl -n '+NAMESPACE+' '+shlex.join(args),capture=True).decode()
    def sql(pod,*statements):
        commands=[]
        for statement in statements:
            commands+=['-c',statement]
        return kube('exec',pod,'--','psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','aggregator','-qAt',*commands)
    before=json.loads(kube('get','pvc','-o','json'))['items']
    (folder/'pvc-before.json').write_text(json.dumps(before,indent=2))
    result=[]
    for letter in 'abc':
        pod='postgres-'+letter+'-0'
        for name,predicate in indexes.items():
            invalid=sql(pod,"SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='hazard_events'::regclass AND c.relname='"+name+"' AND NOT i.indisvalid").strip()
            if invalid=='1' or args.rebuild:
                # A canceled concurrent build can leave an unusable index. Only
                # these two whitelisted, additive indexes may be rebuilt.
                sql(pod,"SET lock_timeout='2s'","SET statement_timeout='120s'",'DROP INDEX CONCURRENTLY IF EXISTS '+name)
                print(json.dumps({'unit':pod,'rebuilding_index':name,'canonical_rows_deleted':False}),flush=True)
            statement=('CREATE INDEX CONCURRENTLY IF NOT EXISTS '+name+' ON hazard_events(partition_id,hazard_id) '
                       'INCLUDE(ingestion_version,revision,hazard_type,severity,area_name,occurred_at) WHERE '+predicate)
            sql(pod,"SET lock_timeout='2s'","SET statement_timeout='120s'",statement)
            print(json.dumps({'unit':pod,'index_prepared':name}),flush=True)
        sql(pod,"SET statement_timeout='30s'",'ANALYZE hazard_events')
        records=json.loads(sql(pod,"SELECT json_agg(r) FROM (SELECT c.relname,i.indisvalid,i.indisready,pg_relation_size(c.oid) AS bytes,pg_get_expr(i.indpred,i.indrelid) AS predicate,pg_get_indexdef(i.indexrelid) AS definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='hazard_events'::regclass AND c.relname IN ('p1_analytics_live','p1_analytics_retracted')) r"))
        if len(records)!=2 or not all(r['indisvalid'] and r['indisready'] for r in records):
            raise RuntimeError('Analytics indexes are not valid/ready; deployment must not proceed')
        if any('->>' in r['predicate'] or '::jsonb' not in r['predicate'] for r in records):
            raise RuntimeError('Legacy text-based retraction indexes found; review and rerun with --apply --rebuild')
        result.append({'unit':pod,'indexes':records})
    after=json.loads(kube('get','pvc','-o','json'))['items']
    if {r['metadata']['name']:r['metadata']['uid'] for r in before}!={r['metadata']['name']:r['metadata']['uid'] for r in after}:
        raise RuntimeError('Unexpected PVC identity change during index preparation')
    (folder/'result.json').write_text(json.dumps({'units':result,'all_pvc_uids_retained':True,'data_deleted':False},indent=2))
    print('Prepared analytics indexes without changing images, credentials or memory limits.',flush=True)
    print('Evidence:',folder.relative_to(ROOT),flush=True)


if __name__=='__main__':
    main()
