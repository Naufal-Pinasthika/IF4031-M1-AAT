#!/usr/bin/env python3
"""Read historical per-pod metrics for an existing throughput run; no load/mutations."""
import argparse
import datetime as dt
import json
from pathlib import Path
import shlex

from provision import Hosts, ROOT, inventory


REMOTE = r'''
import json,subprocess,sys,urllib.parse,urllib.request
cfg=json.load(sys.stdin);ns='sistem-informasi-kebencanaan-bnpb'
def kube(*args):return json.loads(subprocess.check_output(['k3s','kubectl','--request-timeout=10s','-n',ns,*args]))
svc=kube('get','service/prometheus','-o','json')
base='http://'+svc['spec']['clusterIP']+':9090'
def query(expression,when):
 with urllib.request.urlopen(base+'/api/v1/query?'+urllib.parse.urlencode({'query':expression,'time':when}),timeout=10) as r:
  result=json.load(r)
 if result['status']!='success':raise RuntimeError('Prometheus query failed')
 return result['data']['result']
out={'scope':'Read-only retained Prometheus counters; 45-second window from case start+5s to start+50s. Increases are scrape-based estimates, not exact k6 counts. No new load or runtime mutation.','service':kube('get','service/aggregator-vps','-o','json')['spec'],'cases':[]}
for case in cfg['cases']:
 when=case['start']+50
 expression='sum by (pod,node,path,status) (increase(aggregator_http_requests_total{app="aggregator"}[45s]))'
 counts=query(expression,when)
 latency=query('sum by (pod,node) (increase(aggregator_http_request_duration_ms_sum{app="aggregator"}[45s])) / sum by (pod,node) (increase(aggregator_http_request_duration_ms_count{app="aggregator"}[45s]))',when)
 outer=query('1000 * sum by (pod,node) (increase(aat_http_request_duration_seconds_sum{app="aggregator"}[45s])) / sum by (pod,node) (increase(aat_http_request_duration_seconds_count{app="aggregator"}[45s]))',when)
 up=query('min_over_time(up{app="aggregator"}[45s])',when)
 gateway_statuses=query('sum by (pod,node,status) (increase(aat_http_requests_total{app="aggregator-gateway"}[45s]))',when)
 gateway_up=query('min_over_time(up{app="aggregator-gateway"}[45s])',when)
 gateway_errors=query('sum by (pod,node) (increase(gateway_upstream_errors_total[45s]))',when)
 gateway_rejected=query('sum by (pod,node) (increase(gateway_rejected_total[45s]))',when)
 gateway_health={name:query('sum by (pod,node) (increase('+name+'[45s]))',when) for name in ['gateway_discovery_errors_total','gateway_probe_failures_total','gateway_no_backend_total']}
 gateway_phases={name:query('1000 * sum by (pod,node,class) (increase('+name+'_sum[45s])) / sum by (pod,node,class) (increase('+name+'_count[45s]))',when) for name in ['gateway_queue_wait_seconds','gateway_upstream_headers_seconds','gateway_body_lifetime_seconds']}
 gateway_queues={name:query('sum by (pod,node,class) (increase('+name+'[45s]))',when) for name in ['gateway_queued_total','gateway_queue_full_total','gateway_queue_expired_total','gateway_queue_canceled_total']}
 gateway_memory={name:query('max_over_time('+name+'{app="aggregator-gateway"}[45s])',when) for name in ['gateway_connections_open','aat_go_runtime_memory_bytes','aat_go_memory_limit_bytes']}
 read_statistics={name:query('sum by (pod,node) (increase('+name+'{app="aggregator"}[45s]))',when) for name in ['aggregator_read_cache_hits_total','aggregator_read_cache_misses_total','aggregator_read_cache_joined_total','aggregator_read_fills_total','aggregator_read_fill_seconds_sum','aggregator_projection_hits_total','aggregator_projection_fills_total','aggregator_projection_fallback_total']}
 out['cases'].append(dict(case=case['case'],window_end=when,request_counter_increases=counts,mean_handler_ms_all_routes=latency,mean_outer_business_ms=outer,scrape_up_min=up,gateway_statuses=gateway_statuses,gateway_scrape_up_min=gateway_up,gateway_upstream_errors=gateway_errors,gateway_rejected=gateway_rejected,gateway_health=gateway_health,gateway_phase_mean_ms=gateway_phases,gateway_queues=gateway_queues,gateway_memory=gateway_memory,read_statistics=read_statistics))
print(json.dumps(out))
'''


def traffic_summary(case):
    rows={}
    for item in case['request_counter_increases']:
        labels=item['metric']
        if labels.get('path')!='GET /internal/events':
            continue
        row=rows.setdefault(labels['node'],{'all':0.0,'200':0.0,'429':0.0})
        count=float(item['value'][1])
        row['all']+=count
        row[labels['status']]=row.get(labels['status'],0.0)+count
    total=sum(row['all'] for row in rows.values())
    for row in rows.values():
        row['share_percent']=100*row['all']/total if total else None
    return rows


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path)
    parser.add_argument('--incomplete',action='store_true',help='Diagnose retained live snapshots after an aborted campaign; not a completed capacity comparison')
    args=parser.parse_args()
    args.run=args.run.resolve()
    cases=[]
    paths=list(args.run.glob('replicas-*.json'))
    if args.incomplete:
        paths=[p for p in paths if not p.name.endswith('-live.json')]+[p for p in paths if p.name.endswith('-live.json') and not p.with_name(p.name.replace('-live.json','.json')).exists()]
    for path in sorted(paths):
        if path.name.endswith('-live.json'):
            if not args.incomplete:continue
        value=json.loads(path.read_text())
        first=value['snapshots'][0]
        start=dt.datetime.fromisoformat(first['observed_at']).timestamp()-first['elapsed']
        cases.append(dict(case=value.get('case',path.stem.removesuffix('-live')),start=start))
    if not cases or (len(cases)!=4 and not args.incomplete):
        raise ValueError('Expected four completed throughput cases')
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    cases.sort(key=lambda c:c['start'])
    raw=h.ssh(h.nodes[0],'sudo -n python3 -c '+shlex.quote(REMOTE),json.dumps({'cases':cases}).encode(),capture=True)
    result=json.loads(raw)
    result['incomplete_campaign_diagnostic']=args.incomplete
    output=args.run/'historical-routing.json'
    output.write_text(json.dumps(result,indent=2))
    for case in result['cases']:
        print(json.dumps({'case':case['case'],'traffic':traffic_summary(case),
            'inner_mean_ms':case['mean_handler_ms_all_routes'],'outer_mean_ms':case['mean_outer_business_ms'],'gateway_statuses':case['gateway_statuses'],'gateway_upstream_errors':case['gateway_upstream_errors'],'gateway_rejected':case['gateway_rejected']}),flush=True)
    print('Evidence:',output.relative_to(ROOT),flush=True)


if __name__=='__main__':
    main()
