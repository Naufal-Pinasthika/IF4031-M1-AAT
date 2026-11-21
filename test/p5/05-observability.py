from pathlib import Path
import math
import os
import signal
import subprocess
import sys
import time
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import parser,run


def verify(h, load=None):
    h.open_dashboard()
    code,_,_=h.request('dashboard','/')
    h.check(code==200,'Private operations dashboard HTML is accessible')
    html=(h.folder/h.report['requests'][-1]['response_file']).read_text()
    h.check(all(marker in html for marker in ('notifier_coastal_critical_total','aggregator_coastal_circuit_state','/api/trace')),
            'Dashboard contains critical delivery, circuit and correlation-trace panels')
    failures=[]
    for ordinal in range(h.args.repeat):
        if load:
            h.check(load.poll() is None, 'Priority load is still active during trace observation')
        for query in ('up{app="notifier-consumer"}','aggregator_coastal_circuit_state == 1','aat_go_heap_bytes{app="notifier-consumer"}'):
            code,page,_=h.request('dashboard','/api/query?'+urllib.parse.urlencode({'query':query}))
            h.check(code==200 and page.get('status')=='success' and bool(page['data']['result']),
                    'Displayed P5 measurements have live scraped series: '+query)
            h.check(all(math.isfinite(float(row['value'][1])) for row in page['data']['result']),'Panel measurements are finite')
            if query.startswith('up'):
                h.check(all(float(row['value'][1])==1 for row in page['data']['result']),'Notifier Prometheus target is Up')
        code,_,_=h.api('/events?limit=1&source=BMKG','FIELD')
        h.check(code==200,'Trace reference request succeeds')
        correlation=h.report['requests'][-1]['correlation_id']
        code,trace,_=h.request('dashboard','/api/trace?'+urllib.parse.urlencode({'id':correlation}))
        if load:
            h.check(load.poll() is None, 'Priority load remains active through the completed trace lookup')
        names={row['service'] for row in trace.get('steps',[])}
        if code!=200 or not {'client-api','aggregator-gateway','aggregator'} <= names:
            failures.append(ordinal+1)
        h.note({'round':ordinal+1,'correlation_id':correlation,'services':sorted(names),'coverage':trace.get('scope'),
                'both_service_logs_found':ordinal+1 not in failures})
        if ordinal+1<h.args.repeat:
            time.sleep(6)
    h.check(not failures,'Same correlation ID is retained across Client API and Aggregator in every observation')


def scenario(h):
    if not h.args.under_load:
        return verify(h)
    output=h.folder/'priority-load'
    command=[sys.executable,str(Path(__file__).with_name('02-load.py')),'--vps','--mode','priority',
             '--repeat','1','--seconds','120','--env-file',str(h.args.env_file),'--output',str(output)]
    with (h.folder/'priority-load.log').open('w') as log:
        load=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            deadline=time.monotonic()+45
            while not (output/'round-1.log').exists():
                h.check(load.poll() is None and time.monotonic()<deadline,'Laptop k6 priority workload starts')
                time.sleep(.5)
            time.sleep(10)
            h.note({'load':'300 Media VUs and 10 Field requests/second from laptop',
                    'duration_seconds':120,'load_evidence':str(output)})
            try:
                verify(h,load)
            finally:
                status=load.wait(timeout=200)
                h.note({'priority_load_exit':status})
            h.check(status==0,'Concurrent priority workload meets its unchanged role/error thresholds')
        finally:
            if load.poll() is None:
                os.killpg(load.pid,signal.SIGINT)
                try:
                    load.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    os.killpg(load.pid,signal.SIGTERM)
                    load.wait(timeout=15)


if __name__=='__main__':
    p=parser('Verify real P5 dashboard data and request-correlation traces')
    p.add_argument('--under-load',action='store_true',help='Run a 120-second laptop priority workload during the trace checks')
    raise SystemExit(run('p5/05-observability',['P5.7'],p,scenario))
