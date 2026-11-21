from pathlib import Path
import concurrent.futures
import json
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import parser, run, Failure
from coastal import forward, p95


def scenario(h):
    h.check(h.args.seconds>=1,'Observation duration is positive')
    forward(h)
    before = h.pods()
    uid,initial = next((uid,p) for uid,p in before.items() if p['app']=='notifier-consumer')
    h.check(initial['ready'],'Notifier is Ready before the observation window starts')
    results, failures = [], []
    for round_number in range(h.args.repeat):
        rows = []
        deadline = time.monotonic()+h.args.seconds
        while time.monotonic()<deadline:
            def sample(path):
                try:
                    code, body, elapsed = h.request('notifier',path)
                    return {'path':path,'code':code,'seconds':elapsed,'body':body}
                except Failure as error:
                    return {'path':path,'error':str(error)}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                samples = list(pool.map(sample,['/health','/stats','/metrics']))
            rows.extend(samples)
            failures.extend(x for x in samples if x.get('code')!=200 or x.get('seconds',999)>2.5)
            time.sleep(1)
        current_uid,current = next((uid,p) for uid,p in h.pods().items() if p['app']=='notifier-consumer')
        result = {'round':round_number+1,'samples':len(rows),'p95_seconds':p95([x['seconds'] for x in rows if 'seconds' in x]),
                  'pod':current,'same_process':current_uid==uid and current['restarts']==initial['restarts']}
        if not current['ready'] or not result['same_process']:
            failures.append(result)
        h.artifact(f'round-{round_number+1}.json',{'summary':result,'requests':rows})
        h.note(result)
        results.append(result)
    h.artifact('failures.json',failures)
    h.check(not failures,'Concurrent health/statistics/scrapes remain ready and bounded without restarts')


if __name__=='__main__':
    p=parser('Measure live notifier health and statistics repeatedly under concurrent scrapes')
    p.add_argument('--seconds',type=int,default=60)
    raise SystemExit(run('p5/00-health',['P5.7'],p,scenario))
