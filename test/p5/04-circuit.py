from pathlib import Path
import datetime as dt
import json
import math
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import parser,run
from coastal import p95


def scenario(h):
    h.check(h.args.seconds>=10,'Circuit observation window is at least ten seconds')
    config=json.loads(h.kube('get','configmap','aggregator-config','-o','json'))['data']
    interval=int(config['COASTAL_POLL_INTERVAL_MS'])/1000
    cooldown=int(config['COASTAL_CIRCUIT_OPEN_MS'])/1000
    threshold=int(config['COASTAL_CIRCUIT_FAILURES'])
    failures=[]
    for ordinal in range(h.args.repeat):
        started=dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00','Z')
        deadline=time.monotonic()+h.args.seconds
        samples=[]
        while time.monotonic()<deadline:
            code,page,elapsed=h.api('/events?limit=10&source=SENSOR_PANTAI','FIELD')
            valid=code==200 and page.get('partial') is False and bool(page.get('events'))
            if code==503 and elapsed<.3:
                valid=True
            samples.append({'code':code,'seconds':elapsed,'valid':valid,'sources':page.get('sources')})
            time.sleep(.5)
        pods=json.loads(h.kube('get','pods','-l','app=aggregator','-o','json'))['items']
        requests=[]
        for pod in pods:
            raw=h.kube('logs',pod['metadata']['name'],'--since-time='+started)
            for line in raw.splitlines():
                try: row=json.loads(line)
                except ValueError: continue
                if row.get('msg') in ('coastal_request_failed','coastal_request_completed'):
                    requests.append({'pod':pod['metadata']['name'],**row})
        # One lease owner polls. Allow two contract requests per attempt
        # (station discovery + observations), plus startup failures on each pod.
        bound=2*(math.ceil(h.args.seconds/(cooldown+interval))+len(pods)*threshold)
        result={'round':ordinal+1,'observed_upstream_requests':len(requests),'bound':bound,
                'client_samples':len(samples),'p95_client_seconds':p95([s['seconds'] for s in samples]),
                'source_pods':{uid:p for uid,p in h.pods().items() if p['app']=='sensor-pantai'},'requests':requests,'client':samples}
        h.artifact(f'round-{ordinal+1}.json',result)
        h.note({k:v for k,v in result.items() if k not in ('requests','client')})
        if not requests or len(requests)>bound or not all(s['valid'] for s in samples) or result['p95_client_seconds']>=.3:
            failures.append(ordinal+1)
    h.check(not failures,'Unhealthy source calls stay bounded and Field reads return last data/unavailability quickly')


if __name__=='__main__':
    p=parser('Observe bounded upstream attempts and fast client responses during the genuine Sensor Pantai outage')
    p.add_argument('--seconds',type=int,default=30)
    raise SystemExit(run('p5/04-circuit',['P5.4 (outage phase)'],p,scenario))
