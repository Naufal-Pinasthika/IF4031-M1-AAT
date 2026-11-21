from pathlib import Path
import datetime as dt
import json
import math
import sys
import time
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import parser, run, Failure, Inconclusive
from coastal import forward, hazard, p95


def valid(item, stations):
    try:
        required = ['observation_id','station_id','observed_at','sea_level_anomaly_m','wave_height_m','quality','critical']
        if not isinstance(item,dict) or any(item.get(k) is None for k in required):
            return False
        if not isinstance(item['observation_id'],str) or not item['observation_id'].strip() or item['station_id'] not in stations:
            return False
        if type(item['critical']) is not bool or not isinstance(item['quality'],str) or not item['quality'].strip():
            return False
        dt.datetime.fromisoformat(item['observed_at'].replace('Z','+00:00'))
        return all(type(item[k]) in (int,float) and math.isfinite(item[k]) for k in ('sea_level_anomaly_m','wave_height_m')) and item['wave_height_m']>=0
    except (ValueError,TypeError,KeyError,AttributeError):
        return False


def scenario(h):
    h.check(h.args.seconds>=1,'Observation duration is positive')
    forward(h)
    header = {'Authorization':'Bearer '+h.values['PANTAI_AUTH_TOKEN']}
    results = []
    for ordinal in range(h.args.repeat):
        cursor = h.sql("SELECT to_json(opaque_cursor) FROM source_cursors WHERE source='SENSOR_PANTAI'") or ''
        state = {'round':ordinal+1,'critical':{},'errors':[],'source_pages':0,'invalid':[]}
        stations, tail = {}, None
        deadline = time.monotonic()+h.args.seconds
        while time.monotonic()<deadline:
            try:
                if not stations:
                    code,page,_=h.request('sensor','/v1/stations',header)
                    if code!=200:
                        state['errors'].append({'stations_http':code})
                        time.sleep(1)
                        continue
                    stations={s['station_id']:s for s in page['items']}
                began=time.monotonic()
                code,page,_=h.request('sensor','/v1/observations?'+urllib.parse.urlencode({'limit':1000,**({'cursor':cursor} if cursor else {})}),header)
                if code!=200 or not isinstance(page.get('items'),list):
                    state['errors'].append({'observations_http':code})
                    time.sleep(1)
                    continue
                state['source_pages']+=1
                for item in page['items']:
                    if not valid(item,stations):
                        state['invalid'].append(item)
                        continue
                    if item['critical'] and item['observation_id'] not in state['critical']:
                        state['critical'][item['observation_id']]={'hazard_id':hazard(item['observation_id']),'availability_lower_bound':tail}
                if page.get('next_cursor'):
                    cursor=page['next_cursor']
                if not page['items']:
                    tail=began
                for ref, row in state['critical'].items():
                    if row.get('confirmed'):
                        continue
                    code,receipt,_=h.request('notifier','/notifications/confirm',body={'hazard_id':row['hazard_id']},method='POST')
                    if code==200 and receipt.get('durability_confirmed') and receipt['notification']['critical']:
                        row['confirmed']=True
                        row['upper_bound_ms']=(time.monotonic()-row['availability_lower_bound'])*1000 if row['availability_lower_bound'] is not None else None
                    elif code!=404:
                        state['errors'].append({'confirmation_http':code,'ref':ref})
            except Failure as error:
                state['errors'].append({'transport':str(error)})
            time.sleep(.25)
        values=[v['upper_bound_ms'] for v in state['critical'].values() if v.get('upper_bound_ms') is not None]
        missing=[ref for ref,v in state['critical'].items() if not v.get('confirmed')]
        state.update(new_latency_samples=len(values),missing=missing,p95_upper_bound_ms=p95(values),tail_established=tail is not None)
        h.artifact(f'round-{ordinal+1}.json',state)
        h.note({k:v for k,v in state.items() if k not in ('critical','invalid','errors')})
        results.append(state)
    h.check(not any(r['missing'] for r in results),'Every valid observed critical ID is durably confirmed by notifier')
    if any(not r['new_latency_samples'] or r['errors'] or not r['tail_established'] for r in results):
        raise Inconclusive('Official sensor coverage/new critical samples are insufficient; do not claim P5.1 latency or zero loss beyond observed IDs')
    h.check(all(r['p95_upper_bound_ms']<=5000 for r in results),'Conservative same-observer source-availability to durability p95 is at most 5 seconds in every round')


if __name__=='__main__':
    p=parser('Observe the genuine Sensor Pantai and reconcile critical delivery without simulated-clock subtraction')
    p.add_argument('--seconds',type=int,default=60)
    raise SystemExit(run('p5/01-critical',['P5.1'],p,scenario))
