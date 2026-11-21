#!/usr/bin/env python3
"""Observe the unmodified Sensor Pantai and reconcile valid critical IDs.

For newly visible alerts, use the previous successful poll's wall-clock start
as a conservative availability bound. Never subtract simulated observed_at
from host time. Historical startup records are counted, not used for this SLO.
"""
import datetime
import hashlib
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
from pathlib import Path

import smoke

duration = int(os.environ.get('E2E_WINDOW_SECONDS', '90'))
expected = set()
new_alert_bounds = {}
errors = []
invalid_ids = set()
result = {'passed':False}


def timestamp(value):
    # Go emits RFC3339Nano; Python 3.10 accepts microseconds, not nine digits.
    return datetime.datetime.fromisoformat(re.sub(r'(\.\d{6})\d+(?=Z|[+-])', r'\1', value).replace('Z','+00:00'))


def valid(item, stations):
    required = ['observation_id','station_id','observed_at','sea_level_anomaly_m','wave_height_m','quality','critical']
    if not isinstance(item, dict) or any(item.get(k) is None for k in required):
        return False
    if not isinstance(item['observation_id'],str) or not item['observation_id'].strip() or item['station_id'] not in stations:
        return False
    if not isinstance(item['critical'],bool) or not isinstance(item['quality'],str) or not item['quality'].strip():
        return False
    for k in ['sea_level_anomaly_m','wave_height_m']:
        if type(item[k]) not in [float,int] or not math.isfinite(item[k]):
            return False
    if item['wave_height_m'] < 0:
        return False
    try:
        timestamp(item['observed_at'])
    except (ValueError, TypeError):
        return False
    return True


def hazard(ref):
    identity = json.dumps(['SENSOR_PANTAI',ref],separators=(',',':'),ensure_ascii=False)
    return 'hz-'+hashlib.sha256(identity.encode()).hexdigest()


try:
    headers = {'Authorization':'Bearer '+smoke.VALUES['PANTAI_AUTH_TOKEN']}
    ready_deadline = time.monotonic()+600
    while True:
        try:
            status, stations, _ = smoke.request(30090,'/v1/stations',headers)
            if status == 200:
                break
        except (OSError, urllib.error.URLError):
            pass
        assert time.monotonic()<ready_deadline, 'Source did not become ready'
        time.sleep(0.2)
    stations = {x['station_id'] for x in stations['items']}
    cursor = ''
    caught_up = False
    previous_poll = None
    start = time.monotonic()
    last_report = start
    while time.monotonic()-start < duration:
        poll_started = time.time()
        path = '/v1/observations?limit=1000'+('&cursor='+urllib.parse.quote(cursor) if cursor else '')
        try:
            status, page, _ = smoke.request(30090,path,headers)
            if status != 200 or not isinstance(page,dict) or not isinstance(page.get('items'),list):
                errors.append({'status':status,'at':poll_started})
                time.sleep(0.5)
                continue
        except (OSError, urllib.error.URLError):
            errors.append({'status':'network_error','at':poll_started})
            time.sleep(0.5)
            continue
        for item in page['items']:
            if not valid(item,stations):
                if isinstance(item,dict) and isinstance(item.get('observation_id'),str):
                    invalid_ids.add(item['observation_id'])
                continue
            if item['critical']:
                ref = item['observation_id']
                if ref not in expected and caught_up and previous_poll is not None:
                    new_alert_bounds[ref] = previous_poll
                expected.add(ref)
        if page.get('next_cursor'):
            cursor = page['next_cursor']
        if not page['items'] or not page.get('next_cursor'):
            caught_up = True
        previous_poll = poll_started
        if time.monotonic()-last_report >= 15:
            print(f'Observed {len(expected)} valid critical IDs; {len(new_alert_bounds)} newly visible; source errors={len(errors)}',flush=True)
            last_report = time.monotonic()
        time.sleep(0.5 if caught_up else 0.01)
    assert expected, 'No valid critical source record observed'
    deadline = time.monotonic()+30
    while True:
        delivered = {x['hazard_id']:x for x in smoke.notifications_all() if x['source']=='SENSOR_PANTAI' and x.get('critical')}
        missing = {ref for ref in expected if hazard(ref) not in delivered}
        if not missing or time.monotonic()>deadline:
            break
        time.sleep(1)
    latencies = []
    for ref, bound in new_alert_bounds.items():
        if hazard(ref) in delivered:
            received = timestamp(delivered[hazard(ref)]['created_at']).timestamp()
            latencies.append(max(0,round((received-bound)*1000,2)))
    latencies.sort()
    p95 = latencies[max(0,math.ceil(len(latencies)*0.95)-1)] if latencies else None
    result.update({'source_critical_observed':len(expected),'critical_missing':len(missing),'missing_sample':sorted(missing)[:20],
                   'new_alerts':len(new_alert_bounds),'latency_upper_bound_p95_ms':p95,'source_errors':len(errors),
                   'invalid_observed_ids':len(invalid_ids),'window_seconds':duration,
                   'latency_scope':'Newly visible source alerts only; conservative wall-clock polling bound, not simulated observed_at.'})
    assert not missing, f'{len(missing)} observed critical IDs missing at notifier'
    assert p95 is not None, 'No newly visible critical alert in this window; latency remains untested'
    assert p95 <= 5000, f'Critical notification p95 bound {p95} ms exceeds 5 seconds'
    result['passed'] = True
    print(json.dumps(result,indent=2),flush=True)
finally:
    result.setdefault('source_critical_observed',len(expected))
    result.setdefault('new_alerts',len(new_alert_bounds))
    result['new_alert_availability_bounds'] = new_alert_bounds
    result['ran_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (smoke.ROOT/'.e2e/coastal-window-results.json').write_text(json.dumps(result,indent=2))
