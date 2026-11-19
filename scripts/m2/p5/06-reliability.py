#!/usr/bin/env python3
"""Repeated black-box P5 observations against the genuine sensor on the VPS.

Run on VPS A as the lab operator. No source image/profile/data is replaced.
Faults have declared automatic release times; no app repair during recovery.
Evidence is bounded observed-reference evidence, not the encrypted exam oracle.
"""
import argparse
import base64
from collections import Counter
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import random
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

NS = 'sistem-informasi-kebencanaan-bnpb'
BASE = 'http://10.77.0.1'


def kube(*args):
    return subprocess.check_output(['k3s', 'kubectl', '--request-timeout=15s', '-n', NS, *args], text=True, timeout=25)


def secret(name, key):
    return base64.b64decode(json.loads(kube('get', 'secret', name, '-o', 'json'))['data'][key]).decode()


def request(port, path, token=None, method='GET', payload=None):
    headers = {'X-Correlation-ID': 'p5-observer-'+uuid.uuid4().hex}
    if token:
        headers['Authorization'] = 'Bearer '+token
    started = time.monotonic()
    try:
        if payload is not None:headers['Content-Type']='application/json'
        response = urllib.request.urlopen(urllib.request.Request(BASE+':'+str(port)+path, headers=headers, method=method,
            data=json.dumps(payload).encode() if payload is not None else None), timeout=4)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        data = response.read(8 << 20)
        status = response.status
    return status, json.loads(data), round((time.monotonic()-started)*1000, 3)


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def hazard(ref):
    return 'hz-'+hashlib.sha256(json.dumps(['SENSOR_PANTAI', ref], separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def notifications():
    result, cursor, seen = {}, '', set()
    for _ in range(10000):
        status, page, _ = request(30086, '/notifications?limit=1000'+('&cursor='+urllib.parse.quote(cursor, safe='') if cursor else ''))
        if status != 200:
            raise RuntimeError('notifier HTTP '+str(status))
        for item in page['notifications']:
            if item['source'] == 'SENSOR_PANTAI' and item.get('critical'):
                result[item['hazard_id']] = item
        cursor = page.get('next_cursor')
        if not cursor:
            return result
        if cursor in seen:
            raise RuntimeError('notification pagination stalled')
        seen.add(cursor)
    raise RuntimeError('notification pagination bound exceeded')


def valid(item, stations):
    required = ('observation_id', 'station_id', 'observed_at', 'sea_level_anomaly_m', 'wave_height_m', 'quality', 'critical')
    if not isinstance(item, dict) or any(item.get(k) is None for k in required):
        return False
    if not isinstance(item['observation_id'], str) or not item['observation_id'].strip() or not isinstance(item['station_id'], str) or item['station_id'] not in stations:
        return False
    if type(item['critical']) is not bool or not isinstance(item['quality'], str) or not item['quality'].strip():
        return False
    for key in ('sea_level_anomaly_m', 'wave_height_m'):
        if type(item[key]) not in (float, int) or not math.isfinite(item[key]):
            return False
    try:
        timestamp(item['observed_at'])
    except (ValueError, TypeError, AttributeError):
        return False
    return item['wave_height_m'] >= 0


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values)*fraction)-1)] if values else None


def run_round(args, index, mode, token):
    started_wall = time.time()
    state = dict(round=index, mode=mode, started_at=dt.datetime.now(dt.timezone.utc).isoformat(), passed=False,
                 scope='Observed source IDs and conservative availability bounds; not official exam reference',
                 faults=[], errors=[], source_statuses=Counter(), invalid_critical_ids=[], invalid_noncritical=0)
    initial = notifications()
    status, page, _ = request(30090, '/v1/stations', token)
    if status != 200:
        raise RuntimeError('stations unavailable at round start')
    stations = {x['station_id'] for x in page['items']}
    expected, bounds, invalid_critical = set(), {}, set()
    cursor, last_tail, caught_up = '', None, False
    confirmed, samples = {}, {}
    fault_errors, fault_done = [], threading.Event()
    rng = random.SystemRandom()
    fault_delay, fault_duration = rng.uniform(12, 18), rng.uniform(8, 13)

    def fault():
        if mode == 'baseline':
            fault_done.set()
            return
        scaled = False
        try:
            time.sleep(fault_delay)
            if mode == 'consumer-outage':
                kube('scale', 'statefulset/dashboard-consumer', '--replicas=0')
                scaled = True
                state['faults'].append(dict(action='dashboard consumer unavailable', at=time.time()))
            time.sleep(fault_duration)
        except Exception as exc:
            fault_errors.append(type(exc).__name__+': '+str(exc))
        finally:
            if scaled:
                try:
                    kube('scale', 'statefulset/dashboard-consumer', '--replicas=1')
                except Exception as exc:
                    fault_errors.append('consumer release failed: '+str(exc))
            state['faults'].append(dict(action='scheduled fault release; no app repair', at=time.time()))
            fault_done.set()

    worker = threading.Thread(target=fault)
    worker.start()
    deadline = time.monotonic()+args.seconds
    try:
        while time.monotonic() < deadline:
            poll_wall = time.monotonic()
            try:
                status, page, _ = request(30090, '/v1/observations?limit=1000'+('&cursor='+urllib.parse.quote(cursor, safe='') if cursor else ''), token)
                state['source_statuses'][status] += 1
                if status != 200 or not isinstance(page.get('items'), list):
                    time.sleep(0.25)
                    continue
                for item in page['items']:
                    if not valid(item, stations):
                        if isinstance(item, dict) and item.get('critical') is True:
                            invalid_critical.add(str(item.get('observation_id', '<missing ID>')))
                        else:
                            state['invalid_noncritical'] += 1
                        continue
                    if item['critical']:
                        ref = item['observation_id']
                        if ref not in expected and last_tail is not None and hazard(ref) not in initial:
                            bounds[ref] = last_tail
                        expected.add(ref)
                for ref in bounds.keys()-confirmed.keys():
                    code,receipt,_=request(30086,'/notifications/confirm',method='POST',payload={'hazard_id':hazard(ref)})
                    if code==200 and receipt.get('durability_confirmed'):
                        confirmed[ref]=receipt['notification']
                        samples[ref]=(time.monotonic()-bounds[ref])*1000
                    elif code!=404:
                        state['errors'].append('durability confirmation HTTP '+str(code))
                if page.get('next_cursor'):
                    cursor = page['next_cursor']
                # A short nonempty page is not contractually proof of tail.
                # Only an empty unfiltered cursor page establishes that bound.
                if len(page['items']) == 0:
                    caught_up = True
                    last_tail = poll_wall
                time.sleep(0.2 if caught_up else 0.01)
            except (OSError, ValueError, KeyError) as exc:
                state['errors'].append(type(exc).__name__+': '+str(exc))
                time.sleep(0.25)
    finally:
        worker.join(timeout=90)
        state['fault_errors'] = fault_errors
    delivered = {}
    drain = time.monotonic()+args.drain
    while True:
        delivered = notifications()
        missing = {ref for ref in expected if hazard(ref) not in delivered}
        if not missing or time.monotonic() >= drain:
            break
        time.sleep(1)
    latency, unverifiable = [], []
    for ref, bound in bounds.items():
        if ref in samples:
            value = samples[ref]
            if value < 0:
                # Never turn negative cross-clock/inconsistent measurements
                # into zero-latency successes.
                unverifiable.append(ref)
            else:
                latency.append(value)
        else:
            unverifiable.append(ref)
    p95 = percentile(latency, .95)
    late = sum(x > 5000 for x in latency)
    state.update(source_valid_critical=len(expected), missing_critical=len(missing), missing_ids=sorted(missing),
                 invalid_critical_ids=sorted(invalid_critical), newly_visible_critical=len(bounds),
                 measured_new_critical=len(latency), unverified_time_ids=unverifiable,
                 latency_bound_p50_ms=percentile(latency,.5), latency_bound_p95_ms=p95, latency_bound_p99_ms=percentile(latency,.99),
                 late_critical=late, allowed_late_budget=len(latency)-math.ceil(.95*len(latency)),
                 elapsed_seconds=round(time.time()-started_wall,2), source_caught_up=caught_up,
                 evidence_notes='Same-observer monotonic upper bound: last empty unfiltered sensor request start to notifier synchronous durability-confirmation response. Historical IDs excluded; availability is interval-censored, not source observed_at. Invalid critical records reported separately.',
                 latency_samples_ms=samples, latency_evidence='conservative_upper_bound',
                 latency_status='no_new_samples' if not bounds else ('measured' if not unverifiable else 'incomplete'))
    state['reference_coverage_errors'] = len(state['errors']) + sum(count for status,count in state['source_statuses'].items() if status != 200)
    state['passed'] = bool(expected) and caught_up and not missing and p95 is not None and p95 <= 5000 and not unverifiable and not invalid_critical and not fault_errors and not state['reference_coverage_errors'] and fault_done.is_set()
    return state


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--seconds', type=int, default=90)
    p.add_argument('--drain', type=int, default=45)
    p.add_argument('--modes', nargs='+', choices=['baseline','consumer-outage'], default=['baseline'])
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if not 1 <= args.rounds <= 20 or not 60 <= args.seconds <= 600:
        p.error('rounds must be 1..20 and seconds 60..600')
    token = secret('pantai-token', 'token')
    results = []
    for index in range(args.rounds):
        mode = args.modes[index % len(args.modes)]
        result = run_round(args,index+1,mode,token)
        results.append(result)
        args.output.write_text(json.dumps(dict(rounds=results,passed=all(x['passed'] for x in results)),indent=2))
        print(json.dumps(result),flush=True)
    raise SystemExit(0 if all(x['passed'] for x in results) else 1)


if __name__ == '__main__':
    main()
