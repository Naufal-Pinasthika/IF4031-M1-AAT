#!/usr/bin/env python3
"""Repeated M1 fan-out regression on the VPS; retains all consumer PVCs."""
import datetime
import json
from pathlib import Path
import random
import subprocess
import time
import urllib.request

NS = 'sistem-informasi-kebencanaan-bnpb'


def kube(*args):
    return subprocess.check_output(['k3s','kubectl','--request-timeout=10s','-n',NS,*args],text=True,timeout=20)


def stats(service,port):
    pods=json.loads(kube('get','pods','-l','app='+service,'-o','json'))['items']
    ready=next(p for p in pods if not p['metadata'].get('deletionTimestamp') and
               any(c.get('type')=='Ready' and c.get('status')=='True' for c in p['status'].get('conditions',[])))
    with urllib.request.urlopen(f"http://{ready['status']['podIP']}:{port}/stats",timeout=5) as response:
        return json.load(response)


def main():
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    rounds=[]
    for index in range(3):
        before=stats('dashboard-consumer',8085)
        notifier_before=stats('notifier-consumer',8086)
        duration=random.SystemRandom().uniform(12,18)
        result=dict(round=index+1,started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    dashboard_before=before,notifier_before=notifier_before,outage_seconds=duration,passed=False)
        scaled=False
        try:
            kube('scale','statefulset/dashboard-consumer','--replicas=0');scaled=True
            kube('wait','--for=delete','pod/dashboard-consumer-0','--timeout=60s')
            time.sleep(duration)
            middle=stats('notifier-consumer',8086)
            result['notifier_while_dashboard_down']=middle
        finally:
            if scaled:
                # Scheduled removal of injected fault, not an application repair.
                kube('scale','statefulset/dashboard-consumer','--replicas=1')
        released=time.monotonic()
        kube('rollout','status','statefulset/dashboard-consumer','--timeout=90s')
        delta=middle['processed_count']-notifier_before['processed_count']
        target=max(before['processed_count'],middle['processed_count'])
        while True:
            after=stats('dashboard-consumer',8085)
            if after['processed_count']>=target or time.monotonic()-released>60: break
            time.sleep(.5)
        result.update(dashboard_after=after,independent_notifier_records=delta,
                      catchup_seconds=round(time.monotonic()-released,3),
                      passed=delta>0 and after['processed_count']>=target)
        rounds.append(result)
        args.output.write_text(json.dumps(dict(passed=all(r['passed'] for r in rounds),rounds=rounds,
            scope='Consumer progress/count catch-up, not full per-message reference reconciliation or critical latency proof'),indent=2))
        print(json.dumps(result),flush=True)
    raise SystemExit(0 if all(r['passed'] for r in rounds) else 1)


if __name__=='__main__':main()
