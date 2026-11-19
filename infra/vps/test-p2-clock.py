#!/usr/bin/env python3
"""Reversible WSL guest-NTP experiment; never sets the host or guest clock.

Run as WSL root. A transient timer restores originally active NTP even if this
controller is interrupted. A clean 45-second probe is required before any load.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import threading
import uuid

from provision import ROOT, inventory


def clock_is_stable(result):
    return (result.get('samples',0)>0 and
            result.get('backward_wall_offset_changes')==0 and
            result.get('monotonic_backwards')==0)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark',action='store_true')
    parser.add_argument('--workload',choices=['standard','gzip'],default='standard')
    args=parser.parse_args()
    if os.geteuid()!=0:
        parser.error('Run as root in WSL')
    probe=runpy.run_path(str(ROOT/'infra/vps/probe-clock.py'))
    def systemctl(*items,check=True):
        return subprocess.run(['systemctl',*items],check=check,capture_output=True,text=True)
    active=systemctl('is-active','systemd-timesyncd',check=False).stdout.strip()
    # Only clock/network setup needs root. Reuse the operator's verified SSH
    # host-key database and normal environment for the actual benchmark.
    operator=Path(inventory(ROOT/'infra/vps/inventory.json')['key']).owner()
    if operator=='root':
        raise RuntimeError('Expected a non-root operator-owned SSH credential')
    enabled=systemctl('is-enabled','systemd-timesyncd',check=False).stdout.strip()
    if active not in ('active','inactive'):
        raise RuntimeError('Unexpected original NTP state: '+active)
    unit='aat-clock-restore-'+uuid.uuid4().hex[:12]
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    path=ROOT/'.e2e'/('p2-ntp-experiment-'+stamp+'.json')
    path.parent.mkdir(exist_ok=True)
    state=dict(original_active=active,original_enabled=enabled,benchmark_requested=args.benchmark,workload=args.workload)
    network_owned=False
    timer_owned=False
    stop=threading.Event()
    thread=None
    def write():
        path.write_text(json.dumps(state,indent=2))
    try:
        if active=='active':
            subprocess.run(['systemd-run','--unit='+unit,'--on-active=15m',
                            '/bin/systemctl','start','systemd-timesyncd'],check=True)
            timer_owned=True
            systemctl('stop','systemd-timesyncd')
        print('Guest NTP paused; probing clock for 45 seconds.',flush=True)
        state['preflight']=probe['observe'](45)
        write()
        if not clock_is_stable(state['preflight']):
            state['benchmark_skipped']='Clock still unstable with guest NTP inactive'
            print(state['benchmark_skipped'],flush=True)
            return 2
        if not args.benchmark:
            return 0
        subprocess.run([sys.executable,str(ROOT/'infra/vps/load-network.py'),'up'],check=True)
        network_owned=True
        def monitor():
            state['during_benchmark']=probe['observe'](600,stop)
        thread=threading.Thread(target=monitor)
        thread.start()
        previous=set((ROOT/'.e2e').glob('p2-laptop*-throughput-*/result.json'))
        result=subprocess.run(['runuser','-u',operator,'--',sys.executable,
                               str(ROOT/'infra/vps/test-p2-laptop.py'),'--workload',args.workload])
        state['benchmark_exit_code']=result.returncode
        state['clock_monitor_covered_benchmark_end']=thread.is_alive()
        stop.set()
        thread.join()
        thread=None
        state['clock_valid']=(state['clock_monitor_covered_benchmark_end'] and
                              clock_is_stable(state.get('during_benchmark',{})))
        state['benchmark_results']=[str(p.relative_to(ROOT)) for p in sorted(
            set((ROOT/'.e2e').glob('p2-laptop*-throughput-*/result.json'))-previous)]
        state['passed']=result.returncode==0 and state['clock_valid']
        return result.returncode if result.returncode else (0 if state['clock_valid'] else 2)
    finally:
        stop.set()
        if thread:
            thread.join()
        # Each cleanup is attempted even if the preceding cleanup fails.
        try:
            if network_owned:
                subprocess.run([sys.executable,str(ROOT/'infra/vps/load-network.py'),'down'],check=True)
        finally:
            try:
                if active=='active':
                    systemctl('start','systemd-timesyncd')
                state['restored_active']=systemctl('is-active','systemd-timesyncd',check=False).stdout.strip()
                state['restored_enabled']=systemctl('is-enabled','systemd-timesyncd',check=False).stdout.strip()
                if state['restored_active']!=active or state['restored_enabled']!=enabled:
                    raise RuntimeError('NTP state restoration needs inspection')
                if timer_owned:
                    systemctl('stop',unit+'.timer')
                state['ntp_restored']=True
            finally:
                write()
                print('Experiment evidence:',path.relative_to(ROOT),flush=True)


if __name__=='__main__':
    raise SystemExit(main())
