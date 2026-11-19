#!/usr/bin/env python3
"""Identical P2 throughput workload from guarded laptop WSL, never a cluster node."""
import argparse
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import urllib.request

from provision import Hosts, ROOT, inventory
from p2_log_capture import PodLogCapture


def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


harness=module('laptop_memory_harness',ROOT/'scripts/m2/e2e/run-direct-load.py')
load=module('p2_load_helpers',ROOT/'infra/vps/test-p2-load.py')
NS='sistem-informasi-kebencanaan-bnpb'


def compare(rates):
    if len(rates)!=4:
        raise ValueError('Expected ordered 1-3-3-1 cases')
    ratios=[rates[1]/rates[0] if rates[0]>0 else None,
            rates[2]/rates[3] if rates[3]>0 else None]
    return ratios,all(x is not None and x>=2 for x in ratios)


def review_run(directory):
    cases=[]
    for path in sorted(directory.glob('replicas-*.json')):
        if path.name.endswith('-live.json'):
            continue
        case=json.loads(path.read_text())
        cases.append(dict(case=case['case'],original_passed=case['passed'],
                          timing_integrity=load.timing_integrity(case['summary']['metrics'])))
    valid=len(cases)==4 and all(c['timing_integrity']['valid'] for c in cases)
    return dict(valid_for_timing_or_capacity=valid,cases=cases,
                scope='Integrity review only; raw run summaries/verdicts are preserved. Negative timing is not clamped or discarded.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://10.77.0.1:30083')
    parser.add_argument('--k6',default='/mnt/d/aat-m2-e2e/k6/k6-v2.4.0-linux-amd64/k6')
    parser.add_argument('--native-windows',action='store_true',help='Use native Windows k6 and an owned loopback SSH tunnel, avoiding WSL HTTP timing')
    parser.add_argument('--external-native-tunnel',action='store_true',help='Native Windows SSH tunnel is owned/cleaned by the calling PowerShell wrapper')
    parser.add_argument('--review-existing',type=Path,help='Read-only timing review; writes a separate local evidence file, runs no load')
    parser.add_argument('--workload',choices=['standard','gzip'],default='standard',help='gzip is a separately labelled protocol-negotiation experiment, not a silent replacement')
    args=parser.parse_args()
    if args.native_windows and args.workload!='standard':
        parser.error('The native Windows wrapper supports only the unchanged standard workload')
    if args.review_existing:
        result=review_run(args.review_existing)
        (args.review_existing/'integrity-review.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result),flush=True)
        raise SystemExit(0 if result['valid_for_timing_or_capacity'] else 1)
    if args.native_windows:
        args.k6='/mnt/d/aat-m2-e2e/k6/k6-v2.4.0-windows-amd64/k6.exe'
        args.base_url='http://127.0.0.1:43083'
    elif args.external_native_tunnel:
        parser.error('--external-native-tunnel requires --native-windows')
    args.vus=1000
    args.generator_procs=6
    args.min_free_mib=512
    args.max_rss_mib=1536
    (ROOT/'.vps').mkdir(exist_ok=True)
    lock=(ROOT/'.vps/p2-test.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    server=h.nodes[0]
    def kube(*items):
        result=subprocess.run(h.command(server,'sudo -n k3s kubectl --request-timeout=10s -n '+NS+' '+shlex.join(items)),capture_output=True,text=True,timeout=25)
        if result.returncode:
            raise RuntimeError('Kubernetes command failed: '+result.stderr[-1000:])
        return result.stdout.strip()
    harness.kube=kube
    def ready(count):
        kube('scale','deployment/aggregator','--replicas='+str(count))
        kube('rollout','status','deployment/aggregator','--timeout=180s')
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    kind='p2-native-laptop' if args.native_windows else 'p2-laptop'
    if args.workload=='gzip':kind+='-gzip'
    script='scripts/m2/p2/throughput'+('-gzip' if args.workload=='gzip' else '')+'.js'
    name=kind+'-throughput-'+stamp
    directory=ROOT/'.e2e'/name
    directory.mkdir(parents=True)
    control=ROOT/'.e2e'/(kind+'-control-'+stamp)
    control.mkdir()
    state=dict(passed=False,generator='Operator laptop native Windows' if args.native_windows else 'Operator laptop WSL; outside all application nodes',
               workload=args.workload,script=script,script_sha256=hashlib.sha256((ROOT/script).read_bytes()).hexdigest(),
               endpoint=args.base_url,vus=1000,duration_seconds=60,order=[1,3,3,1],cases=[],
               k6_version=subprocess.check_output([args.k6,'version'],text=True).strip(),
               k6_sha256=subprocess.check_output(['sha256sum',args.k6],text=True).split()[0],
               generator_gomaxprocs=6,memory_guard_mib=1536,
               memory_guard_kind='native private bytes' if args.native_windows else 'Linux RSS',
               transport=('Native Windows SSH loopback tunnel' if args.external_native_tunnel else 'WSL SSH/localhost relay') if args.native_windows else 'WireGuard')
    def save():
        (directory/'result.json').write_text(json.dumps(state,indent=2))
    capture=PodLogCapture(h,server,NS,directory)
    tunnel=None
    tunnel_log=None
    env=os.environ.copy()
    for line in (ROOT/'.env').read_text().splitlines():
        key,sep,value=line.partition('=')
        if sep and key=='AGGREGATOR_API_KEY':
            env[key]=value.strip().strip('"\'')
    env.update(AGGREGATOR_BASE_URL=args.base_url,VUS='1000',DURATION='60s',
               K6_ADDRESS='127.0.0.1:6566',K6_NO_USAGE_REPORT='true')
    try:
        if args.native_windows and not args.external_native_tunnel:
            if subprocess.run(['ss','-H','-ltn','sport = :43083'],capture_output=True,text=True,check=True).stdout.strip():
                raise RuntimeError('Native tunnel port already owned')
            command=h.command(server,'',multiplex=False)[:-2]
            command+=['-L','127.0.0.1:43083:10.77.0.1:30083','-o','ExitOnForwardFailure=yes','-N',server['user']+'@'+server['host']]
            tunnel_log=(directory/'ssh-tunnel.log').open('wb')
            tunnel=subprocess.Popen(command,stdout=tunnel_log,stderr=subprocess.STDOUT,preexec_fn=harness.child_limits)
            for _ in range(20):
                if tunnel.poll() is not None:
                    raise RuntimeError('Owned native tunnel exited')
                try:
                    with urllib.request.urlopen(args.base_url+'/ready',timeout=1):
                        break
                except OSError:
                    time.sleep(.5)
        if args.native_windows:
            # Windows-to-WSL localhost relay is deliberately absent in native
            # tunnel mode. Readiness is checked from Windows' network stack.
            for attempt in range(20):
                response=subprocess.run(['/mnt/c/Windows/System32/curl.exe','--silent','--show-error','--fail','--max-time','1',args.base_url+'/ready'],capture_output=True,text=True)
                if response.returncode==0 and json.loads(response.stdout).get('status')=='ready':
                    break
                if attempt==19:
                    raise RuntimeError('Native Windows common entry is not ready')
                time.sleep(.5)
        else:
            with urllib.request.urlopen(args.base_url+'/ready',timeout=10) as response:
                if json.load(response).get('status')!='ready':
                    raise RuntimeError('Common entry not ready')
        if not args.native_windows and subprocess.run(['ss','-H','-ltn','sport = :6566'],capture_output=True,text=True,check=True).stdout.strip():
            raise RuntimeError('k6 status port already owned by another process')
        (directory/'deployment-before.json').write_text(kube('get','deployment/aggregator','-o','json'))
        window=kube('exec','aggregator-coordinator-0','--','psql','-U','aggregator_coord','-d','coordination','-At','-c',
                    "SELECT json_build_object('poll_id',COALESCE(max(poll_id),0),'database_time',clock_timestamp()) FROM poll_attempts")
        (control/'window-start.json').write_text(json.dumps(json.loads(window),indent=2))
        for index,count in enumerate((1,3,3,1),1):
            ready(count)
            capture.observe(json.loads(kube('get','pods','-l','app=aggregator','-o','json'))['items'])
            time.sleep(10)
            case_name='replicas-'+str(count)+'-'+str(index)
            prefix=name+'-'+case_name
            snapshots=[]
            statuses=[]
            execution=None
            last=-5
            def during(_):
                nonlocal execution,last
                try:
                    with urllib.request.urlopen('http://127.0.0.1:6566/v1/status',timeout=1) as r:
                        value=json.load(r)['data']['attributes']
                except OSError:
                    return
                if not value['running']:
                    return
                now=time.monotonic()
                if execution is None:
                    execution=now
                elapsed=now-execution
                observe(elapsed,value)
            def observe(elapsed,value):
                nonlocal last
                statuses.append(dict(elapsed=elapsed,vus=value['vus']))
                if elapsed-last>=5:
                    dep=json.loads(kube('get','deployment/aggregator','-o','json'))
                    pods=json.loads(kube('get','pods','-l','app=aggregator','-o','json'))['items']
                    capture.observe(pods)
                    snapshots.append(dict(elapsed=elapsed,observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                        desired=dep['spec']['replicas'],ready=dep['status'].get('readyReplicas',0)))
                    (directory/(case_name+'-live.json')).write_text(json.dumps(dict(snapshots=snapshots,statuses=statuses),indent=2))
                    if int(elapsed)%20<5:
                        print(json.dumps(dict(case=case_name,elapsed=round(elapsed,2),vus=value['vus'],ready=snapshots[-1]['ready'])),flush=True)
                    last=elapsed
            if args.native_windows:
                case_dir=directory/case_name
                def windows_path(path):
                    return subprocess.check_output(['wslpath','-w',str(path)],text=True).strip()
                command=['/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe','-NoProfile','-ExecutionPolicy','Bypass','-File',
                         windows_path(ROOT/'infra/vps/p2-native-case.ps1'),'-CaseDirectory',windows_path(case_dir),'-K6Path',windows_path(args.k6),'-BaseUrl',args.base_url]
                process=subprocess.Popen(command,stdout=subprocess.PIPE,text=True)
                try:
                    for line in process.stdout:
                        value=json.loads(line)
                        if value['type']=='status':
                            observe(value['elapsed'],value)
                    code=process.wait()
                    if code:
                        raise RuntimeError('Native Windows case failed; inspect retained evidence')
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=15)
                summary=json.loads((case_dir/'summary.json').read_text(encoding='utf-8-sig'))
                resources=json.loads((case_dir/'generator-resources.json').read_text(encoding='utf-8-sig'))
                resources.update(passed=True,peak_rss_mib=resources['peak_resident_mib'])
            else:
                case_error=None
                try:
                    harness.run(args,prefix,script,env,during)
                except RuntimeError as exc:
                    # Exit 99 still produces valuable summary/resource evidence.
                    # Persist the failed case before stopping, without relaxing
                    # its threshold or counting an incomplete campaign as valid.
                    if not (ROOT/'.e2e'/(prefix+'-summary.json')).exists():raise
                    case_error=exc
                summary=json.loads((ROOT/'.e2e'/(prefix+'-summary.json')).read_text())
                resources=json.loads((ROOT/'.e2e'/(prefix+'-generator-resources.json')).read_text())
            metrics=summary['metrics']
            integrity=load.timing_integrity(metrics)
            passed=resources['passed'] and integrity['valid'] and all(load.threshold_ok(t) for m in metrics.values() for t in m.get('thresholds',{}).values())
            report=dict(case=case_name,passed=passed,timing_integrity=integrity,snapshots=snapshots,statuses=statuses,summary=summary,resources=resources)
            (directory/(case_name+'.json')).write_text(json.dumps(report,indent=2))
            rate=load.metric_values(metrics['successful_requests'])['rate']
            state['cases'].append(dict(case=case_name,passed=passed,successful_rps=rate))
            save()
            print(json.dumps(dict(case=case_name,passed=passed,complete_rps=rate,
                                  peak_rss_mib=resources['peak_rss_mib'])),flush=True)
            if not args.native_windows and case_error is not None:
                raise case_error
            if not integrity['valid']:
                raise RuntimeError('Invalid native timing; retained evidence, stopped capacity campaign')
        rates=[case['successful_rps'] for case in state['cases']]
        ratios,ratio_passed=compare(rates)
        state.update(successful_rps=rates,ratios=ratios,passed=ratio_passed and all(c['passed'] for c in state['cases']))
    except BaseException as exc:
        state.update(passed=False,error=type(exc).__name__+': '+str(exc))
        raise
    finally:
        try:
            ready(3)
            state['restored_three']=True
            (directory/'deployment-after.json').write_text(kube('get','deployment/aggregator','-o','json'))
            (directory/'pods-after.json').write_text(kube('get','pods','-l','app=aggregator','-o','json'))
        except BaseException as exc:
            state.update(passed=False,restored_three=False,restore_error=str(exc))
            raise
        finally:
            try:
                capture.close()
            finally:
                if tunnel and tunnel.poll() is None:
                    tunnel.terminate()
                    try:
                        tunnel.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        tunnel.kill();tunnel.wait()
                if tunnel_log:
                    tunnel_log.close()
                save()
                print('Evidence:',directory.relative_to(ROOT),flush=True)
                print('Source boundary:',(control/'window-start.json').relative_to(ROOT),flush=True)
    print(json.dumps(state),flush=True)
    raise SystemExit(0 if state['passed'] else 1)


if __name__=='__main__':
    main()
