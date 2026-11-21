from pathlib import Path
from contextlib import contextmanager
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
from common import NS, parser, run


@contextmanager
def field_logs(h, ordinal):
    if not h.args.vps:
        yield
        return
    path=h.folder/f'round-{ordinal}-field-path.log'
    selector='app in (auth,client-api,aggregator-gateway,aggregator)'
    count=len(json.loads(h.kube('get','pods','-l',selector,'-o','json'))['items'])
    # Drop each pod's initial tail record, then acknowledge that ALL selected
    # streams are subscribed before starting k6. Old run records are not evidence.
    filter_script=(f'!seen[$1]++ {{ ready++; if (ready=={count}) {{ print "AAT_LOG_STREAM_READY"; fflush() }}; next }} '
                   f'/p5-{ordinal}-FIELD-|gateway_probe_failed|gateway_discovery_failed/ {{ print; fflush() }}')
    command=(f"timeout {h.args.seconds+45}s sudo -n k3s kubectl -n {NS} logs "
             '-l '+shlex.quote(selector)+' --prefix --tail=1 --follow --max-log-requests=10 | '
             'awk '+shlex.quote(filter_script))
    with path.open('wb') as log:
        process=subprocess.Popen(h.hosts.command(h.server,command,multiplex=False),
                                 stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+20
            while 'AAT_LOG_STREAM_READY' not in path.read_text(errors='replace'):
                h.check(process.poll() is None and time.monotonic()<deadline,
                        'Every selected server log stream subscribes before k6 starts')
                time.sleep(.05)
            yield
        finally:
            # kubectl's follow stream can lag the last completed k6 response.
            # Drain it briefly before closing this run-owned SSH connection.
            time.sleep(2)
            process.terminate()
            process.wait(timeout=20)
    h.check(f'p5-{ordinal}-FIELD-' in path.read_text(errors='replace'),
            'Live Field-path server logs were retained before rotation')


def scenario(h):
    h.check(bool(shutil.which('k6')),'k6 is installed on the laptop, not an application VPS')
    h.check(h.args.seconds>=60,'Sustained load duration is at least 60 seconds')
    h.auth('MEDIA')
    h.auth('FIELD')
    config=json.loads(h.kube('get','configmap','aggregator-config','-o','json'))['data']
    h.artifact('source-condition.json',{'pods':h.pods(),'coastal':{k:v for k,v in config.items() if k.startswith('COASTAL_')}})
    failures=[]
    for ordinal in range(h.args.repeat):
        env={**os.environ,**{k:h.values[k] for k in ('MEDIA_CLIENT_ID','MEDIA_CLIENT_SECRET','FIELD_CLIENT_ID','FIELD_CLIENT_SECRET')},
             'AUTH_BASE_URL':h.urls['auth'],'CLIENT_API_BASE_URL':h.urls['client'],
             'MODE':h.args.mode,'DURATION':str(h.args.seconds)+'s','ROUND':str(ordinal+1),
             'SUMMARY_PATH':str(h.folder/f'round-{ordinal+1}-summary.json')}
        with field_logs(h,ordinal+1), (h.folder/f'round-{ordinal+1}.log').open('w') as log:
            result=subprocess.run(['k6','run','--quiet',str(Path(__file__).with_name('load.js'))],env=env,stdout=log,stderr=subprocess.STDOUT,
                                  timeout=h.args.seconds+120)
        summary=json.loads(Path(env['SUMMARY_PATH']).read_text())
        metrics={name:{'values':item.get('values'),'thresholds':item.get('thresholds')} for name,item in summary['metrics'].items()
                 if name in ('unexpected_errors','field_errors','media_rejected','bmkg_success_ms','dropped_iterations','http_reqs')}
        h.note({'round':ordinal+1,'mode':h.args.mode,'exit':result.returncode,'metrics':metrics})
        if result.returncode:
            failures.append(ordinal+1)
    h.check(not failures,'All sustained load rounds meet unchanged per-role and latency thresholds')


if __name__=='__main__':
    p=parser('Run report-facing P5 isolation or priority load from the operator laptop')
    p.add_argument('--mode',choices=['isolation','priority'],default='isolation')
    p.add_argument('--seconds',type=int,default=60)
    raise SystemExit(run('p5/02-load',['P5.3','P5.4','P5.5'],p,scenario))
