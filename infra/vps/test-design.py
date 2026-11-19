#!/usr/bin/env python3
"""Run P2/P5 regression tests on A in isolated tooling containers, not live pods."""
import argparse
import datetime
import fcntl
from pathlib import Path
import shlex
import subprocess
from provision import Hosts, ROOT, inventory


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--focused-repeat',type=int,default=1,help='Repeat the bounded health/outage/bulkhead regression (1-50)')
    args=parser.parse_args()
    if not 1<=args.focused_repeat<=50: parser.error('focused-repeat must be 1-50')
    # Isolated containers still consume VPS CPU. Do not let regression builds
    # overlap capacity/chaos runs controlled from this operator checkout.
    (ROOT/'.vps').mkdir(exist_ok=True)
    lock=(ROOT/'.vps/p2-test.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    h=Hosts(inventory(ROOT/'infra/vps/inventory.json'))
    server=h.nodes[0]
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory='/opt/aat/regression/p2p5-'+stamp
    h.ssh(server,'sudo -n install -d -m 700 '+directory)
    ssh=shlex.join(h.command(server,'')[:-2])
    sources=[str(ROOT/'services'/s) for s in ('aggregator','client-api','notifier-consumer')]
    sources.append(str(ROOT/'infra/vps/test-design.sh'))
    subprocess.run(['rsync','-az','--rsync-path=sudo -n rsync','-e',ssh,*sources,
                    server['user']+'@'+server['host']+':'+directory+'/'],check=True)
    evidence=ROOT/'.vps'/('design-tests-'+stamp+'.log')
    evidence.parent.mkdir(exist_ok=True)
    process=subprocess.Popen(h.command(server,'sudo -n bash '+directory+'/test-design.sh '+directory+' '+str(args.focused_repeat)),
                             stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    with evidence.open('w') as log:
        for line in process.stdout:
            print(line,end='',flush=True)
            log.write(line);log.flush()
    code=process.wait()
    print('Evidence:',evidence.relative_to(ROOT),flush=True)
    if code: raise SystemExit(code)


if __name__=='__main__':main()
