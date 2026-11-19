#!/usr/bin/env python3
"""Run P2/P5 regression tests on A in isolated tooling containers, not live pods."""
import datetime
from pathlib import Path
import shlex
import subprocess
from provision import Hosts, ROOT, inventory


def main():
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
    process=subprocess.Popen(h.command(server,'sudo -n bash '+directory+'/test-design.sh '+directory),
                             stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    with evidence.open('w') as log:
        for line in process.stdout:
            print(line,end='',flush=True)
            log.write(line);log.flush()
    code=process.wait()
    print('Evidence:',evidence.relative_to(ROOT),flush=True)
    if code: raise SystemExit(code)


if __name__=='__main__':main()
