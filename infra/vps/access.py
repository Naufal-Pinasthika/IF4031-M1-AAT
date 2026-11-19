#!/usr/bin/env python3
"""Operate the VPS cluster without installing Kubernetes or Docker locally."""
import argparse
import shlex
import subprocess
import resource
from provision import Hosts, ROOT, inventory
from vps_config import TUNNEL_PORTS


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['tunnel', 'observe', 'status', 'smoke', 'kube'])
    p.add_argument('args', nargs=argparse.REMAINDER)
    a = p.parse_args()
    if a.args[:1] == ['--']:
        a.args = a.args[1:]
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    server = h.nodes[0]
    if a.action == 'observe':
        args=h.command(server,'',multiplex=False)[:-2]
        args+=['-L','127.0.0.1:8090:127.0.0.1:8090','-o','ExitOnForwardFailure=yes',
               server['user']+'@'+server['host'],
               'sudo -n k3s kubectl -n sistem-informasi-kebencanaan-bnpb port-forward --address=127.0.0.1 service/observability 8090:8090']
        print('Private operations dashboard: http://localhost:8090 while this process runs.',flush=True)
        subprocess.run(args,check=True)
    elif a.action == 'tunnel':
        args = h.command(server, '', multiplex=False)[:-2]
        for port in TUNNEL_PORTS:
            args += ['-L', f'127.0.0.1:{port}:{server["private_ip"]}:{port}']
        args += ['-L', '127.0.0.1:16443:127.0.0.1:6443', '-o', 'ExitOnForwardFailure=yes', '-N', server['user'] + '@' + server['host']]
        ports = '/'.join(map(str, TUNNEL_PORTS))
        print('Endpoints available on localhost:'+ports+' while this process runs.', flush=True)
        def limits():
            _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard), hard))
        subprocess.run(args, check=True, preexec_fn=limits)
    elif a.action == 'status':
        h.ssh(server, 'sudo -n k3s kubectl get nodes -o wide; sudo -n k3s kubectl get pods -A -o wide')
    elif a.action == 'smoke':
        h.ssh(server, 'sudo -n env E2E_NODE=http://10.77.0.1 python3 /opt/aat/repo/scripts/m2/e2e/smoke.py')
    else:
        h.ssh(server, 'sudo -n k3s kubectl ' + shlex.join(a.args))


if __name__ == '__main__':
    main()
