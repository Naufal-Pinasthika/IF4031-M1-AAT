#!/usr/bin/env python3
"""Provision existing Ubuntu VPSs over SSH; no provider API or Terraform state needed.

Private WireGuard keys never leave their nodes. Tokens travel via SSH stdin,
not command-line arguments or logs. Inventory and runtime files are gitignored.
"""
import argparse
import concurrent.futures
import ipaddress
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def inventory(path):
    data = json.loads(path.read_text())
    nodes = data['nodes']
    if len(nodes) != 3 or [n['role'] for n in nodes] != ['server', 'agent', 'agent']:
        raise ValueError('expected server A and two agents B/C')
    for field in ('name', 'host', 'private_ip'):
        if len({n[field] for n in nodes}) != 3:
            raise ValueError(f'duplicate {field}')
    for n in nodes:
        ipaddress.IPv4Address(n['host'])
        address = ipaddress.IPv4Address(n['private_ip'])
        if address not in ipaddress.ip_network('10.77.0.0/24'):
            raise ValueError('private node address must be in 10.77.0.0/24')
        for field in ('name', 'user'):
            if not re.fullmatch(r'[a-zA-Z0-9_-]+', n[field]):
                raise ValueError(f'invalid {field}')
    if not Path(data['key']).is_file():
        raise ValueError('SSH private key does not exist')
    return data


class Hosts:
    def __init__(self, data):
        self.data = data
        self.nodes = data['nodes']

    def ssh(self, n, command, payload=None, capture=False):
        return subprocess.run(self.command(n, command), input=payload,
                              stdout=subprocess.PIPE if capture else None, check=True).stdout

    def command(self, n, command, multiplex=True):
        args = ['ssh', '-i', self.data['key'], '-o', 'BatchMode=yes',
                '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=15',
                '-o', 'ControlMaster=auto' if multiplex else 'ControlMaster=no',
                '-o', 'ControlPersist=60' if multiplex else 'ControlPersist=no',
                '-o', 'ControlPath=/tmp/aat-vps-%C' if multiplex else 'ControlPath=none',
                '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4',
                n['user'] + '@' + n['host'], command]
        return args

    def write(self, n, path, content, mode='600'):
        # No secret is included in an executable command or emitted to stdout.
        self.ssh(n, f'sudo -n install -d -m 700 {shlex.quote(str(Path(path).parent))}')
        self.ssh(n, f'sudo -n tee {shlex.quote(path)} >/dev/null', content.encode())
        self.ssh(n, f'sudo -n chmod {mode} {shlex.quote(path)}')

    def script(self, n, *args):
        command = 'sudo -n bash -s -- ' + shlex.join(args)
        self.ssh(n, command, (ROOT / 'infra/vps/host.sh').read_bytes())

    def parallel(self, action):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(action, self.nodes))

    def provision(self, version=None):
        self.parallel(lambda n: self.ssh(n, 'sudo -n true; hostname; uname -r; nproc; free -m'))
        self.parallel(lambda n: self.script(n, 'base'))
        self.parallel(lambda n: self.script(n, 'tester-tools'))
        public = {n['name']: self.ssh(n, 'sudo -n cat /etc/wireguard/aat.key | wg pubkey', capture=True).decode().strip()
                  for n in self.nodes}
        for n in self.nodes:
            content = f'[Interface]\nAddress = {n["private_ip"]}/24\nListenPort = 51820\nPrivateKey = NODE_PRIVATE_KEY\nMTU = 1380\n'
            for peer in self.nodes:
                if peer == n:
                    continue
                content += f'\n[Peer]\nPublicKey = {public[peer["name"]]}\nAllowedIPs = {peer["private_ip"]}/32\nEndpoint = {peer["host"]}:51820\nPersistentKeepalive = 25\n'
                self.ssh(n, f'sudo -n ufw allow from {peer["host"]} to any port 51820 proto udp')
            self.write(n, '/etc/aat-vps/network.template', content)
        self.parallel(lambda n: self.script(n, 'network'))
        for n in self.nodes:
            for peer in self.nodes:
                if peer != n:
                    self.ssh(n, f'ping -c 2 -W 3 {peer["private_ip"]}')
        server = self.nodes[0]
        if version is None:
            channels = json.loads(self.ssh(server, 'curl -fsSL --retry 3 https://update.k3s.io/v1-release/channels', capture=True))
            version = next(c['latest'] for c in channels['data'] if c['id'] == 'stable')
        if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+\+k3s[0-9]+', version):
            raise ValueError('unexpected k3s version')
        print('Using k3s', version, flush=True)
        for n in self.nodes:
            self.ssh(n, 'sudo -n install -d -m 700 /etc/rancher/k3s')
            cfg = {'node-name': n['name'], 'node-ip': n['private_ip'], 'flannel-iface': 'aatwg0',
                   'kube-proxy-arg': ['nodeport-addresses=10.77.0.0/24'],
                   'write-kubeconfig-mode': '600'}
            if n['role'] == 'server':
                cfg.update({'advertise-address': n['private_ip'],
                            'tls-san': [n['private_ip'], '127.0.0.1'],
                            'disable': ['traefik', 'servicelb']})
            else:
                cfg.pop('write-kubeconfig-mode')
                cfg['server'] = 'https://' + server['private_ip'] + ':6443'
                cfg['token-file'] = '/etc/rancher/k3s/join-token'
            # JSON is a YAML subset; no YAML library needed on operator laptop.
            self.write(n, '/etc/rancher/k3s/config.yaml', json.dumps(cfg, indent=2) + '\n')
            if n['role'] == 'agent':
                token = self.ssh(server, 'sudo -n cat /var/lib/rancher/k3s/server/node-token', capture=True)
                self.write(n, '/etc/rancher/k3s/join-token', token.decode())
            self.script(n, 'k3s', version, n['role'])
        self.ssh(server, 'sudo -n k3s kubectl wait --for=condition=Ready nodes --all --timeout=240s; sudo -n k3s kubectl get nodes -o wide')
        self.script(server, 'builder')
        self.ssh(server, 'sudo -n install -d -m 700 /opt/aat/repo /opt/aat/backups')
        print('Provisioning complete; public Kubernetes/database ports remain firewalled.', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory', type=Path, default=ROOT / 'infra/vps/inventory.json')
    p.add_argument('--version', help='Pin k3s; otherwise resolve stable ONCE for all three nodes')
    p.add_argument('action', choices=['provision', 'status'])
    args = p.parse_args()
    h = Hosts(inventory(args.inventory))
    if args.action == 'provision':
        h.provision(args.version)
    else:
        h.ssh(h.nodes[0], 'sudo -n k3s kubectl get nodes -o wide; sudo -n k3s kubectl get pods -A -o wide')


if __name__ == '__main__':
    main()
