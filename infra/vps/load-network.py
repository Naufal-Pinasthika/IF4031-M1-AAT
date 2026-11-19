#!/usr/bin/env python3
"""Temporary operator-to-A WireGuard path, without SSH channel limits.

Run in WSL as root. Does not restart the server mesh, change default routes,
publish HTTP ports, or change Kubernetes membership. Always run down afterward.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import shlex
import subprocess
import uuid
from provision import Hosts, ROOT, inventory

STATE = Path('/run/aat-k6-network.json')
KEY = Path('/run/aat-k6-network.key')
IFACE = 'aatload0'
CLIENT = '10.77.0.254'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['up', 'down'])
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('Run as root in WSL')
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    n = h.nodes[0]

    def remote(command):
        command_args = h.command(n, command, multiplex=False)
        # Root uses the operator's existing, verified host fingerprints.
        command_args[1:1] = ['-o', 'UserKnownHostsFile=' + str(Path(h.data['key']).parent / 'known_hosts')]
        return subprocess.check_output(command_args, text=True).strip()

    def local(*items):
        subprocess.run(items, check=True, stdout=subprocess.DEVNULL)

    def down():
        if not STATE.exists():
            raise RuntimeError('No owned load-network state; refusing to remove unknown resources')
        state = json.loads(STATE.read_text())
        remote('sudo -n wg set aatwg0 peer ' + shlex.quote(state['public_key']) + ' remove')
        if state['firewall_added']:
            remote('sudo -n ufw --force delete allow from ' + state['source_ip'] + ' to any port 51820 proto udp')
        if state['interface_added']:
            local('ip', 'link', 'delete', IFACE)
        KEY.unlink(missing_ok=True)
        STATE.unlink()
        print('Removed temporary load-test peer, route, key and firewall rule')

    if args.action == 'down':
        down()
        return
    if STATE.exists() or subprocess.run(['ip', 'link', 'show', IFACE], capture_output=True).returncode == 0:
        raise RuntimeError('Existing load-network state/interface; inspect or run down first')
    if CLIENT in remote('sudo -n wg show aatwg0 allowed-ips'):
        raise RuntimeError('Test client address is already owned by another peer')
    source = remote('printf "%s" "$SSH_CONNECTION"').split()[0]
    ipaddress.IPv4Address(source)
    public_server = remote('sudo -n wg show aatwg0 public-key')
    key = subprocess.check_output(['wg', 'genkey'])
    KEY.touch(mode=0o600, exist_ok=False)
    KEY.write_bytes(key)
    public = subprocess.check_output(['wg', 'pubkey'], input=key).decode().strip()
    # Do not delete a pre-existing firewall rule owned by someone else.
    firewall_needed = source not in remote('sudo -n ufw status')
    state = dict(public_key=public, source_ip=source, firewall_added=False, interface_added=False)

    def save():
        STATE.write_text(json.dumps(state))
        STATE.chmod(0o600)

    save()
    try:
        if firewall_needed:
            remote('sudo -n ufw allow from ' + source + ' to any port 51820 proto udp comment aat-k6-' + uuid.uuid4().hex[:8])
            state['firewall_added'] = True
            save()
        remote('sudo -n wg set aatwg0 peer ' + shlex.quote(public) + ' allowed-ips ' + CLIENT + '/32')
        local('ip', 'link', 'add', IFACE, 'type', 'wireguard')
        state['interface_added'] = True
        save()
        local('wg', 'set', IFACE, 'private-key', str(KEY), 'peer', public_server,
              'allowed-ips', n['private_ip']+'/32', 'endpoint', n['host']+':51820', 'persistent-keepalive', '25')
        local('ip', 'address', 'add', CLIENT+'/32', 'dev', IFACE)
        local('ip', 'link', 'set', IFACE, 'mtu', '1380', 'up')
        local('ip', 'route', 'add', n['private_ip']+'/32', 'dev', IFACE)
        subprocess.run(['curl', '--fail', '--max-time', '10', 'http://'+n['private_ip']+':30083/ready'], check=True)
        print('\nPrivate load-test path ready; run load.py --base-url http://'+n['private_ip']+':30083')
    except BaseException:
        down()
        raise


if __name__ == '__main__':
    main()
