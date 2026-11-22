import argparse
import ipaddress
import json
import re
import shlex

from provision import Hosts, ROOT, inventory


BOOTSTRAP = r'''set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
test "$(. /etc/os-release; echo "$ID")" = ubuntu
apt-get update -qq
apt-get install -y -qq ca-certificates curl gnupg wireguard-tools ufw python3
curl --fail --silent --show-error https://dl.k6.io/key.gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg
printf '%s\n' 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main' > /etc/apt/sources.list.d/k6.list
apt-get update -qq
apt-get install -y -qq "k6=$1"
modprobe wireguard
install -d -m 700 /etc/wireguard /etc/aat-vps
if test ! -s /etc/wireguard/aat-load.key; then
    umask 077
    wg genkey > /etc/wireguard/aat-load.key
fi
k6 version
'''


SERVER_PEER = r'''import json, pathlib, re, shutil, subprocess, sys
p=json.loads(sys.stdin.read())
path=pathlib.Path('/etc/wireguard/aatwg0.conf')
text=path.read_text()
marker='# AAT dedicated load generator\n'
block=(marker+'[Peer]\nPublicKey = '+p['public']+'\nAllowedIPs = '+p['private_ip']+'/32\n'
       +'Endpoint = '+p['host']+':51820\nPersistentKeepalive = 25\n')
if marker in text:
    old=text.split(marker,1)[1].split('[Peer]',2)
    existing=old[1].split('\n[',1)[0]
    if 'PublicKey = '+p['public'] not in existing:
        raise RuntimeError('Existing load-generator identity differs; refuse silent replacement')
elif re.search(re.escape(p['private_ip'])+r'(?:/|\s|$)', text):
    raise RuntimeError('Load generator address is already assigned')
else:
    backup=pathlib.Path('/etc/aat-vps/aatwg0.pre-load.conf')
    if not backup.exists(): shutil.copy2(path,backup); backup.chmod(0o600)
    path.write_text(text.rstrip()+'\n\n'+block)
    path.chmod(0o600)
subprocess.run(['wg','set','aatwg0','peer',p['public'],'allowed-ips',p['private_ip']+'/32',
                'endpoint',p['host']+':51820','persistent-keepalive','25'],check=True)
subprocess.run(['ufw','allow','from',p['host'],'to','any','port','51820','proto','udp'],check=True)
print('Added dedicated peer without restarting the cluster VPN')
'''


CLIENT_PEER = r'''import json,pathlib,subprocess,sys
p=json.loads(sys.stdin.read())
key=pathlib.Path('/etc/wireguard/aat-load.key').read_text().strip()
text=('[Interface]\nAddress = '+p['private_ip']+'/32\nListenPort = 51820\nMTU = 1380\nPrivateKey = '+key+'\n\n'
      +'[Peer]\nPublicKey = '+p['server_public']+'\nAllowedIPs = '+p['server_private']+'/32\n'
      +'Endpoint = '+p['server_host']+':51820\nPersistentKeepalive = 25\n')
path=pathlib.Path('/etc/wireguard/aatwg0.conf')
if path.exists() and path.read_text()!=text:
    raise RuntimeError('Existing generator VPN configuration differs; refuse overwrite')
path.write_text(text);path.chmod(0o600)
subprocess.run(['ufw','allow','22/tcp'],check=True)
subprocess.run(['ufw','allow','from',p['server_host'],'to','any','port','51820','proto','udp'],check=True)
subprocess.run(['ufw','--force','enable'],check=True)
subprocess.run(['systemctl','enable','--now','wg-quick@aatwg0'],check=True)
'''


def remote_python(h, node, program, data):
    command = 'sudo -n python3 -c ' + shlex.quote(program)
    h.ssh(node, command, json.dumps(data).encode())


def main():
    p = argparse.ArgumentParser(description='Provision an external k6 generator with a scoped WireGuard peer; no Kubernetes')
    p.add_argument('--apply', action='store_true')
    p.add_argument('--k6-version', default='1.4.2', help='Pin the same version as the laptop comparison')
    args = p.parse_args()
    data = inventory(ROOT / 'infra/vps/inventory.json')
    h = Hosts(data)
    node, server = data['load_generator'], h.nodes[0]
    ipaddress.IPv4Address(node['host'])
    address = ipaddress.IPv4Address(node['private_ip'])
    if address not in ipaddress.ip_network('10.77.0.0/24') or any(node['private_ip'] == n['private_ip'] or node['host'] == n['host'] for n in h.nodes):
        raise ValueError('Generator must have its own public and private addresses outside the three application nodes')
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', node['user']) or not re.fullmatch(r'\d+\.\d+\.\d+', args.k6_version):
        raise ValueError('Invalid user or k6 version')
    print('Plan: install pinned k6/WireGuard on the load host, add one peer on A, retain B/C and all application data.', flush=True)
    h.ssh(node, 'sudo -n true; nproc; free -m; df -h /')
    if not args.apply:
        return
    h.ssh(node, 'sudo -n bash -s -- ' + shlex.quote(args.k6_version), BOOTSTRAP.encode())
    server_public = h.ssh(server, 'sudo -n wg show aatwg0 public-key', capture=True).decode().strip()
    client_public = h.ssh(node, 'sudo -n sh -c '+shlex.quote('wg pubkey < /etc/wireguard/aat-load.key'), capture=True).decode().strip()
    for key in (server_public, client_public):
        if not re.fullmatch(r'[A-Za-z0-9+/]{43}=', key):
            raise ValueError('Unexpected WireGuard public key')
    remote_python(h, server, SERVER_PEER, {**node, 'public': client_public})
    remote_python(h, node, CLIENT_PEER, {**node, 'server_public': server_public,
        'server_host': server['host'], 'server_private': server['private_ip']})
    h.ssh(node, 'ping -c 3 -W 3 '+server['private_ip']+'; curl --fail --silent --show-error --max-time 10 '+
        'http://'+server['private_ip']+':30083/ready; k6 version')
    print('Generator ready. SSH is control-only; k6 traffic uses the private WireGuard route.', flush=True)


if __name__ == '__main__':
    main()
