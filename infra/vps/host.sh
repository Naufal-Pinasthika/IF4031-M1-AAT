#!/usr/bin/env bash
# Executed through sudo on existing Ubuntu VPSs. Does not delete data.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
case "$1" in
  base)
    test "$(. /etc/os-release; echo "$ID")" = ubuntu
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl python3 wireguard-tools ufw rsync iptables
    modprobe wireguard
    install -d -m 700 /etc/wireguard /etc/aat-vps
    if [[ ! -s /etc/wireguard/aat.key ]]; then
      umask 077
      wg genkey > /etc/wireguard/aat.key
    fi
    sysctl -w net.ipv4.ip_forward=1
    printf 'net.ipv4.ip_forward=1\n' > /etc/sysctl.d/90-aat-forward.conf
    # Keep administrative access; do not flush existing firewall rules.
    ufw allow 22/tcp
    ufw default deny incoming
    ufw default allow outgoing
    ufw allow in on aatwg0
    ufw route allow in on aatwg0
    ufw route allow out on aatwg0
    ufw route allow from 10.42.0.0/16 to 10.42.0.0/16
    ufw allow in from 10.42.0.0/16
    ufw allow in from 10.43.0.0/16
    ;;
  tester-tools)
    # The unchanged Agent has no distro ELF loader. Use k3s' static binary
    # with the same kernel backend; never flush, filter or rewrite its rules.
    python3 - <<'PY'
import json
import os
from pathlib import Path
import subprocess
import shutil
import struct

path = Path('/usr/sbin/iptables')
before = subprocess.check_output([str(path), '-S'])
version = subprocess.check_output([str(path), '--version'])
backend = 'nft' if b'(nf_tables)' in version else 'legacy' if b'(legacy)' in version else None
assert backend, version
source = Path('/var/lib/rancher/k3s/data/current/bin/aux/xtables-'+backend+'-multi').resolve(strict=True)
assert source.is_relative_to('/var/lib/rancher/k3s/data')
data = source.read_bytes()
assert data[:6] == b'\x7fELF\x02\x01', 'Expected static x86-64 ELF from the installed k3s release'
offset, = struct.unpack_from('<Q', data, 32)
entry, count = struct.unpack_from('<HH', data, 54)
assert all(struct.unpack_from('<I', data, offset+i*entry)[0] != 3 for i in range(count)), 'ELF interpreter is not available inside the Agent'
original = os.readlink(path) if path.is_symlink() else None
assert original is not None, 'Refuse overwriting an unrecognized regular host executable'
backup = Path('/etc/aat-vps/iptables-exec-before-tester.json')
backup.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
if not backup.exists():
    with backup.open('x') as output:
        json.dump({'path': str(path), 'link': original, 'resolved': str(path.resolve()), 'version': version.decode().strip()}, output)
    backup.chmod(0o600)
target = Path('/usr/local/libexec/aat-xtables-'+backend+'-multi')
target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
shutil.copyfile(source, target)
target.chmod(0o755)
temporary = path.with_name('iptables.aat-tester-link')
temporary.symlink_to(os.path.relpath(target, path.parent))
temporary.replace(path)
try:
    assert (b'(nf_tables)' in subprocess.check_output([str(path), '--version'])) == (backend == 'nft')
    assert subprocess.check_output([str(path), '-S']) == before
except BaseException:
    temporary.symlink_to(original)
    temporary.replace(path)
    raise
print('Static k3s iptables is executable through /host; kernel backend and filter rules retained')
PY
    ;;
  network)
    # Config is prepared by provision.py; private key stays on this node.
    python3 - <<'PY'
from pathlib import Path
key = Path('/etc/wireguard/aat.key').read_text().strip()
path = Path('/etc/wireguard/aatwg0.conf')
content = Path('/etc/aat-vps/network.template').read_text().replace('NODE_PRIVATE_KEY', key)
changed = not path.exists() or path.read_text() != content
if changed:
    path.write_text(content)
    path.chmod(0o600)
Path('/etc/aat-vps/network.changed').write_text(str(int(changed)))
PY
    if [[ "$(cat /etc/aat-vps/network.changed)" = 1 ]]; then
      systemctl restart wg-quick@aatwg0
    fi
    systemctl enable --now wg-quick@aatwg0
    ufw --force enable
    # Persist the firewall across node reboots, not only this SSH session.
    systemctl enable --now ufw.service
    ;;
  k3s)
    version="$2"; role="$3"
    # Run the official installer with the SAME resolved version on all nodes.
    curl --fail --location --retry 3 https://get.k3s.io -o /etc/aat-vps/install-k3s.sh
    INSTALL_K3S_VERSION="$version" INSTALL_K3S_EXEC="$role" sh /etc/aat-vps/install-k3s.sh
    # Order the service after the encrypted node network, including reboots.
    unit=k3s
    [[ "$role" != agent ]] || unit=k3s-agent
    mkdir -p "/etc/systemd/system/$unit.service.d"
    printf '[Unit]\nRequires=wg-quick@aatwg0.service\nAfter=wg-quick@aatwg0.service\n' > "/etc/systemd/system/$unit.service.d/aat-network.conf"
    systemctl daemon-reload
    systemctl enable --now "$unit"
    ;;
  builder)
    apt-get update -qq
    apt-get install -y -qq docker.io docker-buildx
    systemctl enable --now docker
    ;;
  *) exit 2 ;;
esac
