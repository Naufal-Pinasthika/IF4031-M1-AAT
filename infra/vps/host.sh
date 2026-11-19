#!/usr/bin/env bash
# Executed through sudo on existing Ubuntu VPSs. Does not delete data.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
case "$1" in
  base)
    test "$(. /etc/os-release; echo "$ID")" = ubuntu
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl python3 wireguard-tools ufw rsync
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
