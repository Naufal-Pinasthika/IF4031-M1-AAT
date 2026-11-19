#!/usr/bin/env bash
# Run inside WSL as root. All VMs/disks/network rules belong to this lab.
set -euo pipefail
LAB=/var/lib/aat-m2-e2e
REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
SSH=(ssh -i "${LAB}/id_ed25519" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="${LAB}/known_hosts" -o ConnectTimeout=5)
mkdir -p "${LAB}" "${REPO}/.e2e"

case "${1:-}" in
  build)
    exec bash "${REPO}/scripts/m2/e2e/build.sh" ;;
  images)
    images=()
    shift
    services=("$@")
    if (( ${#services[@]} == 0 )); then services=(aggregator auth bmkg client-api dashboard-consumer notifier-consumer pvmbg); fi
    tag="${IMAGE_TAG:-m2-e2e}"
    if [[ -z "${IMAGE_TAG:-}" && -s "${REPO}/.e2e/image-tag" ]]; then tag="$(<"${REPO}/.e2e/image-tag")"; fi
    for service in "${services[@]}"; do images+=("aat/${service}:${tag}"); done
    docker save --output "${LAB}/apps.tar" "${images[@]}"
    for node in 0 1 2; do
      runtime_ready=false
      for attempt in {1..60}; do
        if "${SSH[@]}" "root@192.168.93.$((10+node))" 'test -S /run/k3s/containerd/containerd.sock && k3s ctr version >/dev/null' 2>/dev/null; then
          runtime_ready=true
          break
        fi
        sleep 2
      done
      [[ "${runtime_ready}" == true ]] || { echo "Container runtime on node ${node} did not become ready" >&2; exit 1; }
      scp -i "${LAB}/id_ed25519" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="${LAB}/known_hosts" "${LAB}/apps.tar" "root@192.168.93.$((10+node)):/tmp/aat-apps.tar"
      "${SSH[@]}" "root@192.168.93.$((10+node))" 'k3s ctr images import /tmp/aat-apps.tar'
    done
    exit 0 ;;
  keepalive)
    touch "${LAB}/running"
    while [[ -f "${LAB}/running" ]]; do sleep 30; done
    exit 0 ;;
  down)
    for node in 2 1 0; do
      "${SSH[@]}" "root@192.168.93.$((10+node))" poweroff || true
    done
    for attempt in {1..45}; do
      active=false
      for node in 0 1 2; do
        pidfile="${LAB}/node${node}/qemu.pid"
        if [[ -f "${pidfile}" ]]; then
          pid="$(<"${pidfile}")"
          if [[ -r "/proc/${pid}/cmdline" ]] && grep -zFq "aat-m2-node${node}" "/proc/${pid}/cmdline"; then active=true; fi
        fi
      done
      if [[ "${active}" == false ]]; then
        rm -f -- "${LAB}/running"
        echo 'All three lab VMs shut down; disks retained.'
        exit 0
      fi
      sleep 1
    done
    echo 'A lab VM is still shutting down; disks were not deleted.' >&2
    exit 1 ;;
  ssh) shift; node="$1"; shift; exec "${SSH[@]}" "root@192.168.93.$((10+node))" "$@" ;;
  kube) shift; exec env KUBECONFIG="${REPO}/.e2e/kubeconfig" kubectl "$@" ;;
  up) ;;
  *) echo "Usage: $0 up|keepalive|ssh <0..2> <command>|kube <args>" >&2; exit 2 ;;
esac

[[ $EUID == 0 ]] || { echo 'Run as root inside WSL' >&2; exit 1; }
control_plane_cpus="${AAT_LAB_CONTROL_PLANE_CPUS:-4}"
[[ "${control_plane_cpus}" =~ ^[2-8]$ ]] || { echo 'AAT_LAB_CONTROL_PLANE_CPUS must be an integer from 2 to 8' >&2; exit 1; }
(( control_plane_cpus + 4 <= $(nproc) )) || { echo 'Lab vCPU allocation exceeds host logical CPUs' >&2; exit 1; }
command -v qemu-system-x86_64 >/dev/null
# A fresh WSL boot may not load the hardware-specific KVM module until asked.
# Do not assume Intel: choosing the wrong module hides a working AMD backend.
if [[ ! -e /sys/class/misc/kvm/dev ]]; then
  case "$(awk '/vendor_id/ {print $3; exit}' /proc/cpuinfo)" in
    AuthenticAMD) modprobe kvm_amd ;;
    GenuineIntel) modprobe kvm_intel ;;
    *) echo 'Unsupported CPU vendor for this KVM lab' >&2; exit 1 ;;
  esac
fi
# Imported WSL distributions do not always populate this device automatically.
if [[ ! -e /dev/kvm && -r /sys/class/misc/kvm/dev ]]; then
  IFS=: read -r major minor < /sys/class/misc/kvm/dev
  mknod /dev/kvm c "${major}" "${minor}"
  chmod 660 /dev/kvm
fi
[[ -e /dev/kvm ]] || { echo 'Nested KVM is required' >&2; exit 1; }
if [[ ! -f "${LAB}/id_ed25519" ]]; then ssh-keygen -q -t ed25519 -N '' -f "${LAB}/id_ed25519"; fi
public_key="$(<"${LAB}/id_ed25519.pub")"

python3 - "${LAB}" <<'PY'
import hashlib,pathlib,sys
lab=pathlib.Path(sys.argv[1])
expected=next(line.split()[0] for line in (lab/'SHA256SUMS').read_text().splitlines() if line.split()[-1].lstrip('*')=='jammy-server-cloudimg-amd64.img')
digest=hashlib.sha256()
with (lab/'jammy.img').open('rb') as f:
    for chunk in iter(lambda:f.read(1024*1024),b''): digest.update(chunk)
actual=digest.hexdigest()
assert actual==expected, 'Ubuntu cloud image checksum mismatch'
print('Ubuntu image checksum verified')
PY

if ! ip link show aatbr0 >/dev/null 2>&1; then
  ip link add aatbr0 type bridge
  ip addr add 192.168.93.1/24 dev aatbr0
fi
ip link set aatbr0 up
sysctl -w net.ipv4.ip_forward=1 >/dev/null
iptables -t nat -C POSTROUTING -s 192.168.93.0/24 ! -d 192.168.93.0/24 -j MASQUERADE 2>/dev/null || iptables -t nat -A POSTROUTING -s 192.168.93.0/24 ! -d 192.168.93.0/24 -j MASQUERADE
iptables -C FORWARD -i aatbr0 -j ACCEPT 2>/dev/null || iptables -I FORWARD -i aatbr0 -j ACCEPT
iptables -C FORWARD -o aatbr0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -I FORWARD -o aatbr0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT

for node in 0 1 2; do
  directory="${LAB}/node${node}"
  mkdir -p "${directory}"
  if [[ ! -f "${directory}/disk.qcow2" ]]; then
    qemu-img create -f qcow2 -F qcow2 -b "${LAB}/jammy.img" "${directory}/disk.qcow2" 35G
    python3 - "${directory}" "${node}" "${public_key}" <<'PY'
import pathlib,sys
directory=pathlib.Path(sys.argv[1]); n=int(sys.argv[2]); key=sys.argv[3]
(directory/'user-data').write_text(f'''#cloud-config
hostname: aat-node-{n}
manage_etc_hosts: true
disable_root: false
ssh_pwauth: false
ssh_authorized_keys:
  - {key}
users:
  - name: root
    ssh_authorized_keys:
      - {key}
''')
(directory/'meta-data').write_text(f'instance-id: aat-m2-node-{n}\nlocal-hostname: aat-node-{n}\n')
(directory/'network-config').write_text(f'''version: 2
ethernets:
  enp1s0:
    match:
      macaddress: "52:54:00:aa:00:{n+1:02x}"
    set-name: enp1s0
    addresses: [192.168.93.{10+n}/24]
    routes:
      - to: default
        via: 192.168.93.1
    nameservers:
      addresses: [1.1.1.1,8.8.8.8]
''')
PY
    cloud-localds --network-config="${directory}/network-config" "${directory}/seed.iso" "${directory}/user-data" "${directory}/meta-data"
  fi
  tap="aattap${node}"
  if ! ip link show "${tap}" >/dev/null 2>&1; then ip tuntap add "${tap}" mode tap; fi
  ip link set "${tap}" master aatbr0
  ip link set "${tap}" up
  if [[ -f "${directory}/qemu.pid" ]]; then
    previous_pid="$(<"${directory}/qemu.pid")"
    if [[ -r "/proc/${previous_pid}/cmdline" ]] && grep -zFq "aat-m2-node${node}" "/proc/${previous_pid}/cmdline"; then continue; fi
  fi
  memory=1280
  cpus=2
  [[ "${node}" != 0 ]] || memory=4096
  [[ "${node}" != 0 ]] || cpus="${control_plane_cpus}"
  qemu-system-x86_64 -name "aat-m2-node${node}" -enable-kvm -cpu host -smp "${cpus}" -m "${memory}" \
    -device virtio-balloon-pci,id=aatballoon,free-page-reporting=on \
    -drive "file=${directory}/disk.qcow2,format=qcow2,if=virtio" \
    -drive "file=${directory}/seed.iso,format=raw,media=cdrom,readonly=on" \
    -netdev "tap,id=net0,ifname=${tap},script=no,downscript=no" \
    -device "virtio-net-pci,netdev=net0,mac=52:54:00:aa:00:0$((node+1))" \
    -display none -serial "file:${directory}/console.log" -monitor "unix:${directory}/monitor.sock,server,nowait" \
    -pidfile "${directory}/qemu.pid" -daemonize
done

for node in 0 1 2; do
  ready=false
  for attempt in {1..90}; do
    if "${SSH[@]}" "root@192.168.93.$((10+node))" 'uname -r; cat /proc/sys/kernel/random/boot_id' 2>/dev/null; then ready=true; break; fi
    sleep 2
  done
  [[ "${ready}" == true ]] || { tail -60 "${LAB}/node${node}/console.log"; exit 1; }
done

if ! "${SSH[@]}" root@192.168.93.10 'test -f /etc/rancher/k3s/k3s.yaml'; then
  "${SSH[@]}" root@192.168.93.10 'curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --disable servicelb --node-ip 192.168.93.10 --tls-san 192.168.93.10" sh -'
fi
token="$("${SSH[@]}" root@192.168.93.10 'cat /var/lib/rancher/k3s/server/node-token')"
for node in 1 2; do
  if ! "${SSH[@]}" "root@192.168.93.$((10+node))" 'test -x /usr/local/bin/k3s'; then
    printf '%s' "${token}" | "${SSH[@]}" "root@192.168.93.$((10+node))" 'read -r -d "" token || true; curl -sfL https://get.k3s.io | K3S_URL=https://192.168.93.10:6443 K3S_TOKEN="$token" sh -'
  fi
done
unset token
"${SSH[@]}" root@192.168.93.10 'cat /etc/rancher/k3s/k3s.yaml' | sed 's/127.0.0.1/192.168.93.10/g' > "${REPO}/.e2e/kubeconfig"
chmod 600 "${REPO}/.e2e/kubeconfig"
if ! command -v kubectl >/dev/null; then
  scp -i "${LAB}/id_ed25519" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="${LAB}/known_hosts" root@192.168.93.10:/usr/local/bin/k3s /usr/local/bin/kubectl
fi
api_ready=false
for attempt in {1..90}; do
  if KUBECONFIG="${REPO}/.e2e/kubeconfig" kubectl get --raw=/readyz --request-timeout=3s >/dev/null 2>&1; then
    api_ready=true
    break
  fi
  sleep 2
done
[[ "${api_ready}" == true ]] || { echo 'Kubernetes API did not become ready' >&2; exit 1; }
KUBECONFIG="${REPO}/.e2e/kubeconfig" kubectl wait --for=condition=Ready node --all --timeout=180s
KUBECONFIG="${REPO}/.e2e/kubeconfig" kubectl get nodes -o wide
