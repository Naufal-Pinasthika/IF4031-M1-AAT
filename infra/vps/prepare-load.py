#!/usr/bin/env python3
"""Raise the SSH operator account FD limit for 10k tunneled connections.

Does not change authentication, firewall, or restart sshd. Applies through PAM
on NEW connections only. Old tunnels must be reconnected.
"""
from provision import Hosts, ROOT, inventory

h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
n = h.nodes[0]
content = f'{n["user"]} soft nofile 65536\n{n["user"]} hard nofile 65536\n'
h.ssh(n, 'sudo -n tee /etc/security/limits.d/90-aat-load.conf >/dev/null', content.encode())
print('New SSH session file descriptor limit:')
import subprocess
subprocess.run(h.command(n, 'ulimit -n', multiplex=False), check=True)
