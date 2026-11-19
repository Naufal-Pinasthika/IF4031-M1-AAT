#!/usr/bin/env python3
"""Return unused guest cache pages to the host without changing VM RAM sizes."""
import subprocess
from pathlib import Path
from resilience import monitor

for node in range(3):
    # Old lab VMs may have booted before this device was added to lab.sh.
    monitor(node, 'device_add virtio-balloon-pci,id=aatballoon,free-page-reporting=on')
    subprocess.run(['bash', str(Path(__file__).with_name('lab.sh')), 'ssh', str(node),
                    'sync; sysctl -w vm.drop_caches=3'], check=True)
