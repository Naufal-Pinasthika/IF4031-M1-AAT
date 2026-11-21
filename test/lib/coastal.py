import hashlib
import datetime as dt
import json
import math
import re
import socket
import subprocess
import time


def forward(h):
    h.check(bool(h.hosts), 'P5 live verification requires --vps')
    targets = {'sensor': h.server['private_ip']+':30090', 'notifier': h.server['private_ip']+':30086'}
    command = h.hosts.command(h.server, '', multiplex=False)[:-2]
    ports = []
    for service, target in targets.items():
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        ports.append(port)
        command += ['-L', f'127.0.0.1:{port}:{target}']
        h.urls[service] = 'http://127.0.0.1:'+str(port)
    command += ['-o','ExitOnForwardFailure=yes','-N',h.server['user']+'@'+h.server['host']]
    process = subprocess.Popen(command)
    h.extra_processes.append(process)
    deadline = time.monotonic()+15
    while True:
        h.check(process.poll() is None, 'Private P5 SSH forwarding is active')
        try:
            for port in ports:
                with socket.create_connection(('127.0.0.1',port),timeout=1):
                    pass
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError('P5 forwarding did not become available')
            time.sleep(.1)


def hazard(ref):
    value = json.dumps(['SENSOR_PANTAI',ref],separators=(',',':'),ensure_ascii=False)
    return 'hz-'+hashlib.sha256(value.encode()).hexdigest()


def p95(values):
    return sorted(values)[math.ceil(.95*len(values))-1] if values else None


def timestamp(value):
    normalized = re.sub(r'\.(\d+)', lambda m: '.'+m.group(1)[:6].ljust(6, '0'), value.replace('Z', '+00:00'))
    return dt.datetime.fromisoformat(normalized).timestamp()


def valid_observation(item, stations):
    try:
        required = ['observation_id', 'station_id', 'observed_at', 'sea_level_anomaly_m', 'wave_height_m', 'quality', 'critical']
        if not isinstance(item, dict) or any(item.get(k) is None for k in required):
            return False
        if not isinstance(item['observation_id'], str) or not item['observation_id'].strip() or item['station_id'] not in stations:
            return False
        if type(item['critical']) is not bool or not isinstance(item['quality'], str) or not item['quality'].strip():
            return False
        timestamp(item['observed_at'])
        return all(type(item[k]) in (int, float) and math.isfinite(item[k]) for k in ('sea_level_anomaly_m', 'wave_height_m')) and item['wave_height_m'] >= 0
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
