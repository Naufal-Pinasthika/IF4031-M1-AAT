from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run
from tester_agent import practice


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Network faults affect only the VPS lab')
    for repetition in range(h.args.repeat):
        def observe(node):
            targets = [(n['private_ip'], 6443 if n['role'] == 'server' else 10250)
                       for n in h.hosts.nodes if n['name'] != node['name']]
            probe = '''import json,socket,subprocess,sys,tempfile,time
log=tempfile.TemporaryFile()
monitor=subprocess.Popen(['sudo','-n','timeout','70','stdbuf','-oL','nft','monitor','ruleset'],stdout=log,stderr=subprocess.STDOUT)
targets=json.loads(sys.argv[1]);end=time.monotonic()+70;previous=None
while time.monotonic()<end:
 connections=[]
 for host,port in targets:
  with socket.socket() as s:
   s.settimeout(0.3);connections.append([host,port,s.connect_ex((host,port))])
 rules=[]
 for command in ('iptables-save','iptables-legacy-save'):
  result=subprocess.run(['sudo','-n',command],capture_output=True,text=True,timeout=2)
  rules.extend(command+':'+line for line in result.stdout.splitlines() if line.startswith('-A'))
 row={'monotonic':time.monotonic(),'connections':connections}
 if rules!=previous: row['rules']=rules;previous=rules
 print(json.dumps(row),flush=True);time.sleep(0.2)
monitor.wait(timeout=5);log.seek(0)
print(json.dumps({'nft_events':log.read().decode(errors='replace')}))
'''
            call = subprocess.run(h.hosts.command(node, 'python3 -u -c '+shlex.quote(probe)+' '+
                shlex.quote(json.dumps(targets)), multiplex=False), capture_output=True, timeout=85, check=True)
            rows = [json.loads(line) for line in call.stdout.splitlines()]
            return {'node': node['name'], 'samples': rows[:-1], 'nft_events': rows[-1]['nft_events']}
        with ThreadPoolExecutor(max_workers=3) as pool:
            observations = [pool.submit(observe, node) for node in h.hosts.nodes]
            try:
                practice(h, 'partition', repetition+1)
            finally:
                traces = [future.result() for future in observations]
                h.artifact(f'round-{repetition+1}-network-traces.json', traces)
        h.check(all(n['samples'] and all(c[2] == 0 for c in n['samples'][0]['connections']) for n in traces),
                'Independent private-node TCP probes were reachable before the fault')
        h.check(any(c[2] != 0 for n in traces for row in n['samples'] for c in row['connections']),
                'Actual inter-node connectivity loss was independently observed')
        peer_ips = {n['private_ip'] for n in h.hosts.nodes}
        h.check(any('drop' in line.lower() and any(ip in line for ip in peer_ips)
                    for n in traces for line in n['nft_events'].splitlines()),
                'Kernel firewall events confirm a DROP rule targeting an actual peer address')
        h.check(all(all(c[2] == 0 for c in n['samples'][-1]['connections']) for n in traces),
                'Independent private-node TCP probes recovered after Agent cleanup')


if __name__ == '__main__':
    p = parser('Observe official network partition and automatic read/storage recovery')
    p.set_defaults(timeout=180)
    raise SystemExit(run('tester-agent/06-partition', ['Official partition and recovery'], p, scenario))
