"""Bounded, filtered pod-log capture that survives pod removal during P2 tests."""
import json
from pathlib import Path
import shlex
import subprocess


REMOTE = r'''
import json,os,select,subprocess,sys,time
from pathlib import Path
cfg=json.loads(sys.argv[1]);stop=Path(cfg['stop']);stop.parent.mkdir(parents=True,exist_ok=True)
p=subprocess.Popen(['k3s','kubectl','-n',cfg['namespace'],'logs',cfg['pod'],'-c',cfg.get('container','aggregator'),'--follow','--since=60s'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
buffer=b'';written=0;deadline=time.monotonic()+420
try:
 while time.monotonic()<deadline and not stop.exists():
  if not select.select([p.stdout],[],[],1)[0]:continue
  block=os.read(p.stdout.fileno(),65536)
  if not block:break
  buffer+=block
  while b'\n' in buffer:
   line,buffer=buffer.split(b'\n',1)
   try:entry=json.loads(line)
   except ValueError:continue
   if not (entry.get('level') in ('WARN','ERROR') or entry.get('msg') in ('aggregator_started','http_server_started') or
      entry.get('msg')=='http_request' and entry.get('status',0)>=500):continue
   if written+len(line)>10*1024*1024:
    if written<=10*1024*1024:
     print(json.dumps({'msg':'capture_truncated','limit_bytes':10*1024*1024}),flush=True);written=10*1024*1024+1
    continue
   print(line.decode('utf-8',errors='replace'),flush=True);written+=len(line)+1
  if len(buffer)>1024*1024:buffer=b''
finally:
 p.terminate()
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:p.kill();p.wait()
'''


class PodLogCapture:
    def __init__(self, hosts, node, namespace, directory):
        self.hosts, self.node, self.namespace = hosts, node, namespace
        self.directory = Path(directory) / 'pod-logs'
        self.directory.mkdir(parents=True)
        self.stop = '/opt/aat/repo/.e2e/' + Path(directory).name + '/log-capture.stop'
        self.streams = {}

    def observe(self, pods):
        for pod in pods:
            name = pod['metadata']['name']
            if name in self.streams or len(self.streams) >= 24:
                continue
            container=next(c['name'] for c in pod['spec']['containers'] if c['name'] in ('aggregator','aggregator-gateway'))
            runtime = next((c for c in pod.get('status', {}).get('containerStatuses', [])
                            if c['name'] == container), {})
            if not runtime.get('containerID'):
                continue  # retry when the pending container starts
            config = dict(namespace=self.namespace, pod=name, stop=self.stop,container=container)
            log = (self.directory / (name + '.jsonl')).open('wb')
            command = 'sudo -n python3 -u -c ' + shlex.quote(REMOTE) + ' ' + shlex.quote(json.dumps(config))
            process = subprocess.Popen(self.hosts.command(self.node, command), stdout=log, stderr=subprocess.STDOUT)
            self.streams[name] = (process, log)
            (self.directory / (name + '.metadata.json')).write_text(json.dumps(dict(
                node=pod['spec'].get('nodeName'), image=pod['spec']['containers'][0]['image'],
                image_id=runtime.get('imageID')), indent=2))

    def close(self):
        if not self.streams:
            return
        try:
            self.hosts.ssh(self.node, 'sudo -n touch ' + shlex.quote(self.stop))
        finally:
            for process, log in self.streams.values():
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                finally:
                    log.close()
