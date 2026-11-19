"""Opt-in CPU profiles via authenticated pod exec, never a public debug port."""
import json
from pathlib import Path
import shlex
import subprocess


REMOTE = r'''
import concurrent.futures,json,subprocess,sys,time
cfg=json.loads(sys.argv[1]);base=['k3s','kubectl','--request-timeout=40s','-n',cfg['namespace'],'exec',cfg['pod'],'-c',cfg['container'],'--','wget','-q','-O','-']
def request(path):return subprocess.run(base+['http://127.0.0.1:6060/'+path],capture_output=True,timeout=38,check=True).stdout
samples=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
 samples.append(json.loads(request('runtime')))
 future=pool.submit(request,'cpu?seconds=30')
 for _ in range(10):
  time.sleep(3);samples.append(json.loads(request('runtime')))
 profile=future.result()
samples.append(json.loads(request('runtime')))
import base64
print(json.dumps(dict(samples=samples,cpu_profile_base64=base64.b64encode(profile).decode())),flush=True)
'''


class PodProfiles:
    def __init__(self, hosts, node, namespace, directory):
        self.hosts, self.node, self.namespace = hosts, node, namespace
        self.directory = Path(directory) / 'cpu-profiles'
        self.streams = []

    def start(self, pods):
        self.directory.mkdir(exist_ok=True)
        for pod in pods:
            if pod['metadata'].get('deletionTimestamp'):
                continue
            name = pod['metadata']['name']
            container = next(c['name'] for c in pod['spec']['containers'] if c['name'] in ('aggregator', 'aggregator-gateway'))
            cfg = dict(namespace=self.namespace, pod=name, container=container)
            path = self.directory / (name + '.capture.json')
            out = path.open('wb')
            command = 'sudo -n python3 -u -c ' + shlex.quote(REMOTE) + ' ' + shlex.quote(json.dumps(cfg))
            process = subprocess.Popen(self.hosts.command(self.node, command), stdout=out,
                                       stderr=subprocess.STDOUT)
            self.streams.append((name, process, out, path))
            (self.directory / (name + '.metadata.json')).write_text(json.dumps(dict(
                node=pod['spec'].get('nodeName'), containers=pod['status'].get('containerStatuses', [])), indent=2))

    def close(self):
        import base64
        for name, process, out, path in self.streams:
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait()
            finally:
                out.close()
            if process.returncode == 0:
                try:
                    capture = json.loads(path.read_text())
                    (self.directory / (name + '.cpu.pprof')).write_bytes(base64.b64decode(capture.pop('cpu_profile_base64'), validate=True))
                    (self.directory / (name + '.runtime.json')).write_text(json.dumps(capture, indent=2))
                except (ValueError, KeyError) as err:
                    print('CPU profile capture invalid:', name, type(err).__name__, flush=True)
            else:
                print('CPU profile capture failed:', name, process.returncode, flush=True)
