from pathlib import Path
import json
import select
import shlex
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run


SERVER = r'''import json,socket,sys,time
ip,peer,token=sys.argv[1:]
with socket.socket() as listener:
 listener.settimeout(15);listener.bind((ip,0));listener.listen(1)
 print(json.dumps({'port':listener.getsockname()[1]}),flush=True)
 with listener.accept()[0] as connection:
  connection.settimeout(3)
  if connection.getpeername()[0]!=peer: raise RuntimeError('Unexpected peer')
  received=b''
  while len(received)<len(token):
   chunk=connection.recv(len(token)-len(received))
   if not chunk: raise RuntimeError('Campaign connection closed early')
   received+=chunk
  if received.decode()!=token: raise RuntimeError('Unexpected campaign token')
  payload=b'x'*65536;started=time.monotonic();sent=0
  while time.monotonic()-started<8:
   connection.sendall(payload);sent+=len(payload)
  print(json.dumps({'sent_bytes':sent,'seconds':time.monotonic()-started}),flush=True)
'''

CLIENT = r'''import json,socket,sys,time
ip,port,token=sys.argv[1:]
with socket.create_connection((ip,int(port)),timeout=5) as connection:
 connection.settimeout(12);connection.sendall(token.encode());started=time.monotonic();total=0
 while True:
  chunk=connection.recv(262144)
  if not chunk: break
  total+=len(chunk)
 seconds=time.monotonic()-started
 print(json.dumps({'received_bytes':total,'seconds':seconds,'mbps':total*8/seconds/1000000}))
'''


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Transport diagnosis requires the configured VPS lab')
    node=h.hosts.data['load_generator']
    active=h.hosts.ssh(node,'pgrep -x k6 || true',capture=True).decode().strip()
    h.check(not active, 'No concurrent generator load during the network measurement')
    results=[]
    for ordinal in range(h.args.repeat):
        command='timeout 25 python3 -u -c '+shlex.quote(SERVER)+' '+shlex.join([
            h.server['private_ip'],node['private_ip'],h.run_id])
        with (h.folder/f'round-{ordinal+1}-server.log').open('w') as log:
            server=subprocess.Popen(h.hosts.command(h.server,command,multiplex=False),
                                    stdout=subprocess.PIPE,stderr=log,text=True)
            try:
                h.check(bool(select.select([server.stdout],[],[],20)[0]), 'Private-only test listener announced its ephemeral port')
                line=server.stdout.readline()
                h.check(bool(line), 'Listener started without opening a public address')
                port=json.loads(line)['port']
                raw=h.hosts.ssh(node,'timeout 20 python3 -c '+shlex.quote(CLIENT)+' '+shlex.join([
                    h.server['private_ip'],str(port),h.run_id]),capture=True)
                result=json.loads(raw)
                server.wait(timeout=10)
                h.check(server.returncode==0,'Bounded private listener closed normally')
                results.append(result);h.artifact('measurements.json',results);h.note({'round':ordinal+1,**result})
            finally:
                if server.poll() is None:
                    server.terminate();server.wait(timeout=10)
        time.sleep(2)
    h.note({'scope':'Single TCP flow over existing WireGuard, no k6 or application payload; not an ISP cap or a P2 pass'})


if __name__=='__main__':
    raise SystemExit(run('p2/02-network',['Transport diagnosis only'],parser('Measure the private data path separately from application throughput'),scenario))
