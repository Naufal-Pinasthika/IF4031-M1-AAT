"""Read-only log trace explorer. Metrics come from Prometheus, not log sampling.

No Secret access, pod exec, database connection, or source manipulation. Log
coverage is bounded and explicitly reported; unavailable steps are never invented.
"""
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import ssl
import urllib.parse
import urllib.request

NS=os.environ.get('POD_NAMESPACE','sistem-informasi-kebencanaan-bnpb')
SA=Path('/var/run/secrets/kubernetes.io/serviceaccount')
CONTEXT=ssl.create_default_context(cafile=str(SA/'ca.crt')) if SA.exists() else None
PORTS={'aggregator':8083,'aggregator-gateway':8083,'auth':8084,'client-api':8080,'bmkg':8081,'pvmbg':8082,'dashboard-consumer':8085,'notifier-consumer':8086,'sensor-pantai':8080}
ALLOWED={'time','timestamp','level','msg','service','correlation_id','method','path','status','latency_ms',
         'pipeline_latency_ms','ingress_latency_ms','duration_ms','hazard_id','source','storage_unit','operation',
         'partition','offset','topic','message_id','error','delivered','applied','events','invalid','sql_operation'}


def scrub(value):
    if isinstance(value,str):
        value=re.sub(r'(?i)bearer\s+\S+','Bearer [redacted]',value)
        value=re.sub(r'postgres(?:ql)?://[^\s]+','[redacted database URL]',value)
        value=re.sub(r'(?i)(password|token|secret|authorization)[=:]\s*[^\s,;]+',r'\1=[redacted]',value)
        return value[:1024]
    return value


def fetch(url,headers=None,api=False):
    if api:
        headers=dict(headers or {},Authorization='Bearer '+(SA/'token').read_text().strip())
    with urllib.request.urlopen(urllib.request.Request(url,headers=headers or {}),timeout=3,context=CONTEXT if api else None) as response:
        data=response.read(4<<20)
        return data


def kube(path):
    return fetch('https://kubernetes.default.svc/api/v1/namespaces/'+NS+'/'+path,api=True)


def pods():return json.loads(kube('pods'))['items']


def trace(correlation):
    if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}',correlation):raise ValueError('invalid correlation ID')
    found=[];coverage=[]
    def read(pod):
        app=pod['metadata'].get('labels',{}).get('app','')
        if app not in PORTS:return [],None
        name=pod['metadata']['name']; records=[]
        try:
            raw=kube('pods/'+name+'/log?sinceSeconds=900&tailLines=5000&limitBytes=4194304').decode(errors='replace')
            for line in raw.splitlines():
                try:record=json.loads(line)
                except ValueError:continue
                if record.get('correlation_id')!=correlation:continue
                record={k:scrub(v) for k,v in record.items() if k in ALLOWED and isinstance(v,(str,int,float,bool,type(None)))}
                record.update(service=app,pod=name,node=pod['spec'].get('nodeName'))
                records.append(record)
            return records,{'pod':name,'service':app,'read':True,'bounded':True,'lines':len(raw.splitlines())}
        except Exception as exc:return [],{'pod':name,'service':app,'read':False,'error':type(exc).__name__}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for records,item in pool.map(read,pods()):
            found.extend(records)
            if item:coverage.append(item)
    found.sort(key=lambda r:r.get('time',r.get('timestamp','')))
    return {'correlation_id':correlation,'steps':found,'coverage':coverage,
            'scope':'Bounded correlation-log trace, last 15 minutes / 5000 lines per pod. Not full distributed-span sampling. Missing source/broker internal spans are explicit.',
            'complete':False}


def overview():
    rows=[]
    for p in pods():
        labels=p['metadata'].get('labels',{})
        rows.append({'pod':p['metadata']['name'],'service':labels.get('app',labels.get('component','unknown')),
                     'role':labels.get('role'),'node':p['spec'].get('nodeName'),'phase':p['status'].get('phase'),
                     'ready':any(x['type']=='Ready' and x['status']=='True' for x in p['status'].get('conditions',[])),
                     'restarts':sum(x['restartCount'] for x in p['status'].get('containerStatuses',[]))})
    return {'pods':rows,'as_of':dt.datetime.now(dt.timezone.utc).isoformat()}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=urllib.parse.urlsplit(self.path);query=urllib.parse.parse_qs(path.query)
        try:
            if path.path=='/health':return self.send(200,b'{"status":"ok"}')
            if path.path=='/':return self.send(200,Path('index.html').read_bytes(),'text/html; charset=utf-8')
            if path.path=='/api/overview':result=overview()
            elif path.path=='/api/trace':result=trace(query.get('id',[''])[0])
            elif path.path=='/api/query':
                expression=query.get('query',[''])[0]
                if len(expression)>2048:raise ValueError('query too long')
                return self.send(200,fetch('http://prometheus:9090/api/v1/query?'+urllib.parse.urlencode({'query':expression})))
            else:return self.send(404,b'{"error":"not found"}')
            self.send(200,json.dumps(result).encode())
        except ValueError:self.send(400,b'{"error":"invalid query"}')
        except Exception as exc:self.send(503,json.dumps({'error':type(exc).__name__}).encode())

    def send(self,status,body,ctype='application/json'):
        self.send_response(status);self.send_header('Content-Type',ctype)
        self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'")
        self.end_headers();self.wfile.write(body)

    def log_message(self,format,*args):pass


if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',8090),Handler).serve_forever()
