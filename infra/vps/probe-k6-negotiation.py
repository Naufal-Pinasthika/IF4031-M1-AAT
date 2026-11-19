#!/usr/bin/env python3
"""One local stock-k6 request: inspect encoding negotiation, never contact VPS."""
from http.server import BaseHTTPRequestHandler,HTTPServer
import json
import subprocess
import threading
from provision import ROOT


def main():
    observed={}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            observed['accept_encoding']=self.headers.get('Accept-Encoding')
            self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
        def log_message(self,*args):pass
    with HTTPServer(('127.0.0.1',0),Handler) as server:
        server.timeout=10
        worker=threading.Thread(target=server.handle_request);worker.start()
        script="import http from 'k6/http'; export default function(){http.get('http://127.0.0.1:"+str(server.server_port)+"/');}"
        result=subprocess.run(['/mnt/d/aat-m2-e2e/k6/k6-v2.4.0-linux-amd64/k6','run','--vus','1','--iterations','1','--no-usage-report','-'],input=script,capture_output=True,text=True,timeout=15)
        worker.join(timeout=11)
        observed.update(k6_exit_code=result.returncode,scope='Exactly one loopback request, no application/VPS workload or credential.')
        path=ROOT/'.e2e/k6-negotiation.json';path.write_text(json.dumps(observed,indent=2))
        print(json.dumps(observed))
        if result.returncode:raise RuntimeError(result.stderr[-1000:])


if __name__=='__main__':main()
