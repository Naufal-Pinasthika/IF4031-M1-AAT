import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading


class Server(ThreadingHTTPServer):
    daemon_threads = True
    frozen = False
    calls = 0
    lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def reply(self, status, body, headers=()):
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in ('connection', 'transfer-encoding', 'content-length'):
                self.send_header(name, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_POST(self):
        if self.path != '/freeze':
            self.reply(404, b'{}')
            return
        with self.server.lock:
            self.server.frozen = True
        self.reply(200, b'{}')
        self.close_connection = True
        # Preserve accepted keep-alive connections but refuse new connections,
        # as a lost gateway does. Control traffic uses the other healthy fixture.
        def stop_listener():
            self.server.shutdown()
            self.server.server_close()
        threading.Thread(target=stop_listener, daemon=True).start()

    def do_GET(self):
        if self.path in ('/health', '/state'):
            self.reply(200, json.dumps({'calls': self.server.calls, 'frozen': self.server.frozen}).encode())
            return
        with self.server.lock:
            self.server.calls += 1
            frozen = self.server.frozen
        if frozen:
            self.connection.settimeout(15)
            try:
                self.connection.recv(1)
            except OSError:
                pass
            self.close_connection = True
            return
        upstream = http.client.HTTPConnection('aggregator', 8083, timeout=10)
        try:
            upstream.request('GET', self.path, headers={k: v for k, v in self.headers.items()
                                                       if k.lower() not in ('host', 'connection')})
            response = upstream.getresponse()
            self.reply(response.status, response.read(), response.getheaders())
        finally:
            upstream.close()


server = Server(('0.0.0.0', 8083), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
threading.Event().wait()
