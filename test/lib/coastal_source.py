import base64
import datetime as dt
import http.server
import json
import os
import socket
import threading
import time
import urllib.parse

lock = threading.Lock()
token = os.environ['SOURCE_TOKEN']
station = {'station_id': 'contract-station', 'name': 'Contract Test', 'province': 'Test', 'latitude': 0, 'longitude': 0}
state = {'items': [], 'available': {}, 'requests': [], 'mode': 'normal', 'origin': '2001-01-01T00:00:00Z', 'started': time.monotonic()}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def reply(self, code, body):
        payload = json.dumps(body).encode()
        try:
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        if self.headers.get('Authorization') != 'Bearer ' + token:
            return self.reply(401, {})
        size = int(self.headers.get('Content-Length', '0'))
        if size > 4 << 20:
            return self.reply(413, {})
        data = json.loads(self.rfile.read(size))
        with lock:
            if self.path == '/admin/config':
                state.update(mode=data['mode'])
                if 'origin' in data:
                    state.update(origin=data['origin'], started=time.monotonic())
            elif self.path == '/admin/append':
                now = time.time()
                state['items'].extend(data['items'])
                for item in data['items']:
                    state['available'].setdefault(item.get('observation_id', ''), now)
            else:
                return self.reply(404, {})
        self.reply(200, {'ok': True})

    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        if url.path in ('/health', '/healthz'):
            return self.reply(200, {'status': 'ok'})
        if url.path in ('/seismic-events', '/tsunami-warnings', '/volcanic-reports'):
            return self.reply(200, [])
        if self.headers.get('Authorization') != 'Bearer ' + token:
            return self.reply(401, {})
        if url.path == '/v1/stations':
            return self.reply(200, {'items': [station]})
        if url.path == '/admin/status':
            with lock:
                body = {'mode': state['mode'], 'requests': list(state['requests']), 'available': dict(state['available']), 'items': len(state['items'])}
            return self.reply(200, body)
        if url.path != '/v1/observations':
            return self.reply(404, {})
        query = urllib.parse.parse_qs(url.query)
        with lock:
            mode = state['mode']
            state['requests'].append({'at': time.time(), 'cursor': query.get('cursor', [''])[0], 'mode': mode})
            origin, started = state['origin'], state['started']
            items = list(state['items'])
        if mode == 'timeout':
            time.sleep(2)
            return self.reply(503, {})
        if mode in ('reset', 'oom'):
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if mode == 'malformed':
            return self.reply(200, {'items': 'not-an-array'})
        if mode == 'unavailable':
            return self.reply(503, {'error': {'code': 'unavailable', 'message': 'controlled outage'}})
        raw_cursor = query.get('cursor', [''])[0]
        try:
            offset = json.loads(base64.urlsafe_b64decode(raw_cursor))['offset'] if raw_cursor else 0
        except (ValueError, KeyError):
            return self.reply(400, {})
        limit = min(1000, max(1, int(query.get('limit', ['100'])[0])))
        page = items[offset:offset + limit]
        next_cursor = base64.urlsafe_b64encode(json.dumps({'offset': offset + len(page)}).encode()).decode()
        clock = dt.datetime.fromisoformat(origin.replace('Z', '+00:00'))
        if mode != 'stalled':
            clock += dt.timedelta(seconds=time.monotonic() - started)
        self.reply(200, {'generated_at': clock.isoformat().replace('+00:00', 'Z'), 'items': page, 'next_cursor': next_cursor})


http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
