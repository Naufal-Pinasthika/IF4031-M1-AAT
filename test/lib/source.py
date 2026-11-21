import base64
import http.server
import json
import os
import threading
import time
import urllib.parse

lock = threading.Condition()
state = {'events': [], 'reports': [], 'raw': None, 'mode': 'legacy', 'cut_every': 0,
         'connections': [], 'ended_at': None, 'delay': .001, 'generation': 0,
         'started_at': 0, 'prefix': 0}
token = os.environ['SOURCE_TOKEN']


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def respond(self, code, body):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        if self.headers.get('Authorization') != 'Bearer ' + token:
            return self.respond(401, {'error': 'unauthorized'})
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            parts, size = [], 0
            while True:
                length = int(self.rfile.readline().split(b';')[0], 16)
                if not length:
                    self.rfile.readline()
                    break
                size += length
                if size > 16 << 20:
                    return self.respond(413, {'error': 'fixture_too_large'})
                parts.append(self.rfile.read(length))
                self.rfile.read(2)
            raw = b''.join(parts)
        else:
            raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        try:
            data = json.loads(raw)
        except ValueError:
            return self.respond(400, {'error': 'invalid_fixture_json', 'bytes_received': len(raw),
                                     'content_length': self.headers.get('Content-Length'),
                                     'transfer_encoding': self.headers.get('Transfer-Encoding')})
        with lock:
            if self.path == '/admin/feed':
                previous = state['events']
                prefix = 0
                for old, new in zip(previous, data['events']):
                    if old != new:
                        break
                    prefix += 1
                delay, now = data.get('delay', .001), time.time()
                state.update(events=data['events'], cut_every=data.get('cut_every', 0),
                             delay=delay, started_at=now, prefix=prefix,
                             ended_at=now + max(0, len(data['events']) - prefix - 1) * delay,
                             connections=[])
                state['generation'] += 1
            elif self.path == '/admin/reports':
                state.update(reports=data.get('reports', []), raw=data.get('raw'), mode=data.get('mode', 'legacy'))
            else:
                return self.respond(404, {'error': 'not_found'})
            lock.notify_all()
        self.respond(200, {'accepted': True})

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path)
        if path.path == '/health':
            return self.respond(200, {'status': 'ok'})
        if self.headers.get('Authorization') != 'Bearer ' + token and path.path not in ('/seismic-events', '/tsunami-warnings'):
            return self.respond(401, {'error': 'unauthorized'})
        if path.path in ('/v1/session', '/admin/status'):
            with lock:
                now = time.time()
                available = len(state['events']) if not state['delay'] else min(len(state['events']),
                            state['prefix'] + max(0, int((now - state['started_at']) / state['delay']) + 1))
                result = {'profile': 'controlled-e2e', 'speed': 60, 'latest_offset': available - 1,
                          'ended_at': state['ended_at'] if state['ended_at'] is not None and now >= state['ended_at'] else None,
                          'observed_at': now, 'burst_started_at': state['started_at'], 'prefix': state['prefix'],
                          'interval_seconds': state['delay'], 'connections': list(state['connections'])}
            return self.respond(200, result)
        if path.path == '/volcanic-reports':
            with lock:
                raw, reports, mode = state['raw'], list(state['reports']), state['mode']
            if raw is not None:
                payload = base64.b64decode(raw)
            else:
                result = []
                for report in reports:
                    report = dict(report)
                    if mode == 'rename':
                        report['report_ref'] = report.pop('report_id')
                    elif mode == 'type-change':
                        report['eruption_count_24h'] = str(report['eruption_count_24h'])
                    result.append(report)
                payload = json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            return self.wfile.write(payload)
        if path.path != '/v1/stream':
            return self.respond(200, [])
        offset = int(urllib.parse.parse_qs(path.query).get('from', ['0'])[0])
        with lock:
            generation = state['generation']
            connection = {'from': offset, 'opened_at': time.time(), 'last_sent': offset - 1,
                          'client_ip': self.client_address[0], 'closed_at': None}
            state['connections'].append(connection)
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Connection', 'close')
        self.end_headers()
        sent = 0
        self.close_connection = True
        try:
            while True:
                with lock:
                    if generation != state['generation']:
                        return
                    if offset >= len(state['events']):
                        lock.wait(timeout=1)
                        self.wfile.write(b': keepalive\n\n')
                        self.wfile.flush()
                        continue
                    event = state['events'][offset]
                    cut, delay = state['cut_every'], state['delay']
                    ready_at = state['started_at'] + max(0, offset - state['prefix']) * delay
                    if time.time() < ready_at:
                        lock.wait(timeout=ready_at - time.time())
                        continue
                payload = {'offset': offset, **event['data']}
                frame = f'id: {offset}\nevent: {event["type"]}\ndata: {json.dumps(payload)}\n\n'.encode()
                self.wfile.write(frame)
                self.wfile.flush()
                connection['last_sent'] = offset
                offset += 1
                sent += 1
                if cut and sent >= cut:
                    return
        except (BrokenPipeError, ConnectionResetError):
            return
        finally:
            connection['closed_at'] = time.time()


http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
