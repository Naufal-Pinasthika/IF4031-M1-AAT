from pathlib import Path
import base64
import importlib.util
import json
import os
import select
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import ROOT, parser, run

spec = importlib.util.spec_from_file_location('capacity', Path(__file__).with_name('01-capacity.py'))
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def scenario(h):
    h.changes()
    h.check(h.args.repeat == 1, 'Run one bounded calibration per invocation; rerun for an independent snapshot')
    h.check(h.args.vps, 'Generator calibration uses the external VPS')
    go = Path('/mnt/c/Program Files/Go/bin/go.exe')
    h.check(go.is_file(), 'Windows Go is available for a Linux fixture cross-build')
    binary = h.folder / 'fixture-linux'
    windows = lambda path: subprocess.check_output(['wslpath', '-w', str(path)], text=True).strip()
    subprocess.run([str(go), 'build', '-o', windows(binary), windows(ROOT/'test/lib/response-fixture/main.go')],
        env={**os.environ, 'GOOS': 'linux', 'GOARCH': 'amd64', 'CGO_ENABLED': '0',
             'WSLENV': os.environ.get('WSLENV', '')+':GOOS:GOARCH:CGO_ENABLED'}, check=True)
    h.check(binary.read_bytes()[:4] == b'\x7fELF', 'Fixture cross-build produced a Linux ELF binary')
    code, page, _ = h.request('aggregator', '/internal/events?limit=20',
        {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']})
    h.check(code == 200 and page.get('partial') is False and len(page.get('events', [])) == 20,
        'Calibration snapshots a complete real 20-event response')
    body = (h.folder / h.report['requests'][-1]['response_file']).read_bytes()
    folder = '.local/state/aat-load/' + h.run_id
    upload = 'import pathlib,sys,os;os.umask(0o077);p=pathlib.Path.home()/'+repr(folder)+';p.mkdir(parents=True,exist_ok=False);f=p/"fixture";f.write_bytes(sys.stdin.buffer.read());f.chmod(0o700)'
    h.hosts.ssh(h.server, 'python3 -c '+shlex.quote(upload), binary.read_bytes(), capture=True)
    fixture = subprocess.Popen(h.hosts.command(h.server, 'exec ~/'+folder+'/fixture', multiplex=False),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    identity = None
    try:
        fixture.stdin.write(json.dumps({'address': h.server['private_ip'], 'key': h.values['AGGREGATOR_API_KEY'],
            'body': base64.b64encode(body).decode()}).encode()+b'\n')
        fixture.stdin.close()
        h.check(bool(select.select([fixture.stdout], [], [], 15)[0]), 'Private calibration listener started')
        line = fixture.stdout.readline()
        if not line:
            h.artifact('fixture-error.json', {'stderr': fixture.stderr.read().decode(errors='replace')})
        h.check(bool(line), 'Fixture returned its private listening address')
        identity = json.loads(line)
        started = h.hosts.ssh(h.server, 'python3 -c '+shlex.quote(
            'from pathlib import Path;print(Path("/proc/'+str(identity['pid'])+'/stat").read_text().split()[21])'), capture=True).decode().strip()
        identity['started'] = started
        h.artifact('calibration.json', {'body_bytes': len(body), 'listener': identity['address'],
            'scope': 'Static response fixture: diagnostic only, never application throughput or P2 acceptance'})
        result = capacity.remote_generator(h, 'calibration', 'scripts/m2/p2/throughput.js',
            {'VUS': str(h.args.vus), 'DURATION': '60s', 'AGGREGATOR_BASE_URL': 'http://'+identity['address']}, None)
        h.artifact('diagnostic.json', result)
        h.check(result['exit_code'] == 0 and result['valid_timing'], 'Calibration finished with the original full-body validation')
    finally:
        if identity is not None and 'started' in identity:
            cleanup = 'from pathlib import Path;import os,signal;p=Path("/proc/'+str(identity['pid'])+'");' \
                'same=p.exists() and (p/"stat").read_text().split()[21]=='+repr(identity['started'])+';' \
                'os.kill('+str(identity['pid'])+',signal.SIGTERM) if same else None'
            h.hosts.ssh(h.server, 'python3 -c '+shlex.quote(cleanup), capture=True)
        if fixture.poll() is None:
            fixture.terminate()
        fixture.wait(timeout=10)


if __name__ == '__main__':
    p = parser('Calibrate the generator with a real response snapshot, outside all application services; not a P2 pass')
    p.add_argument('--vus', type=int, default=1000)
    p.add_argument('--max-rss-mib', type=int, default=6144)
    p.add_argument('--min-free-mib', type=int, default=512)
    p.add_argument('--generator-procs', type=int, default=8)
    p.add_argument('--generator-gogc', type=int, default=300)
    p.set_defaults(seconds=60, repeat=1)
    raise SystemExit(run('p2/04-generator', ['Generator diagnostic only'], p, scenario))
