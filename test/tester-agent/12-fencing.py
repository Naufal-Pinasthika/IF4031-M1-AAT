from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
import select
import shlex
import subprocess
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run
from sandbox import Sandbox


@contextmanager
def continuous_field_reads(h):
    samples = []
    stop = threading.Event()
    started = time.monotonic()
    def observe():
        while not stop.is_set():
            try:
                code, page, seconds = h.api('/events?limit=20', 'FIELD')
                row = {'status': code, 'seconds': seconds,
                    'valid': code == 200 and isinstance(page.get('events'), list)}
            except Exception as error:
                row = {'valid': False, 'error': str(error)}
            samples.append({'at_seconds': time.monotonic()-started, **row})
            stop.wait(1)
    worker = threading.Thread(target=observe, daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=60)
        h.artifact('continuous-field-read-availability.json', samples)
    h.check(not worker.is_alive() and bool(samples) and all(row['valid'] for row in samples),
            'Continuous Field reads survive the entire freeze/skew window, including waits for takeover and process resume')


class FencingSandbox(Sandbox):
    def __enter__(self):
        super().__enter__()
        try:
            h = self.h
            live = json.loads(h.kube('get', 'daemonset/aggregator-gateway', '-o', 'json'))
            image = live['spec']['template']['spec']['containers'][0]['image']
            backend = self.resource('Service', self.name+'-backends', spec={'clusterIP': 'None',
                'selector': {'app': self.name+'-agg'}, 'ports': [{'port': 8083, 'targetPort': 8083}]})
            gateway, service = self.deployment(self.name+'-gateway', 'other', image, 8083,
                [{'name': 'AGGREGATOR_BACKEND_DNS', 'value': self.name+'-backends.'+NS+'.svc.cluster.local'},
                 {'name': 'GATEWAY_REPLICAS', 'value': str(len(h.hosts.nodes))}],
                command=['/app/gateway'], envFrom=[{'secretRef': {'name': self.name}}])
            gateway['kind'] = 'DaemonSet'
            gateway['spec'].pop('replicas')
            gateway['spec']['template']['spec']['containers'][0]['readinessProbe']['httpGet']['path'] = '/ready'
            service['spec']['trafficDistribution'] = 'PreferSameNode'
            self.apply([backend, gateway, service])
            h.kube('rollout', 'status', 'daemonset/'+self.name+'-gateway', '--timeout=180s')
            h.kube('patch', 'deployment/'+self.name+'-api', '--type=strategic', '-p', json.dumps({
                'spec': {'template': {'spec': {'containers': [{'name': 'app', 'env': [
                    {'name': 'AGGREGATOR_URL', 'value': 'http://'+self.name+'-gateway:8083'}]}]}}}}))
            h.kube('rollout', 'status', 'deployment/'+self.name+'-api', '--timeout=180s')
            self.tunnel.terminate()
            self.tunnel.wait(timeout=10)
            ips = {name: h.kube('get', 'service', self.name+'-'+name, '-o', 'jsonpath={.spec.clusterIP}').strip()
                   for name in ('gateway', 'api', 'source')}
            self.open_tunnel({'aggregator': ips['gateway']+':8083', 'client': ips['api']+':8080',
                              'fixture': ips['source']+':8080'})
            h.artifact('fencing-read-topology.json', {'gateway': 'per-node DaemonSet',
                'service_policy': 'PreferSameNode', 'reason': 'Mirror production L7 routing rather than bypassing it with a direct Aggregator Service'})
            return self
        except BaseException:
            from sandbox import sys_exc
            self.__exit__(*sys_exc())
            raise

    def configure(self, config):
        config['AGGREGATOR_POLL_INTERVAL_MS'] = '500'

    def configure_credentials(self, values):
        for key in ('P1_STORAGE_A_URL', 'P1_STORAGE_B_URL', 'P1_STORAGE_C_URL'):
            url = urllib.parse.urlsplit(values[key])
            query = dict(urllib.parse.parse_qsl(url.query))
            query.update(pool_max_conns='4', pool_min_conns='0')
            values[key] = urllib.parse.urlunsplit(url._replace(query=urllib.parse.urlencode(query)))


def lease(h):
    return h.sql("SELECT row_to_json(l) FROM source_leases l WHERE source='PVMBG'")


def admit(h):
    return h.sql("""SELECT COALESCE(json_agg(row_to_json(x)),'[]'::json) FROM (
 SELECT m.source_ref_id,m.received_at,m.correlation_id,p.holder_id,p.fencing_token
 FROM durable_inbox m JOIN poll_attempts p ON m.correlation_id='poll-PVMBG-'||p.poll_id
 WHERE m.source='PVMBG' AND m.kind='volcanic-report' ORDER BY m.ordinal) x""")


def append_reports(h, lab, reports, label):
    at = h.sql('SELECT to_json(clock_timestamp())')
    refs = []
    for i in range(3):
        ref = lab.name+'-'+label+'-'+str(i)
        refs.append(ref)
        reports.append({'report_id': ref, 'volcano_id': 'MERAPI', 'alert_level': 'Waspada',
            'eruption_count_24h': 1, 'ash_column_height_m': 100, 'reported_at': at})
    lab.control('/admin/reports', {'reports': reports})
    return refs


def freeze(h, lab, reports, number):
    old = lease(h)
    pod = json.loads(h.kube('get', 'pod', old['holder_id'], '-o', 'json'))
    h.check(pod['metadata']['labels'].get('aat-e2e') == lab.suffix, 'Only the run-owned polling process may be frozen')
    node = next(n for n in h.hosts.nodes if n['name'] == pod['spec']['nodeName'])
    container = pod['status']['containerStatuses'][0]['containerID'].split('://', 1)[1]
    inspect = json.loads(h.hosts.ssh(node, 'sudo -n k3s crictl inspect '+shlex.quote(container), capture=True))
    pid = int(inspect['info']['pid'])
    duration = int(h.args.freeze_seconds)
    script = '''import json,os,pathlib,signal,sys,time
pid=int(sys.argv[1]);duration=int(sys.argv[2]);p=pathlib.Path('/proc')/str(pid)
assert b'/app/aggregator' in (p/'cmdline').read_bytes()
started=(p/'stat').read_text().split()[21]
signal.signal(signal.SIGHUP,signal.SIG_IGN)
try:
 os.kill(pid,signal.SIGSTOP)
 print(json.dumps({'pid':pid,'paused':True,'duration':duration}),flush=True)
 time.sleep(duration)
finally:
 if p.exists() and (p/'stat').read_text().split()[21]==started:os.kill(pid,signal.SIGCONT)
print(json.dumps({'pid':pid,'continued':True}),flush=True)
'''
    proc = subprocess.Popen(h.hosts.command(node, 'sudo -n python3 -u -c '+shlex.quote(script)+' '+str(pid)+' '+str(duration),
                                            multiplex=False), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        h.check(bool(select.select([proc.stdout], [], [], 10)[0]), 'Freeze controller acknowledged the actual process')
        ack = json.loads(proc.stdout.readline())
        h.check(ack.get('paused') is True, 'All threads of the actual source owner are paused')
        refs = append_reports(h, lab, reports, 'freeze-'+str(number))
        observed = []
        deadline = time.monotonic()+duration-2
        replacement = None
        while time.monotonic() < deadline:
            current = lease(h)
            code, page, seconds = h.api('/events?limit=20', 'FIELD')
            observed.append({'status': code, 'seconds': seconds,
                'valid': code == 200 and isinstance(page.get('events'), list)})
            if current and current['holder_id'] != old['holder_id'] and current['fencing_token'] > old['fencing_token']:
                replacement = current
                break
            time.sleep(.3)
        h.check(replacement is not None and proc.poll() is None, 'A higher epoch took ownership while the old process was still frozen')
        takeover_at = h.sql('SELECT to_json(clock_timestamp())')
        h.wait(lambda: set(refs) <= {r['source_ref_id'] for r in admit(h)}, 'The replacement admits fresh source identities before the old process resumes')
        h.check(proc.poll() is None, 'Replacement admission completed inside the actual freeze window')
        stdout, stderr = proc.communicate(timeout=duration+10)
        h.check(proc.returncode == 0 and 'continued' in stdout, 'The planned freeze ended without restarting the old process')
        time.sleep(3)
        later = admit(h)
        stale = [r for r in later if r['holder_id'] == old['holder_id'] and r['fencing_token'] <= old['fencing_token'] and r['received_at'] >= takeover_at]
        h.artifact(f'freeze-{number}.json', {'old': old, 'replacement': replacement, 'takeover_observed_at': takeover_at,
            'controller': ack, 'controller_end': stdout, 'controller_stderr': stderr, 'reads': observed, 'admissions': later, 'stale_admissions': stale})
        h.check(not stale, 'The resumed old epoch admitted no source message after the replacement took ownership')
        current = json.loads(h.kube('get', 'pod', old['holder_id'], '-o', 'json'))
        h.check(current['metadata']['uid'] == pod['metadata']['uid'] and
                current['status']['containerStatuses'][0]['containerID'] == pod['status']['containerStatuses'][0]['containerID'],
                'The same old process resumed; a crash/restart did not substitute for the freeze test')
        h.note({'freeze_round': number, 'duration_seconds': duration, 'old_owner': old['holder_id'],
                'replacement': replacement['holder_id'], 'old_epoch': old['fencing_token'],
                'new_epoch': replacement['fencing_token'], 'stale_admissions': len(stale)})
        return observed
    finally:
        # The remote controller ignores SSH hangup and always resumes its exact
        # process after the planned duration, even if local observation fails.
        if proc.poll() is None:
            proc.communicate(timeout=duration+15)


def skew(h, lab, reports, number):
    owner = lease(h)
    pod = json.loads(h.kube('get', 'pod', owner['holder_id'], '-o', 'json'))
    node = next(n for n in h.hosts.nodes if n['name'] == pod['spec']['nodeName'])
    h.check(node['role'] == 'agent', 'Sustained clock skew targets a worker, never the singleton coordinator/control-plane')
    script = '''import json,signal,subprocess,time
signal.signal(signal.SIGHUP,signal.SIG_IGN)
origin=time.time()-time.monotonic()
ntp=subprocess.check_output(['timedatectl','show','-p','NTP','--value'],text=True).strip()
subprocess.run(['timedatectl','set-ntp','false'],check=True)
try:
 for offset in (45,-30):
  time.clock_settime(time.CLOCK_REALTIME,time.monotonic()+origin+offset)
  end=time.monotonic()+20
  while time.monotonic()<end:
   print(json.dumps({'realtime':time.time(),'monotonic':time.monotonic(),'origin':origin,'offset':offset}),flush=True);time.sleep(.1)
finally:
 time.clock_settime(time.CLOCK_REALTIME,time.monotonic()+origin)
 if ntp=='yes':subprocess.run(['timedatectl','set-ntp','true'],check=True)
print(json.dumps({'restored':True}),flush=True)
'''
    began = h.sql('SELECT to_json(clock_timestamp())')
    proc = subprocess.Popen(h.hosts.command(node, 'sudo -n python3 -u -c '+shlex.quote(script), multiplex=False),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    reads = []
    lines, reader = [], None
    try:
        h.check(bool(select.select([proc.stdout], [], [], 15)[0]), 'The clock fault is acknowledged before checking ingestion')
        first = proc.stdout.readline()
        lines.append(first)
        reader = threading.Thread(target=lambda: lines.extend(proc.stdout.readlines()), daemon=True)
        reader.start()
        append_reports(h, lab, reports, 'skew-'+str(number))
        while proc.poll() is None:
            code, page, seconds = h.api('/events?limit=20', 'FIELD')
            reads.append({'status': code, 'seconds': seconds,
                'valid': code == 200 and isinstance(page.get('events'), list)})
            time.sleep(.5)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.wait(timeout=60)
        if reader:
            reader.join(timeout=10)
    stderr = proc.stderr.read()
    trace = [json.loads(row) for row in lines]
    samples = [r for r in trace if 'offset' in r]
    h.artifact(f'skew-{number}.json', {'node': node['name'], 'owner': owner, 'clock_samples': trace,
        'reads': reads, 'stderr': stderr, 'admissions': admit(h)})
    h.check(proc.returncode == 0 and trace[-1].get('restored'), 'The planned clock fault restored original wall clock and NTP configuration')
    for offset in (45, -30):
        phase = [r for r in samples if r['offset'] == offset]
        h.check(len(phase) >= 100 and phase[-1]['monotonic']-phase[0]['monotonic'] >= 18,
                'Each positive/negative clock offset persisted for at least 18 independently sampled seconds')
        h.check(all(abs(r['realtime']-r['monotonic']-r['origin']-offset)<.5 for r in phase),
                'Actual realtime versus monotonic samples match the declared sustained offset')
    rows = h.sql("SELECT COALESCE(json_agg(row_to_json(p) ORDER BY poll_id),'[]'::json) FROM poll_attempts p WHERE source='PVMBG' AND started_at>='"+began+"'")
    overlap = [(a,b) for i,a in enumerate(rows) for b in rows[i+1:] if a['status'] == b['status'] == 'completed'
               and a['holder_id'] != b['holder_id'] and a['finished_at'] and b['started_at'] < a['finished_at']]
    h.check(bool(rows) and any(p['status']=='completed' for p in rows) and not overlap,
            'Polling continues under sustained skew without overlapping completed owners')
    h.note({'skew_round': number, 'node': node['name'], 'offsets': [45,-30], 'samples': len(samples), 'overlapping_completed_owners': len(overlap)})
    return reads


def scenario(h):
    h.changes()
    with FencingSandbox(h) as lab, continuous_field_reads(h):
        reports = []
        reads = []
        ttl = int(json.loads(h.kube('get', 'configmap', lab.name, '-o', 'json'))['data']['AGGREGATOR_LEASE_TTL_MS'])/1000
        h.check(h.args.freeze_seconds > ttl+5, 'The real process freeze exceeds the configured source lease lifetime')
        initial = append_reports(h, lab, reports, 'prime')
        h.wait(lambda: set(initial) <= {r['source_ref_id'] for r in admit(h)}, 'The original owner can admit source data before fault injection')
        for number in range(1, h.args.repeat+1):
            reads.extend(freeze(h, lab, reports, number))
        owner = lease(h)
        pod = json.loads(h.kube('get', 'pod', owner['holder_id'], '-o', 'json'))
        if pod['spec']['nodeName'] == h.server['name']:
            reads.extend(freeze(h, lab, reports, 'worker-handoff'))
        for number in range(1, h.args.repeat+1):
            reads.extend(skew(h, lab, reports, number))
        actual = h.complete_audit('PVMBG')
        expected = {r['report_id'] for r in reports}
        h.check({e['source_ref_id'] for e in actual} == expected and len(actual) == len(expected),
                'Every source identity is present exactly once after all freeze/skew faults')
        requests = lab.control('/admin/status')['data_requests']
        counts = Counter((r['poll_id'],r['path']) for r in requests)
        h.check(all(r['poll_id'] and r['holder'] and r['epoch'] for r in requests) and all(n==1 for n in counts.values()),
                'Independent upstream request records retain poll/holder/epoch and show no repeated poll endpoint')
        target = '/dev/shm/'+lab.name+'-fence.test'
        base = ['sudo', '-n', 'k3s', 'kubectl', '-n', NS, 'exec', '-i', 'aggregator-coordinator-0', '--', 'sh', '-ec']
        def remote(script, **kwargs):
            return subprocess.run(h.hosts.command(h.server, shlex.join(base+[script]), multiplex=False),
                                  timeout=90, **kwargs)
        try:
            remote('umask 077; dd of='+target+' bs=1048576 status=none; chmod 700 '+target,
                   input=h.args.store_test_binary.read_bytes(), check=True, capture_output=True)
            pattern = 'TestPostgres(RejectsFrozenOwnerBeforeAnyShardSeesNewToken|RenewalCannotReviveExpiredEpoch|PollGateHasOneWinnerAndFencesStaleRequests)'
            result = remote('P2_TEST_DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:5432/'+lab.database+'" '+
                target+' -test.v -test.count=3 -test.timeout=60s -test.run='+shlex.quote(pattern), capture_output=True)
            (h.folder/'storage-fence-regressions.log').write_bytes(result.stdout+result.stderr)
            h.check(result.returncode == 0 and b'--- PASS:' in result.stdout and b'SKIP' not in result.stdout,
                    'Actual stale admission, renewal and HTTP guard primitives reject obsolete epochs in isolated PostgreSQL schemas')
        finally:
            remote('rm -f -- '+target, capture_output=True)
        h.artifact('field-read-availability.json', reads)
        h.check(all(r['valid'] for r in reads), 'Every observed Field read survives freeze and sustained skew; fencing success cannot hide an availability failure')


if __name__ == '__main__':
    p = parser('Exercise real source-owner process freeze and sustained worker clock skew against isolated VPS state')
    p.add_argument('--freeze-seconds', type=int, default=30)
    p.add_argument('--store-test-binary', type=Path, required=True, help='Fresh Linux/amd64 Go test binary for services/aggregator/internal/store')
    raise SystemExit(run('tester-agent/12-fencing', ['P2.4 sustained worker skew', 'P2.5 real polling-process freeze'], p, scenario))
