from pathlib import Path
import concurrent.futures
import copy
import csv
import datetime as dt
import hashlib
import io
import json
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Harness, Inconclusive, NS, parser, run


class ReadObserver:
    def __init__(self, parent):
        args = copy.copy(parent.args)
        args.output = parent.folder / 'observer'
        self.h = Harness('p1/live-observer', ['P1.4', 'P1.7'], args)
        self.phase = 'healthy'
        self.stop_event, self.first_round = threading.Event(), threading.Event()
        self.records, self.failures, self.partial_endpoints = [], [], set()
        self.thread = None
        self.closed = False

    def start(self):
        self.h.open()
        self.thread = threading.Thread(target=self.observe, name='p1-read-observer')
        self.thread.start()
        if not self.first_round.wait(75):
            raise RuntimeError('Independent read observer did not complete its healthy baseline')
        self.h.check(not self.failures, 'All three read endpoints are successful before fault injection')

    def observe(self):
        h = self.h
        targets = [('aggregator', '/internal/events?limit=50', {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']}),
                   ('aggregator', '/internal/audit/events?limit=50', {'Authorization': 'Bearer ' + h.values['P1_AUDIT_TOKEN']}),
                   ('client', '/analytics', None)]
        while not self.stop_event.is_set():
            for service, endpoint, headers in targets:
                item = {'phase': self.phase, 'endpoint': endpoint, 'started_at': dt.datetime.now(dt.timezone.utc).isoformat()}
                try:
                    code, body, elapsed = h.api(endpoint) if service == 'client' else h.request(service, endpoint, headers)
                    item.update(status=code, elapsed=elapsed, partial=body.get('partial'),
                                unavailable_units=body.get('unavailable_units', []),
                                correlation_id=h.report['requests'][-1]['correlation_id'])
                    if code != 200 or elapsed >= 5 or not isinstance(body.get('partial'), bool) or body['partial'] and not body.get('unavailable_units'):
                        self.failures.append(dict(item))
                    if body.get('partial'):
                        self.partial_endpoints.add(endpoint)
                except Exception as error:
                    item['error'] = str(error)
                    self.failures.append(dict(item))
                item['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
                self.records.append(item)
                h.artifact('traffic.json', self.records)
                h.artifact('api-failures.json', self.failures)
            self.first_round.set()
            self.stop_event.wait(1)

    def stop(self):
        if self.closed:
            return
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=85)
            if self.thread.is_alive():
                raise RuntimeError('Observer did not terminate within its bounded HTTP request budget')
            self.thread = None
        self.h.report['status'] = 'failed' if self.failures else 'passed'
        if not self.records or any(not item['passed'] for item in self.h.report['checks']):
            self.h.report['status'] = 'failed'
        self.h.close()
        self.closed = True


def persist_ledger(h, ledger):
    with tempfile.TemporaryDirectory(prefix='aat-p1-ledger-') as directory:
        path = Path(directory) / 'physical.sqlite'
        with sqlite3.connect(path) as evidence:
            ledger.backup(evidence)
        shutil.copyfile(path, h.folder / 'physical.sqlite')


def scenario(h):
    h.check(shutil.disk_usage(h.folder).free >= 4 * 1024**3,
            'Evidence storage has at least 4 GiB free before any live-volume action or manifest reconstruction')
    if h.args.verify_run:
        return verify_previous(h)
    h.changes()
    h.check(bool(h.hosts), 'Live disk-loss test requires VPS inventory')
    h.check(h.args.repeat == 1, 'One explicitly selected live volume is erased per invocation')
    h.check(h.args.claim in {'data-postgres-' + c + '-0' for c in 'abc'}, 'Target is exactly one canonical claim')
    name = h.args.claim.removeprefix('data-').removesuffix('-0')
    pod_name = name + '-0'
    pod = json.loads(h.kube('get', 'pod', pod_name, '-o', 'json'))
    node = next(n for n in h.hosts.nodes if n['name'] == pod['spec']['nodeName'])
    h.check(node['role'] == 'agent', 'Never erase the control-plane canonical unit in this test')
    worker = node['name']
    h.check(worker == h.args.worker, 'Live pod placement matches the explicitly selected worker')
    all_pods = json.loads(h.kube('get', 'pods', '-o', 'json'))['items']
    stores = [p for p in all_pods if p['metadata']['labels'].get('bnpb.io/p1-storage') == 'true']
    h.check(len(stores) == 3 and len({p['spec']['nodeName'] for p in stores}) == 3,
            'Three canonical volumes are physically placed on different nodes')
    claims = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
    old = next(c for c in claims if c['metadata']['name'] == h.args.claim)
    volume = json.loads(h.kube('get', 'pv', old['spec']['volumeName'], '-o', 'json'))
    path = volume['spec'].get('local', volume['spec'].get('hostPath', {})).get('path', '')
    expected = '/var/lib/rancher/k3s/storage/' + volume['metadata']['name'] + '_' + NS + '_' + h.args.claim
    h.check(path == expected and volume['spec']['persistentVolumeReclaimPolicy'] == 'Delete',
            'Deletion targets one exact node-local application directory, not the VPS disk')
    h.check(volume['spec']['claimRef']['uid'] == old['metadata']['uid'], 'PV belongs to the selected live PVC')
    h.check(json.loads(h.kube('get', 'statefulset', name, '-o', 'json'))['spec']['replicas'] == 1,
            'Selected canonical StatefulSet has one original replica')
    agent_uid = json.loads(h.kube('-n', 'tester-agent', 'get', 'daemonset', 'tester-agent', '-o', 'json'))['metadata']['uid']
    h.artifact('target.json', {'worker': worker, 'pod': pod_name, 'claim': old, 'volume': volume,
                             'all_pvc_uids': {c['metadata']['name']: c['metadata']['uid'] for c in claims},
                             'tester_agent_uid': agent_uid})
    # Keep random index I/O off a Windows-mounted repo. Persist a sequential backup as evidence.
    ledger = sqlite3.connect('')
    ledger.executescript('PRAGMA synchronous=OFF; PRAGMA cache_size=-65536; CREATE TABLE copies(stage TEXT,unit TEXT,id TEXT,part INT,version INT,revision INT,digest TEXT,PRIMARY KEY(stage,unit,id)) WITHOUT ROWID; CREATE INDEX copies_identity ON copies(stage,id);')
    store_names = [pod_name] + sorted(p['metadata']['name'] for p in stores if p['metadata']['name'] != pod_name)

    def capture(stage, names, identities=None):
        for unit in names:
            where = '' if identities is None else ' WHERE hazard_id IN (' + ','.join("'" + key.replace("'", "''") + "'" for key in identities) + ')'
            query = "COPY (SELECT hazard_id,partition_id,ingestion_version,revision,md5(to_jsonb(e)::text) FROM hazard_events e" + where + ") TO STDOUT WITH (FORMAT csv,DELIMITER E'\\t')"
            command = 'sudo -n k3s kubectl -n ' + NS + ' ' + shlex.join(['exec', unit, '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'aggregator', '-qAt',
                '-c', "SET statement_timeout='90s'", '-c', query])
            # An independent SSH channel closes on timeout; no persistent master
            # may retain an abandoned COPY after the observer is interrupted.
            raw = subprocess.run(h.hosts.command(h.server, command, multiplex=False), capture_output=True,
                                 text=True, timeout=120, check=True).stdout
            prefix = '' if identities is None else 'recheck-' + str(time.monotonic_ns()) + '-'
            (h.folder / (prefix + stage + '-' + unit + '.tsv')).write_text(raw)
            if identities is None:
                ledger.execute('DELETE FROM copies WHERE stage=? AND unit=?', (stage, unit))
            ledger.executemany('INSERT OR REPLACE INTO copies VALUES(?,?,?,?,?,?,?)',
                ((stage, unit, r[0], int(r[1]), int(r[2]), int(r[3]), r[4]) for r in sorted(csv.reader(io.StringIO(raw), delimiter='\t'), key=lambda r: r[0])))
            ledger.commit()
            count = ledger.execute('SELECT count(*) FROM copies WHERE stage=? AND unit=?', (stage, unit)).fetchone()[0]
            h.note({'snapshot': stage, 'unit': unit, 'records': count})

    capture('before', store_names)
    # A later revision is valid live-feed progress; the same revision must retain every byte.
    valid = '(s.version>b.version OR (s.version=b.version AND s.revision>b.revision) OR (s.version=b.version AND s.revision=b.revision AND s.digest=b.digest))'
    unprotected = ledger.execute('SELECT count(*) FROM copies b WHERE b.stage=\'before\' AND b.unit=? AND NOT EXISTS(SELECT 1 FROM copies s INDEXED BY copies_identity WHERE s.stage=\'before\' AND s.unit<>b.unit AND s.id=b.id AND ' + valid + ')', (pod_name,)).fetchone()[0]
    h.check(unprotected == 0, 'Every row on the volume to be erased has an equal/newer intact peer before destruction')
    ledger.executescript("CREATE TABLE baseline AS SELECT b.* FROM copies b WHERE stage='before' AND NOT EXISTS(SELECT 1 FROM copies s INDEXED BY copies_identity WHERE s.stage='before' AND s.id=b.id AND (s.version>b.version OR (s.version=b.version AND s.revision>b.revision) OR (s.version=b.version AND s.revision=b.revision AND s.unit<b.unit))); CREATE UNIQUE INDEX baseline_id ON baseline(id);")
    baseline_count = ledger.execute('SELECT count(*) FROM baseline').fetchone()[0]
    unsafe_query = 'SELECT b.id FROM baseline b WHERE (SELECT count(*) FROM copies s INDEXED BY copies_identity WHERE s.stage=\'before\' AND s.id=b.id AND ' + valid + ')<2 LIMIT 1001'
    waiting = [r[0] for r in ledger.execute(unsafe_query)]
    wait_end = time.monotonic() + 120
    while waiting:
        h.note({'preflight_waiting_for_frozen_baseline_replicas': len(waiting)})
        h.check(len(waiting) <= 1000 and time.monotonic() < wait_end,
                'Frozen baseline replicas converge within the pre-destruction safety budget')
        # Refresh only these identities, never lower or redefine the baseline version.
        capture('before', store_names, waiting)
        waiting = [r[0] for r in ledger.execute(unsafe_query)]
        if waiting:
            time.sleep(2)
    h.check(True, 'Every frozen baseline event has two equal/newer physical copies before the destructive test')
    digest = hashlib.sha256()
    for row in ledger.execute('SELECT id,version,revision,digest FROM baseline ORDER BY id'):
        digest.update(('\t'.join(map(str, row)) + '\n').encode())
    h.artifact('baseline-summary.json', {'records': baseline_count, 'physical_manifest_sha256': digest.hexdigest(),
        'unprotected_target_rows': unprotected, 'assistant_checksum_used': False, 'live_updates_allowed_only_if_newer': True})
    h.note({'baseline_unique_records': baseline_count, 'unprotected_target_rows': unprotected})
    new_events = []
    writes = []
    write_failures = []
    observer = ReadObserver(h)

    def write_during_recovery():
        event = {'source': 'BMKG', 'source_ref_id': 'p1-live-' + h.run_id + '-' + str(len(new_events)),
            'hazard_type': 'SEISMIC', 'severity': 'NORMAL', 'area_name': 'P1 live recovery practice',
            'latitude': -1.0, 'longitude': 120.0, 'occurred_at': '2025-01-01T00:00:00Z',
            'attributes': {'practice': True, 'test_run': h.run_id}}
        new_events.append(event)
        started = time.monotonic()
        try:
            code, body, elapsed = h.request('aggregator', '/internal/import/events',
                {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']}, [event], 'POST')
        except Exception as error:
            code, body, elapsed = 0, {'error': {'code': 'transport_failure', 'message': str(error)}}, time.monotonic() - started
        writes.append({'phase': observer.phase, 'status': code, 'body': body, 'elapsed': elapsed,
                       'source_ref_id': event['source_ref_id']})
        h.artifact('writes.json', writes)
        acknowledged = code == 200 and body == {'accepted': 1, 'failed': 0}
        unavailable = code == 503 and (body == {'accepted': 0, 'failed': 1} or
            body.get('error', {}).get('code') == 'backend_unavailable')
        if not (acknowledged or unavailable):
            # Retain the failure but continue observation/restoration. A bad
            # response must not leave the worker window unobserved again.
            write_failures.append(writes[-1])
        h.artifact('write-failures.json', write_failures)

    changed = False
    try:
        observer.start()
        h.note({'destructive_target': h.args.claim, 'worker': worker, 'rows_have_surviving_peer': True,
                'original_volume_permanently_deleted': True, 'other_volumes_deleted': False})
        observer.phase = 'erase-live-volume'
        h.kube('scale', 'statefulset/' + name, '--replicas=0')
        changed = True
        h.kube('wait', '--for=delete', 'pod/' + pod_name, '--timeout=90s')
        current = json.loads(h.kube('get', 'pvc', h.args.claim, '-o', 'json'))
        h.check(current['metadata']['uid'] == old['metadata']['uid'], 'PVC identity unchanged immediately before deletion')
        h.kube('delete', 'pvc', h.args.claim, '--wait=true', '--timeout=90s')
        h.kube('wait', '--for=delete', 'pv/' + volume['metadata']['name'], '--timeout=120s')
        h.hosts.ssh(node, 'sudo -n test ! -e ' + shlex.quote(path), capture=True)
        h.artifact('erasure.json', {'deleted_claim_uid': old['metadata']['uid'], 'deleted_pv': volume['metadata']['name'],
                                  'exact_old_data_directory_absent': True, 'os_and_ssh_retained': True})
        unit = 'aat-p1-live-' + h.run_id.lower()
        observer.phase = 'worker-loss'
        command = ('sudo -n systemd-run --unit=' + shlex.quote(unit) + ' --on-active=70s --timer-property=AccuracySec=1s '
                   '/usr/bin/systemctl start k3s-agent && sudo -n systemctl stop k3s-agent && sudo -n /usr/local/bin/k3s-killall.sh')
        started = time.monotonic()
        saw_down = False
        def stop_worker():
            with (h.folder / 'worker-injection.log').open('wb') as log:
                subprocess.run(h.hosts.command(node, command, multiplex=False), stdout=log, stderr=log,
                               timeout=45, check=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            fault = pool.submit(stop_worker)
            while time.monotonic() - started < 130:
                write_during_recovery()
                status = json.loads(h.kube('get', 'node', worker, '-o', 'json'))['status']['conditions']
                ready = any(c['type'] == 'Ready' and c['status'] == 'True' for c in status)
                saw_down |= not ready
                h.report['observations'].append({'worker_ready': ready, 'elapsed': time.monotonic() - started})
                h.artifact('worker-observations.json', h.report['observations'])
                if fault.done():
                    fault.result()
                    if saw_down and ready:
                        break
                time.sleep(2)
        h.check(saw_down and ready, 'Worker becomes NotReady and returns automatically without manual restart')
        observer.phase = 'surviving-data-verification'
        capture('degraded', store_names[1:])
        missing = ledger.execute('SELECT count(*) FROM baseline b WHERE NOT EXISTS(SELECT 1 FROM copies s INDEXED BY copies_identity WHERE s.stage=\'degraded\' AND s.id=b.id AND ' + valid + ')').fetchone()[0]
        h.artifact('degraded-comparison.json', {'baseline_records': baseline_count, 'missing_or_regressed_records': missing})
        h.check(missing == 0, 'Every baseline event is intact or legitimately newer on surviving storage')
        observer.phase = 'empty-disk-repair'
        h.kube('scale', 'statefulset/' + name, '--replicas=1')
        h.kube('rollout', 'status', 'statefulset/' + name, '--timeout=180s')
        replacement = json.loads(h.kube('get', 'pvc', h.args.claim, '-o', 'json'))
        h.check(replacement['metadata']['uid'] != old['metadata']['uid'], 'Actual live database has a new empty-volume identity')
        h.artifact('replacement.json', replacement)
        end = time.monotonic() + h.args.timeout
        while True:
            write_during_recovery()
            raw = h.kube('exec', pod_name, '--', 'psql', '-U', 'postgres', '-d', 'aggregator', '-At', '-c',
                         "SELECT json_build_object('status',status,'copied',copied,'checkpoint',checkpoint) FROM p1_metadata")
            progress = json.loads(raw)
            h.note({'repair': progress})
            if progress['status'] == 'ready':
                break
            h.check(time.monotonic() < end, 'Automatic live-volume repair finishes within the declared observation budget')
            time.sleep(3)
        observer.phase = 'restored-data-verification'
        capture('after', store_names)
        incomplete = ledger.execute('SELECT count(*) FROM baseline b WHERE (SELECT count(*) FROM copies s INDEXED BY copies_identity WHERE s.stage=\'after\' AND s.id=b.id AND ' + valid + ')<2').fetchone()[0]
        observer.stop()
        api_failures = observer.failures
        saw_partial = {'/internal/events?limit=50', '/internal/audit/events?limit=50', '/analytics'} <= observer.partial_endpoints
        h.artifact('traffic.json', observer.records)
        h.artifact('api-failures.json', api_failures)
        h.artifact('final-comparison.json', {'baseline_records': baseline_count, 'without_two_intact_or_newer_copies': incomplete,
            'read_failures': api_failures, 'observed_explicit_partial': saw_partial,
            'partial_endpoints': sorted(observer.partial_endpoints), 'old_events_reimported': False})
        h.check(incomplete == 0, 'All baseline events automatically regain two intact/newer physical copies')
        h.check(saw_partial, 'Actual missing storage is explicitly labelled in event, Audit and dashboard reads')
        h.check(any(w['body'].get('accepted', 0) for w in writes), 'New live writes are acknowledged during recovery')
        for event in new_events:
            code, body, _ = h.request('aggregator', '/internal/import/events',
                {'X-Aggregator-Key': h.values['AGGREGATOR_API_KEY']}, [event], 'POST')
            h.check(code == 200 and body == {'accepted': 1, 'failed': 0},
                    'Only new fault-window inputs are retried, after old baseline replication was proved')
        final_claims = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
        final_ids = {c['metadata']['name']: c['metadata']['uid'] for c in final_claims}
        h.check(all(final_ids.get(c['metadata']['name']) == c['metadata']['uid'] for c in claims if c['metadata']['name'] != h.args.claim),
                'Every unrelated original application PVC identity is retained')
        h.check(json.loads(h.kube('-n', 'tester-agent', 'get', 'daemonset', 'tester-agent', '-o', 'json'))['metadata']['uid'] == agent_uid,
                'Official Tester Agent identity is retained')
        h.note({'canonical_data_recovered': True, 'baseline_records': baseline_count, 'api_failure_count': len(api_failures)})
        h.check(not write_failures, 'Recovery writes acknowledge exactly one replicated input or explicitly return unavailable')
        h.check(not api_failures, 'All sampled live APIs meet the declared read bounds; failures remain visible')
    finally:
        try:
            if changed:
                # Restore desired replica count even if an assertion fails; never restore data with SQL.
                h.kube('scale', 'statefulset/' + name, '--replicas=1')
        finally:
            try:
                observer.stop()
            finally:
                persist_ledger(h, ledger)
                ledger.close()


def verify_previous(h):
    h.check(bool(h.hosts), 'Interrupted-run verification requires VPS inventory and performs no mutation')
    previous = h.args.verify_run.resolve()
    target = json.loads((previous / 'target.json').read_text())
    summary = json.loads((previous / 'baseline-summary.json').read_text())
    erasure = json.loads((previous / 'erasure.json').read_text())
    h.check(target['worker'] == h.args.worker and target['claim']['metadata']['name'] == h.args.claim,
            'Read-only verification targets exactly the previously authorized run')
    h.artifact('previous-run.json', {'path': str(previous), 'baseline': summary, 'erasure': erasure,
        'original_window_verdict': 'inconclusive: observation process did not retain a final result'})
    ledger = sqlite3.connect('')
    ledger.executescript('PRAGMA synchronous=OFF; PRAGMA cache_size=-65536; CREATE TABLE copies(stage TEXT,unit TEXT,id TEXT,part INT,version INT,revision INT,digest TEXT,PRIMARY KEY(stage,unit,id)) WITHOUT ROWID; CREATE INDEX copies_identity ON copies(stage,id);')

    def ingest(stage, unit, raw):
        ledger.executemany('INSERT INTO copies VALUES(?,?,?,?,?,?,?)',
            ((stage, unit, r[0], int(r[1]), int(r[2]), int(r[3]), r[4]) for r in sorted(csv.reader(io.StringIO(raw), delimiter='\t'), key=lambda r:r[0])))
        ledger.commit()
        h.note({'snapshot': stage, 'unit': unit, 'records': ledger.execute('SELECT count(*) FROM copies WHERE stage=? AND unit=?',(stage,unit)).fetchone()[0]})

    try:
        for letter in 'abc':
            unit = 'postgres-' + letter + '-0'
            ingest('before', unit, (previous / ('before-' + unit + '.tsv')).read_text())
        ledger.executescript("CREATE TABLE baseline AS SELECT b.* FROM copies b WHERE stage='before' AND NOT EXISTS(SELECT 1 FROM copies s INDEXED BY copies_identity WHERE s.stage='before' AND s.id=b.id AND (s.version>b.version OR (s.version=b.version AND s.revision>b.revision) OR (s.version=b.version AND s.revision=b.revision AND s.unit<b.unit))); CREATE UNIQUE INDEX baseline_id ON baseline(id);")
        digest = hashlib.sha256()
        for row in ledger.execute('SELECT id,version,revision,digest FROM baseline ORDER BY id'):
            digest.update(('\t'.join(map(str,row))+'\n').encode())
        count = ledger.execute('SELECT count(*) FROM baseline').fetchone()[0]
        h.check(count == summary['records'] and digest.hexdigest() == summary['physical_manifest_sha256'],
                'Reconstructed original baseline matches its previously recorded count and SHA-256 exactly')
        for letter in 'abc':
            unit = 'postgres-' + letter + '-0'
            query = "COPY (SELECT hazard_id,partition_id,ingestion_version,revision,md5(to_jsonb(e)::text) FROM hazard_events e) TO STDOUT WITH (FORMAT csv,DELIMITER E'\\t')"
            raw = h.kube('exec', unit, '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'aggregator', '-qAt', '-c', query)
            (h.folder / ('after-' + unit + '.tsv')).write_text(raw)
            ingest('after', unit, raw)
        valid = '(s.version>b.version OR (s.version=b.version AND s.revision>b.revision) OR (s.version=b.version AND s.revision=b.revision AND s.digest=b.digest))'
        missing = ledger.execute('SELECT count(*) FROM baseline b WHERE NOT EXISTS(SELECT 1 FROM copies s INDEXED BY copies_identity WHERE s.stage=\'after\' AND s.id=b.id AND '+valid+')').fetchone()[0]
        incomplete = ledger.execute('SELECT count(*) FROM baseline b WHERE (SELECT count(*) FROM copies s INDEXED BY copies_identity WHERE s.stage=\'after\' AND s.id=b.id AND '+valid+')<2').fetchone()[0]
        h.artifact('physical-verdict.json', {'baseline_records': count, 'missing_or_regressed': missing,
            'without_two_intact_or_newer_copies': incomplete, 'canonical_data_recovered': missing == incomplete == 0,
            'reimported_old_events': False, 'original_window_verdict': 'inconclusive'})
        h.check(missing == 0 and incomplete == 0, 'All original baseline events have two intact/equal-or-newer physical copies')
        claim = json.loads(h.kube('get','pvc',h.args.claim,'-o','json'))
        h.check(claim['metadata']['uid'] != target['claim']['metadata']['uid'], 'Erased original volume was replaced, not remounted')
        h.artifact('replacement.json', claim)
        pods = [p for p in h.pods().values() if p['app'] in ('postgres-a','postgres-b','postgres-c')]
        h.check(len(pods) == 3 and all(p['ready'] for p in pods) and len({p['node'] for p in pods}) == 3,
                'Recovered canonical copies are on three distinct Ready storage nodes')
        claims = json.loads(h.kube('get','pvc','-o','json'))['items']
        ids = {c['metadata']['name']:c['metadata']['uid'] for c in claims}
        h.check(all(ids.get(name)==uid for name,uid in target['all_pvc_uids'].items() if name!=h.args.claim),
                'Every unrelated application PVC identity is unchanged')
        uid=json.loads(h.kube('-n','tester-agent','get','daemonset','tester-agent','-o','json'))['metadata']['uid']
        h.check(uid==target['tester_agent_uid'], 'Official Tester Agent identity is unchanged')
        events=json.loads(h.kube('get','events','-A','--field-selector','involvedObject.kind=Node,involvedObject.name='+h.args.worker,'-o','json'))['items']
        h.artifact('worker-events.json',events)
        h.note({'canonical_data_recovered': True, 'baseline_records': count, 'original_recovery_window': 'inconclusive'})
        for _ in range(3):
            code,_,elapsed=h.request('aggregator','/internal/events?limit=50',{'X-Aggregator-Key':h.values['AGGREGATOR_API_KEY']})
            h.check(code==200 and elapsed<5, 'Post-recovery read is successful and bounded')
        code,body,elapsed=h.api('/analytics')
        h.artifact('analytics-verdict.json',{'status':code,'elapsed':elapsed,'body':body})
        h.check(code==200 and elapsed<5,'Post-recovery global analytics is successful and bounded')
        raise Inconclusive('Final data recovery verified; the interrupted original observation window remains inconclusive')
    finally:
        persist_ledger(h, ledger)
        ledger.close()


if __name__ == '__main__':
    p = parser('Erase one explicitly selected live canonical volume and verify automatic node/data recovery')
    p.add_argument('--worker', required=True)
    p.add_argument('--claim', required=True)
    p.add_argument('--verify-run', type=Path, help='Read-only verification of an already erased/replaced run; never repeats the fault')
    p.set_defaults(timeout=1200, repeat=1)
    raise SystemExit(run('p1/04-live-recovery', ['P1.4', 'P1.5', 'P1.6', 'P1.7'], p, scenario))
