from pathlib import Path
from collections import Counter
import datetime as dt
import hashlib
import json
import re
import socket
import subprocess
import sys
import time
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run, Failure


def stamp(value):
    value = value.replace('Z', '+00:00')
    value = re.sub(r'\.(\d+)(?=[+-]\d{2}:\d{2}$)', lambda m: '.'+m[1][:6].ljust(6, '0'), value)
    return dt.datetime.fromisoformat(value)


def snapshot(h):
    return h.sql("""SELECT json_build_object('at',clock_timestamp(),
 'poll_id',COALESCE((SELECT max(poll_id) FROM poll_attempts),0),
 'outbox_version',COALESCE((SELECT max(version) FROM ingestion_outbox),0),
 'leases',(SELECT json_agg(row_to_json(l)) FROM source_leases l WHERE source IN ('BMKG','PVMBG')),
 'cursors',(SELECT json_agg(row_to_json(c)) FROM source_cursors c WHERE source IN ('BMKG','PVMBG')))""")


def open_bmkg(h):
    ip = h.kube('get', 'service/bmkg', '-o', 'jsonpath={.spec.clusterIP}').strip()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    cmd = h.hosts.command(h.server, '', multiplex=False)[:-2]
    cmd += ['-L', f'127.0.0.1:{port}:{ip}:8081', '-o', 'ExitOnForwardFailure=yes', '-N', h.server['user']+'@'+h.server['host']]
    proc = subprocess.Popen(cmd)
    h.extra_processes.append(proc)
    h.urls['bmkg'] = 'http://127.0.0.1:'+str(port)
    end = time.monotonic()+15
    while True:
        if proc.poll() is not None:
            raise Failure('BMKG observation tunnel exited')
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                return
        except OSError:
            if time.monotonic() >= end:
                raise Failure('BMKG observation tunnel did not start')
            time.sleep(.1)


def recent_audit(h, source, since):
    out, cursor, seen = [], None, set()
    while True:
        query = {'source': source, 'since': since, 'limit': 1000}
        if cursor:
            query['cursor'] = cursor
        code, body, _ = h.request('aggregator', '/internal/audit/events?'+urllib.parse.urlencode(query),
                                {'Authorization': 'Bearer '+h.values['P1_AUDIT_TOKEN']})
        h.check(code == 200 and not body.get('partial'), 'Recent canonical Audit API response is complete')
        out.extend(body['items'])
        cursor = body.get('next_cursor')
        if not cursor:
            return out
        h.check(cursor not in seen, 'Recent audit pagination advances')
        seen.add(cursor)


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Targeted polling test runs from the laptop against the VPS cluster')
    h.check(h.args.observe_seconds >= 12, 'Observe at least one normal mock generation interval')
    open_bmkg(h)
    interval_ms = int(json.loads(h.kube('get', 'configmap/aggregator-config', '-o', 'json'))['data']['AGGREGATOR_POLL_INTERVAL_MS'])
    unavailable_reads = []
    for repetition in range(h.args.repeat):
        before = snapshot(h)
        pods = h.pods()
        owner = next(l for l in before['leases'] if l['source'] == h.args.source)
        target = next((p for p in pods.values() if p['name'] == owner['holder_id']), None)
        h.check(target is not None and target['app'] == 'aggregator' and target['ready'], 'Fault target is the recorded Ready polling owner')
        affected = {l['source']: l for l in before['leases'] if l['holder_id'] == owner['holder_id']}
        h.artifact(f'round-{repetition+1}-before.json', {'snapshot': before, 'pods': pods, 'affected_sources': affected,
                    'fault_kind': 'targeted owner pod deletion, supplemental to official Tester Agent practice'})
        started = time.monotonic()
        h.kube('delete', 'pod', target['name'], '--grace-period=0', '--force', '--wait=false')
        recovered, samples = {}, []
        end = started+60
        while len(recovered) < len(affected):
            rows = h.sql(f"""SELECT COALESCE(json_agg(row_to_json(p)),'[]'::json) FROM poll_attempts p
 WHERE poll_id>{int(before['poll_id'])} AND status='completed' AND source IN ('BMKG','PVMBG')""")
            for row in rows:
                source = row['source']
                if source in affected and row['holder_id'] != target['name'] and row['fencing_token'] > affected[source]['fencing_token']:
                    recovered.setdefault(source, {'observed_seconds': time.monotonic()-started, 'poll': row})
            try:
                code, _, seconds = h.api('/events?limit=1&source=BMKG', 'FIELD')
                samples.append({'at_seconds': time.monotonic()-started, 'status': code, 'seconds': seconds})
            except Failure as error:
                samples.append({'at_seconds': time.monotonic()-started, 'error': str(error)})
            if time.monotonic() > end:
                break
            if len(recovered) < len(affected):
                time.sleep(.5)
        h.artifact(f'round-{repetition+1}-takeover.json', {'sources': recovered, 'client_samples': samples})
        unavailable_reads.extend({'round': repetition+1, **s} for s in samples if s.get('status') != 200)
        h.check(set(recovered) == set(affected), 'Every affected mock source has a completed poll by a new fenced owner within 60 seconds')
        h.check(all(r['observed_seconds'] <= 60 for r in recovered.values()), 'Observed completed-poll takeover meets the unchanged 60-second target')
        until = time.monotonic()+h.args.observe_seconds
        while time.monotonic() < until:
            h.request('aggregator', '/ready')
            time.sleep(2)
        cutoff = snapshot(h)
        since = stamp(before['at']).isoformat()
        source_specs = [
            ('BMKG', 'bmkg', '/seismic-events', 'event_id', 'occurred_at', {'X-BMKG-Key': h.values['BMKG_API_KEY']}),
            ('BMKG', 'bmkg', '/tsunami-warnings', 'warning_id', None, {'X-BMKG-Key': h.values['BMKG_API_KEY']}),
            ('PVMBG', 'mock', '/volcanic-reports', 'report_id', 'reported_at', {'Authorization': 'Bearer '+h.values['PVMBG_TOKEN']})]
        expected = {'BMKG': set(), 'PVMBG': set()}
        for source, service, endpoint, identity, timestamp, headers in source_specs:
            code, items, _ = h.request(service, endpoint+'?'+urllib.parse.urlencode({'since': since}), headers)
            h.check(code == 200 and isinstance(items, list), 'Source window can be independently enumerated: '+endpoint)
            if timestamp:
                items = [i for i in items if stamp(i[timestamp]) <= stamp(cutoff['at'])]
            else:
                # Warnings are filtered by the mock using their parent quake's
                # occurrence time; enumerate this immutable returned set too.
                pass
            expected[source].update(i[identity] for i in items)
        h.artifact(f'round-{repetition+1}-expected.json', {s: sorted(ids) for s, ids in expected.items()})
        def delivered():
            pages = {s: recent_audit(h, s, since) for s in expected}
            h.artifact(f'round-{repetition+1}-actual.json', pages)
            return pages if all(expected[s] <= {e['source_ref_id'] for e in pages[s]} for s in expected) else None
        actual = h.wait(delivered, 'A source event generated in the fault window never reached canonical storage')
        h.check(all(expected.values()), 'Both mock sources produced new data during this fault window')
        for source, items in actual.items():
            keys = [(e['source'], e['source_ref_id']) for e in items]
            h.check(len(keys) == len(set(keys)), 'No duplicate canonical identity in recent '+source+' audit')
        ids = []
        for source, refs in expected.items():
            ids += ['hz-'+hashlib.sha256(json.dumps([source, ref], separators=(',', ':')).encode()).hexdigest() for ref in refs]
        sql_ids = ','.join("'"+identity+"'" for identity in ids)
        admissions = h.sql("SELECT COALESCE(json_agg(row_to_json(x)),'[]'::json) FROM (SELECT hazard_id,count(*) AS versions FROM ingestion_outbox WHERE version>"+str(int(before['outbox_version']))+" AND hazard_id IN ("+sql_ids+") GROUP BY hazard_id) x")
        h.artifact(f'round-{repetition+1}-admissions.json', admissions)
        h.check(len(admissions) == len(ids) and all(row['versions'] == 1 for row in admissions), 'Each source event in this fixed mock lifetime was admitted once in the crash window')
        polls = h.sql(f"SELECT COALESCE(json_agg(row_to_json(p)),'[]'::json) FROM poll_attempts p WHERE poll_id>{int(before['poll_id'])} AND poll_id<={int(cutoff['poll_id'])} AND source IN ('BMKG','PVMBG')")
        counts = Counter()
        for service in ('bmkg', 'pvmbg'):
            raw = h.kube('logs', 'deployment/'+service, '--since-time='+since)
            h.artifact(f'round-{repetition+1}-{service}-logs.json', {'log': raw})
            for line in raw.splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                match = re.fullmatch(r'poll-(BMKG|PVMBG)-(\d+)', row.get('correlation_id', ''))
                if match:
                    counts[(match[1], int(match[2]), row['path'])] += 1
        duplicates = [{'source': s, 'poll_id': p, 'path': route, 'requests': n} for (s,p,route),n in counts.items() if n > 1]
        missing_logs = []
        for poll in polls:
            if poll['status'] != 'completed':
                continue
            paths = ['/seismic-events', '/tsunami-warnings'] if poll['source'] == 'BMKG' else ['/volcanic-reports']
            for path in paths:
                if counts[(poll['source'], poll['poll_id'], path)] != 1:
                    missing_logs.append({'poll': poll, 'path': path})
        cadence_violations = []
        for source in ('BMKG', 'PVMBG'):
            ordered = sorted([p for p in polls if p['source'] == source], key=lambda p: stamp(p['started_at']))
            for previous, current in zip(ordered, ordered[1:]):
                distance_ms = (stamp(current['started_at'])-stamp(previous['started_at'])).total_seconds()*1000
                if distance_ms < interval_ms-20:
                    cadence_violations.append({'previous': previous, 'current': current, 'distance_ms': distance_ms})
        h.artifact(f'round-{repetition+1}-polls.json', {'polls': polls, 'duplicates': duplicates, 'missing_logs': missing_logs,
                    'configured_interval_ms': interval_ms, 'cadence_violations': cadence_violations})
        h.check(not duplicates and not missing_logs, 'Upstream logs contain exactly one request per completed poll endpoint and no repeated poll endpoint')
        h.check(not cadence_violations, 'Source polling cadence is shared across owners, without multiplied logical poll periods')
        h.wait(lambda: all(p['ready'] for p in h.pods().values() if p['app'] == 'aggregator'), 'Aggregator replicas did not become Ready')
        h.note({'round': repetition+1, 'target': target['name'], 'takeover_seconds': {s: r['observed_seconds'] for s,r in recovered.items()},
                'expected_events': {s: len(v) for s,v in expected.items()}, 'upstream_duplicates': len(duplicates),
                'client_non_200': sum(s.get('status') != 200 for s in samples),
                'scope': 'Normal-clock, targeted polling-owner crash with real mocks; not the clock-skew, freeze, sustained load or entire P2 acceptance'})
    h.artifact('unavailable-reads.json', unavailable_reads)
    h.check(not unavailable_reads, 'Field reads through the common entrypoint remain successful during each observed owner crash')


if __name__ == '__main__':
    p = parser('Crash the actual polling owner and verify takeover, source-window coverage, deduplication and upstream request logs')
    p.add_argument('--source', choices=['BMKG', 'PVMBG'], default='BMKG')
    p.add_argument('--observe-seconds', type=int, default=16)
    raise SystemExit(run('tester-agent/02-polling-failover', ['P2.2 crash window', 'P2.3 targeted owner crash'], p, scenario))
