from pathlib import Path
import datetime as dt
import json
import re
import sys
import time
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import parser, run, Failure, Inconclusive


def agent_rows(raw):
    rows = []
    for line in raw.splitlines():
        match = re.search(r'\{.*$', line)
        if match:
            try:
                rows.append(json.loads(match[0]))
            except ValueError:
                pass
    return rows


def mock_mode(items):
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        raise Inconclusive('Cannot establish a restorable initial PVMBG schema from live source data')
    item = items[0]
    if 'report_ref' in item and 'report_id' not in item:
        return 'rename'
    if isinstance(item.get('eruption_count_24h'), str):
        return 'type-change'
    if 'alert_level' not in item:
        return 'missing'
    return 'additive' if 'confidence_level' in item else 'legacy'


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Official schema practice runs on the VPS cluster')
    at = h.sql('SELECT to_json(to_char(clock_timestamp(),\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'))')
    query = '/volcanic-reports?'+urllib.parse.urlencode({'since': (dt.datetime.fromisoformat(at.replace('Z', '+00:00'))-dt.timedelta(seconds=30)).isoformat()})
    headers = {'Authorization': 'Bearer '+h.values['PVMBG_TOKEN']}
    def initial_rows():
        code, rows, _ = h.request('mock', query, headers)
        return rows if code == 200 and isinstance(rows, list) and rows else None
    baseline = h.wait(initial_rows, 'PVMBG produced no readable fresh records before schema injection')
    original = mock_mode(baseline)
    changed = False
    try:
        h.schema('legacy')
        changed = True
        for repetition in range(h.args.repeat):
            before = h.pods()
            ordinal = h.sql('SELECT to_json(COALESCE(max(ordinal),0)) FROM durable_inbox')
            since = h.sql('SELECT to_json(clock_timestamp())')
            raw = h.kube('-n', 'tester-agent', 'exec', 'daemonset/tester-agent', '--', '/app', 'run', 'practice', 'schema')
            h.artifact(f'round-{repetition+1}-submission.json', {'output': raw})
            end = time.monotonic()+h.args.timeout
            while True:
                logs = h.kube('-n', 'tester-agent', 'logs', '-l', 'app=tester-agent', '--since-time='+since,
                              '--prefix', '--tail=200', '--max-log-requests=3')
                rows = agent_rows(logs)
                began = any('skenario "schema"' in r.get('action', '') for r in rows)
                finished = any('pemulihan akhir selesai' in r.get('action', '') for r in rows)
                if began and finished:
                    break
                if time.monotonic() >= end:
                    h.artifact(f'round-{repetition+1}-agent.json', {'log': logs})
                    raise Failure('Official schema practice did not report completion')
                h.request('aggregator', '/ready')
                time.sleep(2)
            h.artifact(f'round-{repetition+1}-agent.json', {'log': logs})
            raw_mock = h.kube('logs', 'deployment/pvmbg', '--since-time='+since)
            h.artifact(f'round-{repetition+1}-pvmbg.json', {'log': raw_mock})
            admin = []
            for line in raw_mock.splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get('method') == 'POST' and row.get('path') == '/admin/schema-version':
                    admin.append(row)
            h.check(bool(admin), 'The official Agent request actually reached the PVMBG schema endpoint')
            h.note({'round': repetition+1, 'official_admin_statuses': [r['status'] for r in admin]})
            h.check(all(200 <= r['status'] < 300 for r in admin), 'PVMBG accepts the official Agent schema injection')
            changed = True
            code, _, _ = h.request('mock', query)
            h.check(code == 401, 'Normal PVMBG data listener still rejects missing Bearer credentials')
            code, _, _ = h.request('mock', query, {'X-BMKG-Key': h.values['BMKG_API_KEY']})
            h.check(code == 401, 'BMKG credentials do not grant PVMBG data access')
            ip = h.kube('get', 'service/pvmbg-admin', '-o', 'jsonpath={.spec.clusterIP}').strip()
            positive = h.hosts.ssh(h.server, 'curl --fail --silent --max-time 3 http://'+ip+':8089/health', capture=True)
            h.check(json.loads(positive).get('status') == 'ok', 'Control listener is live while its network isolation is tested')
            probe = ("import json,socket\n"
                     "ip=socket.gethostbyname('pvmbg-admin')\n"
                     "data=socket.create_connection((socket.gethostbyname('pvmbg'),8082),timeout=2);data.close()\n"
                     "try:\n"
                     " c=socket.create_connection((ip,8089),timeout=2);c.close();out={'blocked':False}\n"
                     "except (TimeoutError,ConnectionRefusedError) as error:\n"
                     " out={'blocked':True,'failure':type(error).__name__,'data_port_reachable':True}\n"
                     "print(json.dumps(out))\n")
            isolation = json.loads(h.kube('exec', 'deployment/observability', '--', 'python3', '-c', probe))
            h.artifact(f'round-{repetition+1}-admin-isolation.json', isolation)
            h.check(isolation['blocked'], 'An ordinary application pod cannot connect to the tester-only control port')
            code, after, _ = h.request('mock', query, headers)
            h.artifact(f'round-{repetition+1}-schema.json', {'before_mode': original, 'response_status': code,
                        'agent_actions': rows, 'application_pods': h.pods()})
            h.check(code == 200 and isinstance(after, list), 'PVMBG still responds with a parseable collection after practice schema injection')
            mode = mock_mode(after)
            h.note({'round': repetition+1, 'observed_schema_mode': mode})
            if mode in ('legacy', 'additive'):
                def processed():
                    rows = h.sql("SELECT COALESCE(json_agg(row_to_json(x)),'[]'::json) FROM ("
                        "SELECT m.ordinal,m.source_ref_id,m.status,m.reason,"
                        "(convert_from(c.payload,'UTF8')::jsonb ? 'confidence_level') AS additive "
                        "FROM durable_inbox m JOIN inbox_chunks c USING(message_id) "
                        f"WHERE m.source='PVMBG' AND m.kind='volcanic-report' AND c.chunk=0 AND m.ordinal>{int(ordinal)} "
                        "ORDER BY m.ordinal DESC LIMIT 30) x")
                    return [r for r in rows if r['status'] == 'done' and r['additive'] == (mode == 'additive')]
                observed = h.wait(processed, 'No newly received report in the active practice schema completed processing')
                h.artifact(f'round-{repetition+1}-processing.json', observed)
            for uid, p in before.items():
                if p['app'] in ('pvmbg', 'aggregator'):
                    current = h.pods().get(uid)
                    h.check(current is not None and current['ready'] and current['restarts'] == p['restarts'], 'Schema injection does not crash or restart '+p['name'])
    finally:
        if changed:
            # End-of-campaign stimulus cleanup, after Agent recovery completes;
            # never patch adapters or repair applications inside the fault window.
            h.schema(original)


if __name__ == '__main__':
    raise SystemExit(run('tester-agent/03-schema', ['Tester Agent admin integration', 'P3 schema crash smoke'],
                        parser('Verify the official schema fault reaches the mock and is not blocked by its integration'), scenario))
