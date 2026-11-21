from pathlib import Path
import json
import shlex
import subprocess
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run
from sandbox import Sandbox


def scenario(h):
    h.check(h.args.count > 0, 'Report count is positive')
    with Sandbox(h) as lab:
        accepted = []
        for round_number in range(h.args.repeat):
            app = lab.name + '-lock-' + str(round_number)
            sql = (f"SET application_name='{app}'; SET statement_timeout='90s'; BEGIN; "
                   "LOCK TABLE field_report_results IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(75); ROLLBACK")
            command = 'sudo -n k3s ' + shlex.join(['kubectl', '-n', NS, 'exec', 'aggregator-coordinator-0', '--',
                       'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'aggregator_coord', '-d', lab.database, '-c', sql])
            blocker = subprocess.Popen(h.hosts.command(h.server, command), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                h.wait(lambda: h.sql("SELECT to_json(count(*)) FROM pg_locks l JOIN pg_stat_activity a USING(pid) "
                                    f"WHERE a.application_name='{app}' AND l.mode='AccessExclusiveLock' AND l.granted") == 1,
                       'Controlled downstream lock was not established')
                reports = []
                for i in range(h.args.count):
                    body = {'report_id': f'{lab.name}-{round_number}-{i}', 'description': 'Durable receiver recovery'}
                    key = {'Idempotency-Key': body['report_id']}
                    code, result, elapsed = h.api('/field-reports', 'FIELD', body, key)
                    h.check(code == 202 and elapsed < 2, 'HTTP 202 remains fast while downstream processing is blocked')
                    reports.append({'body': body, 'key': key, 'id': result['report_id'], 'time_total': elapsed})
                ids = ','.join("'" + r['id'] + "'" for r in reports)
                h.check(h.sql(f"SELECT to_json(count(*)) FROM durable_inbox WHERE message_id IN ({ids}) AND status='done'") == 0,
                        'Acknowledgements preceded downstream completion')
                h.wait(lambda: h.sql("SELECT to_json(count(*)) FROM durable_inbox WHERE source='FIELD_REPORT' "
                                    "AND status='retrying' AND reason LIKE 'downstream_retry:%'") > 0,
                       'Transient downstream failures did not retain a retryable reason')
                pods = json.loads(h.kube('get', 'pods', '-l', 'app=' + lab.name + '-agg', '-o', 'json'))['items']
                h.check(len(pods) == 3, 'Three receiver instances are fault targets')
                h.kube('delete', 'pod', *[p['metadata']['name'] for p in pods], '--grace-period=0', '--force', '--wait=false')
                h.note({'round': round_number, 'fault': 'killed_all_receivers_after_202', 'acknowledged': len(reports)})
            finally:
                h.sql(f"SELECT COALESCE(json_agg(pg_terminate_backend(pid)),'[]'::json) FROM pg_stat_activity WHERE application_name='{app}' AND datname='{lab.database}'")
                try:
                    blocker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    blocker.terminate()
                    blocker.wait(timeout=10)
            h.kube('rollout', 'status', 'deployment/' + lab.name + '-agg', '--timeout=180s')
            h.wait(lambda: h.sql(f"SELECT to_json(count(*)) FROM field_report_results WHERE report_id IN ({ids})") == len(reports),
                   'Acknowledged reports were lost after the receiver crash')
            for report in reports:
                code, retry, _ = h.api('/field-reports', 'FIELD', report['body'], report['key'])
                h.check(code == 202 and retry['report_id'] == report['id'], 'Retry after crash preserves the same report identity')
                h.check(h.api('/field-reports', 'FIELD', {**report['body'], 'description': 'conflict'}, report['key'])[0] == 409,
                        'Conflicting retry cannot overwrite the original acknowledgement')
            h.check(h.sql(f"SELECT to_json(count(*)) FROM durable_inbox WHERE message_id IN ({ids}) AND status='done'") == len(reports),
                    'Every acknowledged identity completed exactly once')
            accepted.extend(reports)
        h.artifact('acknowledgements.json', accepted)
        h.note({'acknowledged': len(accepted), 'crash_rounds': h.args.repeat, 'lost': 0, 'duplicate_results': 0})


if __name__ == '__main__':
    p = parser('Verify durable acknowledgements across blocked workers and receiver crashes')
    p.add_argument('--count', type=int, default=20)
    raise SystemExit(run('p3/05-delivery', ['P3.2', 'P3.6'], p, scenario))
