from datetime import datetime, timezone
from collections import Counter
from pathlib import Path
import gzip
import hashlib
import json
import re
import shlex
import sys
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import Failure, NS, parser, run
from tester_agent import practice


def verify_backup(h):
    manifest = json.loads(h.args.backup_manifest.read_text())
    h.check(manifest.get('status') == 'complete' and manifest.get('namespace') == NS,
            'A completed backup of this namespace is required before data destruction')
    age = (datetime.now(timezone.utc)-datetime.fromisoformat(manifest['created_at'])).total_seconds()
    h.check(0 <= age < 6*3600, 'Backup is less than six hours old')
    folder = manifest['remote_folder']
    h.check(bool(re.fullmatch(r'\.local/state/aat-backups/\d{8}T\d{6}Z-[0-9a-f]{8}', folder)),
            'Backup path is an exact private campaign directory')
    destination = h.hosts.data['load_generator']
    h.check(manifest['destination'] == 'load_generator' and
            destination['host'] not in {n['host'] for n in h.hosts.nodes},
            'Backup destination is outside the application cluster')
    for record in manifest['files']:
        h.check(Path(record['name']).name == record['name'] and record['bytes'] > 0 and
                bool(re.fullmatch('[0-9a-f]{64}', record['sha256'])), 'Backup entry has a safe name and checksum')
    remote = '''import hashlib,json,pathlib,sys
p=pathlib.Path.home()/sys.argv[1]
m=json.loads((p/'backup-manifest.json').read_text())
for r in m['files']:
 f=p/r['name'];d=hashlib.sha256()
 with f.open('rb') as stream:
  while block:=stream.read(1048576): d.update(block)
 assert f.stat().st_size==r['bytes'] and d.hexdigest()==r['sha256'],r['name']
print(json.dumps(m))
'''
    verified = json.loads(h.hosts.ssh(destination, 'python3 -c '+shlex.quote(remote)+' '+shlex.quote(folder), capture=True))
    h.check(verified == manifest, 'All off-cluster backup files match their full SHA-256 and size')
    claims = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
    h.check(manifest['pvc_uids'] == {p['metadata']['name']: p['metadata']['uid'] for p in claims},
            'Backed-up volume identities still match the lab')
    h.artifact('backup-verification.json', {'run_id': manifest['run_id'], 'remote_folder': folder,
        'files': len(manifest['files']), 'bytes': sum(r['bytes'] for r in manifest['files']), 'sha256_verified': True})


def identities(h, label):
    cursor, seen, result = None, set(), set()
    for _ in range(2000):
        query = urllib.parse.urlencode({'limit': 1000, **({'cursor': cursor} if cursor else {})})
        code, page, _ = h.request('aggregator', '/internal/audit/events?'+query,
            {'Authorization': 'Bearer '+h.values['P1_AUDIT_TOKEN']}, private=True)
        h.check(code == 200 and page.get('partial') is False, 'Baseline comparison uses complete Audit pages')
        for event in page['items']:
            if event['source'] != 'SATELIT':
                result.add(event['source']+'\t'+event['source_ref_id']+'\t'+event['hazard_id'])
        cursor = page.get('next_cursor')
        if not cursor:
            break
        h.check(cursor not in seen, 'Audit pagination advances')
        seen.add(cursor)
    else:
        raise Failure('Audit enumeration exceeded its bounded page count')
    digest = hashlib.sha256()
    with gzip.open(h.folder/(label+'-identities.txt.gz'), 'wb') as output:
        for identity in sorted(result):
            row = (identity+'\n').encode()
            output.write(row)
            digest.update(row)
    h.artifact(label+'-summary.json', {'identities': len(result), 'sha256': digest.hexdigest(),
        'scope': 'All enumerated non-SATELIT canonical identities. Live satellite fires may legitimately retract; no assistant reference dataset is substituted.'})
    h.check(bool(result), 'Baseline contains real canonical records')
    return result


def scenario(h):
    h.changes()
    h.check(h.args.vps, 'Destructive practice is restricted to the backed-up VPS lab')
    verify_backup(h)
    before = identities(h, 'before')
    expected_ids = {row.rsplit('\t', 1)[1] for row in before}
    def physical_copies(label):
        copies, counts = Counter(), {}
        for letter in 'abc':
            rows = set(h.kube('exec', 'postgres-'+letter+'-0', '--', 'psql', '-U', 'postgres', '-d', 'aggregator',
                '-At', '-c', "SELECT hazard_id FROM hazard_events WHERE source<>'SATELIT'").splitlines())
            copies.update(rows)
            counts[letter] = len(rows)
        under = sorted(key for key in expected_ids if copies[key] < 2)
        h.artifact(label+'-physical-copies.json', {'rows_by_store': counts,
            'baseline_identities': len(expected_ids), 'fewer_than_two_copies': len(under),
            'scope': 'Distinct physical PostgreSQL stores, not duplicate API rows'})
        h.artifact(label+'-under-replicated.json', under)
        return not under
    h.check(physical_copies('before'), 'Every baseline identity has two physical copies before destructive practice')
    def database_generations():
        return {letter: h.kube('exec', 'postgres-'+letter+'-0', '--', 'psql', '-U', 'postgres', '-d', 'aggregator',
            '-At', '-c', 'SELECT system_identifier FROM pg_control_system()').strip() for letter in 'abc'}
    for repetition in range(h.args.repeat):
        generations = database_generations()
        h.artifact('round-'+str(repetition+1)+'-before-database-generations.json', generations)
        availability_error = None
        try:
            result = practice(h, 'wipe-node', repetition+1)
        except Failure as error:
            observation = json.loads((h.folder/('round-'+str(repetition+1)+'-observation.json')).read_text())
            if not observation.get('application_recovered'):
                raise
            availability_error = str(error)
            result = {'lost_nodes': observation.get('lost_nodes', [])}
        recovered_generations = database_generations()
        h.artifact('round-'+str(repetition+1)+'-database-generations.json',
                   {'before': generations, 'after': recovered_generations, 'lost_nodes': result['lost_nodes']})
        reinitialized = bool(result['lost_nodes']) and generations != recovered_generations
        after = identities(h, 'round-'+str(repetition+1)+'-after')
        missing = sorted(before-after)
        h.artifact('round-'+str(repetition+1)+'-missing.json', missing)
        h.check(not missing, 'Every baseline non-retractable canonical identity survived the official wipe')
        h.wait(lambda: physical_copies('round-'+str(repetition+1)+'-after'),
               'Autonomous recovery restores two physical copies of every baseline identity')
        h.note({'round': repetition+1, 'baseline_identities': len(before), 'after_identities': len(after),
                'missing': len(missing), 'backup_restored_during_observation': False,
                'availability_error': availability_error, 'physical_cluster_reinitialized': reinitialized})
        h.check(reinitialized,
                'Physical PostgreSQL reinitialization is independently established; a reboot alone is insufficient destructive-loss proof')
        h.check(availability_error is None, 'Data preservation does not hide any failure in the continuous wipe observation')


if __name__ == '__main__':
    p = parser('Run official wipe-node only after verifying a fresh off-cluster backup')
    p.add_argument('--backup-manifest', type=Path, required=True)
    p.set_defaults(timeout=600, repeat=1)
    raise SystemExit(run('tester-agent/08-wipe-node', ['Official destructive practice and known-record preservation'], p, scenario))
