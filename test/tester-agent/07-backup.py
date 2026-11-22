from pathlib import Path
import json
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from common import NS, parser, run


WRITER = '''import hashlib,json,os,pathlib,shutil,sys
os.umask(0o077)
p=pathlib.Path.home()/sys.argv[1];p.parent.mkdir(parents=True,exist_ok=True)
if p.exists(): raise RuntimeError('Refuse backup overwrite')
digest=hashlib.sha256();size=0
with p.with_suffix(p.suffix+'.partial').open('xb') as output:
 while chunk:=sys.stdin.buffer.read(1048576):
  if shutil.disk_usage(p.parent).free<1073741824: raise RuntimeError('Backup storage headroom exhausted')
  output.write(chunk);digest.update(chunk);size+=len(chunk)
p.with_suffix(p.suffix+'.partial').rename(p)
print(json.dumps({'name':p.name,'bytes':size,'sha256':digest.hexdigest()}))
'''


def scenario(h):
    h.changes()
    h.check(h.args.vps and h.args.repeat == 1, 'One backup of the real VPS lab is created outside the cluster per invocation')
    destination = h.hosts.data['load_generator']
    h.check(destination['host'] not in {node['host'] for node in h.hosts.nodes}, 'Backup storage is outside every application node')
    remote_folder = '.local/state/aat-backups/'+h.run_id
    free = int(h.hosts.ssh(destination, 'python3 -c '+shlex.quote('import pathlib,shutil;print(shutil.disk_usage(pathlib.Path.home()).free)'), capture=True))
    h.check(free >= 8*1024**3, 'External backup host has at least 8 GiB headroom')
    resources = json.loads(h.kube('get', 'deployments,statefulsets,services,configmaps,secrets,pvc,networkpolicies', '-o', 'json'))
    h.artifact('private-resources.json', resources)
    claims = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
    volumes = json.loads(h.kube('get', 'pv', '-o', 'json'))['items']
    h.artifact('private-volumes.json', volumes)
    manifest = {'status': 'incomplete', 'namespace': NS, 'run_id': h.run_id, 'files': [],
        'destination': 'load_generator', 'remote_folder': remote_folder,
        'created_at': h.report['started_at'],
        'pvc_uids': {p['metadata']['name']: p['metadata']['uid'] for p in claims},
        'consistency': 'Transaction-consistent logical dump per database; clean offline Kafka/bbolt archives. Not a globally atomic snapshot.'}

    def remember(record):
        manifest['files'].append(record)
        h.artifact('backup-manifest.json', manifest)

    def writer(name):
        return 'python3 -c '+shlex.quote(WRITER)+' '+shlex.quote(remote_folder+'/'+name)

    def upload(name, payload):
        record = json.loads(h.hosts.ssh(destination, writer(name), payload, capture=True))
        remember(record)
        return record

    def capture(name, command, node=None):
        with (h.folder/(name+'.stderr')).open('wb') as errors:
            reader = subprocess.Popen(h.hosts.command(node or h.server, command), stdout=subprocess.PIPE, stderr=errors)
            sink = subprocess.Popen(h.hosts.command(destination, writer(name)), stdin=reader.stdout,
                                    stdout=subprocess.PIPE, stderr=errors)
            reader.stdout.close()
            try:
                output, _ = sink.communicate(timeout=300)
                source_exit = reader.wait(timeout=30)
            finally:
                for process in (sink, reader):
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=10)
        h.check(source_exit == 0 and sink.returncode == 0, 'Backup stream completed: '+name)
        record = json.loads(output)
        h.check(record['bytes'] > 0, 'Backup archive is nonempty: '+name)
        remember(record)
        return record

    upload('private-resources.json', (h.folder/'private-resources.json').read_bytes())
    upload('private-volumes.json', (h.folder/'private-volumes.json').read_bytes())
    databases = [('aggregator-coordinator-0', 'aggregator_coord', 'coordination'),
                 ('auth-postgres-0', 'auth_app', 'auth')]
    databases += [('postgres-'+unit+'-0', 'postgres', 'aggregator') for unit in 'abc']
    for name, database in [('coordinator-ha', 'coordination'), ('notifier-ha', 'notifications')]:
        cluster = json.loads(h.kube('get', 'cluster', name, '--ignore-not-found', '-o', 'json') or '{}')
        if cluster:
            h.check(bool(cluster.get('status', {}).get('currentPrimary')), 'Staged database has a primary before backup: '+name)
            databases.append((cluster['status']['currentPrimary'], 'postgres', database))
    for pod, user, database in databases:
        base = ['sudo', '-n', 'k3s', 'kubectl', '-n', NS, 'exec', '-i', pod, '--']
        archive = capture(pod+'.dump', shlex.join(base+['pg_dump', '-U', user, '-d', database, '-Fc']))
        prefix = h.hosts.ssh(destination, 'head -c 1048576 ~/'+remote_folder+'/'+pod+'.dump', capture=True)
        h.check(prefix[:5] == b'PGDMP', 'PostgreSQL custom archive header is valid: '+pod)
        check = 'file=$(mktemp /dev/shm/aat-backup-check.XXXXXX); trap \'rm -f "$file"\' EXIT; ' \
            'head -c '+str(len(prefix))+' > "$file"; pg_restore --list "$file"'
        catalogue = subprocess.run(h.hosts.command(h.server, shlex.join(base+['sh', '-ec', check])),
            input=prefix, capture_output=True, check=True, timeout=30).stdout
        upload(pod+'.catalogue', catalogue)
        capture(pod+'.globals.sql', shlex.join(base+['pg_dumpall', '-U', user, '--globals-only']))
        h.note({'backup_database': pod, 'bytes': archive['bytes']})
    for app in ('kafka', 'dashboard-consumer', 'notifier-consumer'):
        workload = json.loads(h.kube('get', 'statefulset', app, '-o', 'json'))
        replicas = workload['spec']['replicas']
        h.check(replicas == 1, 'Offline archive is scoped to the current single-instance '+app)
        claim = next(p for p in claims if p['metadata']['name'] == 'data-'+app+'-0')
        volume = next(v for v in volumes if v['metadata']['name'] == claim['spec']['volumeName'])
        path = volume['spec'].get('hostPath', volume['spec'].get('local', {}))['path']
        node_name = volume['spec']['nodeAffinity']['required']['nodeSelectorTerms'][0]['matchExpressions'][0]['values'][0]
        node = next(n for n in h.hosts.nodes if n['name'] == node_name)
        resolved = h.hosts.ssh(node, 'sudo -n readlink -f -- '+shlex.quote(path), capture=True).decode().strip()
        h.check(resolved.startswith('/var/lib/rancher/k3s/storage/pvc-') and Path(resolved).name.endswith('_'+NS+'_data-'+app+'-0'),
                'Resolved archive target is exactly the application local PVC: '+app)
        try:
            h.kube('scale', 'statefulset/'+app, '--replicas=0')
            h.wait(lambda: not h.kube('get', 'pod', app+'-0', '--ignore-not-found', '-o', 'json').strip(),
                   'Pod did not stop cleanly before its offline archive: '+app)
            archive = capture(app+'.tar.gz', 'sudo -n tar -C '+shlex.quote(resolved)+' -czf - .', node=node)
            verify = 'import pathlib,gzip,tarfile;p=pathlib.Path.home()/'+repr(remote_folder+'/'+archive['name'])+'\n' \
                'with gzip.open(p,"rb") as stream:\n while stream.read(1048576): pass\n' \
                'with tarfile.open(p,"r:gz") as tar:\n assert all(not pathlib.PurePosixPath(m.name).is_absolute() and ".." not in pathlib.PurePosixPath(m.name).parts for m in tar)\n'
            h.hosts.ssh(destination, 'python3 -c '+shlex.quote(verify), capture=True)
        finally:
            h.kube('scale', 'statefulset/'+app, '--replicas='+str(replicas))
            h.kube('rollout', 'status', 'statefulset/'+app, '--timeout=180s')
    current = json.loads(h.kube('get', 'pvc', '-o', 'json'))['items']
    h.check(manifest['pvc_uids'] == {p['metadata']['name']: p['metadata']['uid'] for p in current},
            'Backup maintenance retained all existing volume identities')
    manifest['status'] = 'complete'
    h.artifact('backup-manifest.json', manifest)
    h.hosts.ssh(destination, writer('backup-manifest.json'), json.dumps(manifest).encode(), capture=True)
    h.note({'backup': str(h.folder/'backup-manifest.json'), 'files': len(manifest['files']),
            'bytes': sum(f['bytes'] for f in manifest['files']), 'scope': manifest['consistency']})


if __name__ == '__main__':
    p = parser('Create private off-cluster database and offline-volume backups before destructive Agent practice')
    p.set_defaults(repeat=1, timeout=150)
    raise SystemExit(run('tester-agent/07-backup', ['Destructive-test safety prerequisite, not fault tolerance acceptance'], p, scenario))
