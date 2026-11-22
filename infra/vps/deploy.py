#!/usr/bin/env python3
"""One command to provision (optional), build remotely, import and deploy.

No source, credentials or images are pushed to public registries.
Uses existing inventory.json; retains PVCs and secrets on redeployment.
"""
import argparse
import concurrent.futures
import datetime
import json
from pathlib import Path
import re
import shlex
import subprocess
from provision import Hosts, ROOT, inventory
from vps_config import NAMESPACE as NS, SERVICE_ENDPOINTS

TESTER = 'https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml'


def import_worker(hosts, node, tag):
    source = subprocess.Popen(hosts.command(hosts.nodes[0], f'sudo -n cat /opt/aat/images/{tag}.tar', multiplex=False), stdout=subprocess.PIPE)
    try:
        target = subprocess.run(hosts.command(node, 'sudo -n k3s ctr images import -', multiplex=False), stdin=source.stdout, timeout=300)
        source.stdout.close()
        if source.wait() != 0 or target.returncode != 0:
            raise RuntimeError('Image transfer/import failed on ' + node['name'])
    finally:
        if source.poll() is None:
            source.terminate()
            source.wait()


def services():
    result = []
    for name, port, nodeport, backend in SERVICE_ENDPOINTS:
        result.append({'apiVersion': 'v1', 'kind': 'Service',
                       'metadata': {'name': name + '-vps', 'namespace': NS},
                       'spec': {'type': 'NodePort', 'selector': {'app': backend},
                                'ports': [{'port': port, 'targetPort': port, 'nodePort': nodeport}]}})
        if backend == 'aggregator-gateway':
            result[-1]['spec'].update(trafficDistribution='PreferSameNode',
                                     externalTrafficPolicy='Cluster', internalTrafficPolicy='Cluster')
    return {'apiVersion': 'v1', 'kind': 'List', 'items': result}


def deploy(hosts, tag, skip_build=False, no_cache=False, build_only=False):
    server = hosts.nodes[0]
    hosts.ssh(server, 'sudo -n install -d -m 700 /opt/aat/repo /opt/aat/backups')
    ssh = shlex.join(hosts.command(server, '')[:-2])
    subprocess.run(['rsync', '-az', '--rsync-path=sudo -n rsync', '-e', ssh,
                    '--exclude=.git/', '--exclude=.env', '--exclude=.e2e/', '--exclude=.vps/',
                    '--exclude=__pycache__/', '--exclude=infra/vps/inventory.json', '--exclude=*.pem',
                    str(ROOT) + '/', server['user'] + '@' + server['host'] + ':/opt/aat/repo/'], check=True)
    # These directories contain disposable build inputs, not runtime data. A
    # plain overlay leaves deleted Go/Python files active on the next build.
    # Limit deletion to verified source copies; never delete the repo root,
    # .env, evidence, image archives, or Kubernetes persistent volumes.
    for directory in ('services', 'scripts'):
        target = '/opt/aat/repo/' + directory
        hosts.ssh(server, 'test "$(readlink -f ' + target + ')" = ' + target)
        subprocess.run(['rsync', '-az', '--delete', '--rsync-path=sudo -n rsync', '-e', ssh,
                        '--exclude=.env', '--exclude=__pycache__/', '--exclude=*.pem',
                        str(ROOT / directory) + '/',
                        server['user'] + '@' + server['host'] + ':' + target + '/'], check=True)
    env_path = ROOT / '.env'
    if not env_path.is_file():
        raise ValueError('Create .env with scripts/setup-env.sh first; existing credentials are never replaced automatically')
    hosts.write(server, '/opt/aat/repo/.env', env_path.read_text())
    if not skip_build:
        build_option = ' --no-cache' if no_cache else ''
        hosts.ssh(server, f'sudo -n bash /opt/aat/repo/infra/vps/build.sh {tag}{build_option}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda n: import_worker(hosts, n, tag), hosts.nodes[1:]))
    if build_only:
        print('Images built/imported on all nodes; application workloads unchanged.', flush=True)
        return
    hosts.ssh(server, f'sudo -n bash -c {shlex.quote("cd /opt/aat/repo && bash scripts/deploy-k8s.sh --k3s --image-tag " + tag + " --timeout 600s")}')
    hosts.ssh(server, 'sudo -n k3s kubectl apply -f -', json.dumps(services()).encode())
    hosts.ssh(server, f'sudo -n k3s kubectl apply -f {TESTER}')
    hosts.ssh(server, 'sudo -n k3s kubectl -n tester-agent rollout status daemonset/tester-agent --timeout=300s')
    hosts.ssh(server, 'sudo -n install -d -m 700 /opt/aat/repo/.e2e; sudo -n install -m 600 /etc/rancher/k3s/k3s.yaml /opt/aat/repo/.e2e/kubeconfig')
    print('Deployment complete. Run access.py tunnel and access.py smoke.', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory', type=Path, default=ROOT / 'infra/vps/inventory.json')
    p.add_argument('--provision', action='store_true')
    p.add_argument('--skip-build', action='store_true', help='Reuse a previously built --tag')
    p.add_argument('--no-cache', action='store_true', help='Rebuild image layers without cache and pull base images')
    p.add_argument('--build-only', action='store_true', help='Build/import images without applying Kubernetes workloads')
    p.add_argument('--tag', default='m2-vps-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S'))
    args = p.parse_args()
    if args.skip_build and args.no_cache:
        p.error('--skip-build cannot be combined with --no-cache')
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}', args.tag):
        raise ValueError('invalid image tag')
    hosts = Hosts(inventory(args.inventory))
    if args.provision:
        hosts.provision()
    deploy(hosts, args.tag, args.skip_build, args.no_cache, args.build_only)


if __name__ == '__main__':
    main()
