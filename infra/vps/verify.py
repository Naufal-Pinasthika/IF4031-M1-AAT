#!/usr/bin/env python3
"""Read-only deployment evidence and platform access-boundary verification."""
import datetime
import json
import socket
from provision import Hosts, ROOT, inventory

NS = 'sistem-informasi-kebencanaan-bnpb'


def main():
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    a = h.nodes[0]
    output = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'nodes': [], 'public_ports': []}
    for node in h.nodes:
        boot = h.ssh(node, 'cat /proc/sys/kernel/random/boot_id', capture=True).decode().strip()
        output['nodes'].append({'name': node['name'], 'boot_id': boot})
        for port in (5432, 6443, 9092, 30083):
            try:
                with socket.create_connection((node['host'], port), timeout=1):
                    reachable = True
            except OSError:
                reachable = False
            output['public_ports'].append({'node': node['name'], 'port': port, 'reachable': reachable})
    assert len({n['boot_id'] for n in output['nodes']}) == 3, 'shared guest kernel identity'
    assert not any(p['reachable'] for p in output['public_ports']), 'internal port publicly reachable'
    kube = 'sudo -n k3s kubectl -n ' + NS
    # Owners must connect; an unrelated API pod must not. Tester Agent is
    # explicitly exempted by the existing NetworkPolicy, not blocked here.
    h.ssh(a, kube + ' exec deployment/aggregator -- nc -z -w 3 postgres-a 5432')
    h.ssh(a, kube + ' exec deployment/client-api -- sh -c "if nc -z -w 3 postgres-a 5432; then exit 1; else echo non-owner-denied; fi"')
    output['canonical_owner_boundary'] = 'passed'
    h.ssh(a, kube + ' exec deployment/auth -- nc -z -w 3 auth-postgres 5432')
    h.ssh(a, kube + ' exec deployment/client-api -- sh -c "if nc -z -w 3 auth-postgres 5432; then exit 1; else echo non-owner-denied; fi"')
    output['auth_owner_boundary'] = 'passed'
    output['pods'] = json.loads(h.ssh(a, kube + ' get pods -o json', capture=True))
    # Capture only operational metadata, not container env/configuration.
    output['pods'] = [{'name': p['metadata']['name'], 'node': p['spec']['nodeName'],
                       'phase': p['status']['phase'],
                       'restarts': sum(c['restartCount'] for c in p['status'].get('containerStatuses', []))}
                      for p in output['pods']['items']]
    output['smoke'] = json.loads(h.ssh(a, 'sudo -n cat /opt/aat/repo/.e2e/smoke-results.json', capture=True))
    assert output['smoke']['passed'], 'latest application smoke test failed'
    destination = ROOT / '.vps'
    destination.mkdir(exist_ok=True, mode=0o700)
    (destination / 'verification.json').write_text(json.dumps(output, indent=2) + '\n')
    print('Distinct kernel boots, public port restrictions, owner-only storage and latest smoke: PASS')
    print('Evidence:', destination / 'verification.json')


if __name__ == '__main__':
    main()
