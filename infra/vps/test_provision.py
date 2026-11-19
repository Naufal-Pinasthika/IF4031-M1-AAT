import json
from pathlib import Path
import tempfile
import unittest
import yaml
from provision import ROOT, inventory
from deploy import services


class ProvisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.key = Path(self.temp.name) / 'key'
        self.key.touch()
        self.data = json.loads((ROOT / 'infra/vps/inventory.example.json').read_text())
        self.data['key'] = str(self.key)
        for i, node in enumerate(self.data['nodes'], 1):
            node['host'] = f'192.0.2.{i}'
        self.path = Path(self.temp.name) / 'inventory.json'

    def tearDown(self):
        self.temp.cleanup()

    def validate(self):
        self.path.write_text(json.dumps(self.data))
        return inventory(self.path)

    def test_valid_inventory(self):
        self.assertEqual(len(self.validate()['nodes']), 3)

    def test_reject_duplicate_hosts(self):
        self.data['nodes'][1]['host'] = self.data['nodes'][0]['host']
        with self.assertRaises(ValueError):
            self.validate()

    def test_reject_shell_injection(self):
        self.data['nodes'][0]['user'] = 'ubuntu;whoami'
        with self.assertRaises(ValueError):
            self.validate()

    def test_reject_public_overlay(self):
        self.data['nodes'][1]['private_ip'] = '203.0.113.2'
        with self.assertRaises(ValueError):
            self.validate()

    def test_application_yaml(self):
        docs = list(yaml.safe_load_all((ROOT / 'k8s/base/apps/workloads.yaml').read_text()))
        agg = next(d for d in docs if d['kind'] == 'Deployment' and d['metadata']['name'] == 'aggregator')
        spread = agg['spec']['template']['spec']['topologySpreadConstraints'][0]
        self.assertEqual(spread['minDomains'], 3)
        self.assertEqual(spread['matchLabelKeys'], ['pod-template-hash'])
        self.assertEqual(agg['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'], '512Mi')

    def test_nodeports_unique(self):
        resources = services()['items']
        self.assertEqual(len({r['spec']['ports'][0]['nodePort'] for r in resources}), 5)
        self.assertTrue(all(r['metadata']['namespace'] == 'sistem-informasi-kebencanaan-bnpb' for r in resources))

    def test_single_common_entry_and_bounded_gateway(self):
        docs=list(yaml.safe_load_all((ROOT/'k8s/base/apps/gateway.yaml').read_text()))
        headless=next(d for d in docs if d['kind']=='Service')
        self.assertEqual(headless['spec']['clusterIP'],'None')
        self.assertEqual(headless['spec']['selector'],{'app':'aggregator'})
        gateway=next(d for d in docs if d['kind']=='Deployment')
        pod=gateway['spec']['template']
        self.assertEqual(pod['metadata']['labels']['role'],'other')
        self.assertFalse(pod['spec']['automountServiceAccountToken'])
        limits=pod['spec']['containers'][0]['resources']
        self.assertEqual(limits['requests']['memory'],'128Mi')
        self.assertEqual(limits['limits']['memory'],'256Mi')
        nodeport=next(s for s in services()['items'] if s['metadata']['name']=='aggregator-vps')
        self.assertEqual(nodeport['spec']['selector'],{'app':'aggregator-gateway'})


if __name__ == '__main__':
    unittest.main()
