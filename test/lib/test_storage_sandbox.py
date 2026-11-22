import copy
import types
import unittest
from unittest.mock import Mock, patch

from sandbox import Sandbox
from storage import StorageSandbox


class StorageSandboxGatewayTests(unittest.TestCase):
    def test_ready_gateway_pod_image_works_without_a_gateway_controller_lookup(self):
        manifests = []

        def kube(*args):
            if args[:3] in (('get', 'deployment', 'aggregator-gateway'),
                             ('get', 'daemonset', 'aggregator-gateway')):
                raise AssertionError('Gateway image discovery must not depend on controller kind')
            if args[:2] == ('get', 'service'):
                return '10.43.0.10'
            if args[0] in ('rollout', 'patch'):
                return ''
            raise AssertionError('Unexpected remote operation: ' + repr(args))

        def check(condition, message):
            self.assertTrue(condition, message)

        # Normalized pod metadata returned by Harness.pods(), independent of controller kind.
        pods = {
            'ready-gateway': {
                'name': 'aggregator-gateway-abcde', 'app': 'aggregator-gateway',
                'node': 'aat-vps-a', 'ready': True, 'restarts': 0,
                'images': ['aat/aggregator:deployed-gateway'], 'dns_config': {}, 'resources': {},
            },
            'unready-gateway': {
                'name': 'aggregator-gateway-fghij', 'app': 'aggregator-gateway',
                'node': 'aat-vps-b', 'ready': False, 'restarts': 0,
                'images': ['aat/aggregator:unready-version'], 'dns_config': {}, 'resources': {},
            },
        }
        harness = types.SimpleNamespace(
            urls={'aggregator': 'http://original'}, sql=lambda sql: None,
            hosts=types.SimpleNamespace(nodes=[{'name': 'aat-vps-' + c} for c in 'abc']),
            kube=kube, check=check, pods=lambda: pods,
        )
        lab = StorageSandbox(harness)
        lab.tunnel = Mock()
        lab.apply = lambda objects: manifests.extend(copy.deepcopy(objects))
        lab.open_tunnel = Mock()
        # Skip only the unrelated remote base setup and error cleanup.
        with patch.object(Sandbox, '__enter__', return_value=lab), \
                patch.object(StorageSandbox, '__exit__', return_value=None):
            result = lab.__enter__()
        self.assertIs(result, lab)
        gateway = next(obj for obj in manifests if obj['kind'] == 'Deployment')
        container = gateway['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['image'], 'aat/aggregator:deployed-gateway')
        self.assertEqual(container['command'], ['/app/gateway'])
        self.assertEqual(gateway['spec']['replicas'], 2)


if __name__ == '__main__':
    unittest.main()
