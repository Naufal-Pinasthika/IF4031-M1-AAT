import json
import threading
import unittest
from unittest.mock import patch

import server


class TraceTests(unittest.TestCase):
    def test_ingress_is_read_before_slow_downstream_tails_evict_its_record(self):
        ingress_started = threading.Event()
        evicted = threading.Event()
        pods = [{'metadata': {'name': name, 'labels': {'app': app}}, 'spec': {'nodeName': 'test'}}
                for name, app in [('agg-0', 'aggregator'), ('agg-1', 'aggregator'),
                                 ('agg-2', 'aggregator'), ('gateway-0', 'aggregator-gateway'),
                                 ('gateway-1', 'aggregator-gateway'), ('client', 'client-api')]]

        def logs(path):
            if path.startswith('pods/client/'):
                ingress_started.set()
                if evicted.is_set():
                    return b''
            elif not ingress_started.wait(.2):
                evicted.set()
            return json.dumps({'correlation_id': 'trace-test', 'msg': 'completed', 'latency_ms': 3}).encode()

        with patch.object(server, 'pods', return_value=pods), patch.object(server, 'kube', side_effect=logs):
            result = server.trace('trace-test')
        self.assertFalse(evicted.is_set())
        self.assertEqual({step['service'] for step in result['steps']},
                         {'client-api', 'aggregator', 'aggregator-gateway'})
        self.assertEqual(len(result['coverage']), 6)
        self.assertFalse(result['complete'])

    def test_bounded_trace_still_reports_missing_logs_and_redacts_credentials(self):
        pods = [{'metadata': {'name': app, 'labels': {'app': app}}, 'spec': {}}
                for app in ('client-api', 'aggregator')]

        def logs(path):
            if path.startswith('pods/aggregator/'):
                raise TimeoutError()
            return json.dumps({'correlation_id': 'trace-test', 'error': 'Bearer private-value',
                               'authorization': 'private-value'}).encode()

        with patch.object(server, 'pods', return_value=pods), patch.object(server, 'kube', side_effect=logs):
            result = server.trace('trace-test')
        self.assertNotIn('private-value', json.dumps(result))
        self.assertTrue(any(not row['read'] for row in result['coverage']))
        self.assertFalse(result['complete'])


if __name__ == '__main__':
    unittest.main()
