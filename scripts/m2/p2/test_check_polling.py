import unittest
from check_polling import check


def item(identifier, source):
    return dict(poll_id=identifier, source=source, holder_id='owner', fencing_token=3, status='completed')


def request(identifier, source, path):
    return dict(msg='http_request', service=source, path=path,
                correlation_id=f'poll-{source}-{identifier}', status=200)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.ledger = [{'items': [item(1, 'BMKG'), item(2, 'PVMBG')]}]
        self.logs = [request(1, 'BMKG', '/seismic-events'),
                     request(1, 'BMKG', '/tsunami-warnings'),
                     request(2, 'PVMBG', '/volcanic-reports')]

    def test_complete_evidence(self):
        self.assertTrue(check(self.ledger, self.logs)['passed'])

    def test_duplicate_request_fails(self):
        self.assertFalse(check(self.ledger, self.logs + self.logs[:1])['passed'])

    def test_missing_endpoint_fails(self):
        self.assertFalse(check(self.ledger, self.logs[1:])['passed'])

    def test_entire_missing_completed_cycle_fails(self):
        self.ledger[0]['items'].append(item(3, 'BMKG'))
        self.assertFalse(check(self.ledger, self.logs)['passed'])

    def test_empty_evidence_fails(self):
        self.assertFalse(check(self.ledger, [])['passed'])

    def test_missing_metadata_fails(self):
        self.assertFalse(check([{'items': []}], self.logs)['passed'])

    def test_unattributed_business_requests_fail(self):
        self.logs[0]['correlation_id'] = 'old-owner-without-ledger'
        self.assertFalse(check(self.ledger, self.logs)['passed'])


if __name__ == '__main__':
    unittest.main()
