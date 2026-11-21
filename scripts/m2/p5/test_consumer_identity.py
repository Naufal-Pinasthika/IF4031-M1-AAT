import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    'identity_catchup', Path(__file__).with_name('09-consumer-identity-catchup.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class IdentityTests(unittest.TestCase):
    def test_equal_counts_with_different_ids_are_not_catchup(self):
        self.assertEqual(m.missing_ids({'a', 'b'}, [
            {'hazard_id': 'a'}, {'hazard_id': 'c'}]), {'b'})

    def test_newer_dashboard_may_have_additional_identities(self):
        self.assertFalse(m.missing_ids({'a'}, [
            {'hazard_id': 'a'}, {'hazard_id': 'b'}]))

    def test_duplicate_dashboard_identity_is_an_error(self):
        with self.assertRaises(RuntimeError):
            m.missing_ids({'a'}, [{'hazard_id': 'a'}, {'hazard_id': 'a'}])


if __name__ == '__main__':
    unittest.main()
