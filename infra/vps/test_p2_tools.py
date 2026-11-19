import importlib.util
from pathlib import Path
import unittest


def module(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'))
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


load=module('test-p2-load')
control=module('test-p2-control')
collector=module('collect-p2')


class ToolsTests(unittest.TestCase):
    def test_legacy_threshold_failed_flag_is_not_ok_flag(self):
        self.assertTrue(load.threshold_ok(False))
        self.assertFalse(load.threshold_ok(True))

    def test_modern_threshold_and_metric_formats(self):
        self.assertTrue(load.threshold_ok({'ok':True}))
        self.assertFalse(load.threshold_ok({'ok':False}))
        self.assertEqual(load.metric_values({'values':{'rate':5}}),{'rate':5})
        self.assertEqual(load.metric_values({'rate':5}),{'rate':5})

    def test_sql_literal_is_not_shell_quoting(self):
        self.assertEqual(control.literal('BMKG'),"'BMKG'")
        self.assertEqual(control.literal("a'b"),"'a''b'")

    def test_generated_collector_is_valid_python(self):
        code=collector.render({'poll_id':123,'database_time':'2026-11-19 08:00:00.123456+00'},'/opt/aat/repo/.e2e/test')
        compile(code,'<collector>','exec')
        self.assertNotIn('START',code)

    def test_rolling_requires_observed_ready_binaries_not_only_deployment_tag(self):
        sample={'desired':3,'pods':[dict(ready=True,image='new',image_id='sha256:1') for _ in range(3)]}
        self.assertTrue(load.rollout_observed([sample],'new'))
        self.assertFalse(load.rollout_observed([sample],'old'))
        sample['pods'][0]['ready']=False
        self.assertFalse(load.rollout_observed([sample],'new'))
        self.assertFalse(load.rollout_observed([{'desired':3,'pods':[]}],'new'))


if __name__=='__main__':unittest.main()
