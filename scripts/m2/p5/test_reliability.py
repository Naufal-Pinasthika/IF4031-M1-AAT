import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('reliability',Path(__file__).with_name('06-reliability.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class ObserverTests(unittest.TestCase):
    def simulate(self,new=True,confirm=True):
        clock=[1000.0];calls=[0]
        item=dict(observation_id='arbitrary-random-identity',station_id='s',observed_at='2099-01-01T00:00:00Z',
                  sea_level_anomaly_m=1,wave_height_m=2,quality='ok',critical=True)
        notification=dict(hazard_id=m.hazard(item['observation_id']),source='SENSOR_PANTAI',critical=True,
                          created_at='1970-01-01T00:00:00Z')
        def request(port,path,token=None,method='GET',payload=None):
            clock[0]+=.01
            if 'stations' in path:return 200,{'items':[{'station_id':'s'}]},10
            if 'confirm' in path:return (200,{'notification':notification,'durability_confirmed':True},10) if confirm else (404,{},10)
            calls[0]+=1
            return 200,{'items':[item] if new and calls[0]==2 else [],'next_cursor':'cursor'},10
        with patch.object(m,'request',side_effect=request),patch.object(m,'notifications',side_effect=[{}, {notification['hazard_id']:notification}]):
            with patch.object(m.time,'monotonic',side_effect=lambda:clock[0]),patch.object(m.time,'sleep',side_effect=lambda n:clock.__setitem__(0,clock[0]+n)):
                return m.run_round(SimpleNamespace(seconds=60,drain=0),1,'baseline','unit-only')
    def test_uses_monotonic_confirmed_upper_bound_not_source_or_notifier_clock(self):
        result=self.simulate()
        self.assertTrue(result['passed'],result)
        self.assertGreater(result['latency_bound_p95_ms'],0)
        self.assertLess(result['latency_bound_p95_ms'],1000)
    def test_no_new_samples_cannot_pass(self):
        result=self.simulate(new=False)
        self.assertFalse(result['passed'])
        self.assertIsNone(result['latency_bound_p95_ms'])
        self.assertEqual(result['latency_status'],'no_new_samples')
    def test_visible_without_durability_confirmation_cannot_pass(self):
        result=self.simulate(confirm=False)
        self.assertFalse(result['passed'])
        self.assertEqual(result['missing_critical'],0)
        self.assertEqual(result['measured_new_critical'],0)
        self.assertEqual(len(result['unverified_time_ids']),1)


if __name__=='__main__':unittest.main()
