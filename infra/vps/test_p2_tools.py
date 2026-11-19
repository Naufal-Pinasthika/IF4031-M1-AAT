import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


def module(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'))
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


load=module('test-p2-load')
control=module('test-p2-control')
collector=module('collect-p2')
capture=module('p2_log_capture')
profiles=module('p2_profiles')
routing=module('analyze-p2-routing')
laptop=module('test-p2-laptop')
clock=module('probe-clock')
clock_experiment=module('test-p2-clock')
capacity_observer=module('observe-p2-capacity')
capacity_contract=module('check-p2-capacity')


class ToolsTests(unittest.TestCase):
    def test_capacity_observer_and_routing_remote_code_compile(self):
        compile(load.RUNNER,'<scoped-generator>','exec')
        self.assertIn("(p/'cgroup').read_text()",load.RUNNER)
        self.assertIn("if own_scope and cmd.startswith",load.RUNNER)
        compile(capacity_observer.REMOTE,'<capacity-observer>','exec')
        compile(routing.REMOTE,'<routing-observer>','exec')
        self.assertIn('gateway_phase_mean_ms',routing.REMOTE)
        self.assertIn('gateway_queue_expired_total',routing.REMOTE)
        compile(capacity_contract.REMOTE,'<capacity-contract>','exec')
        self.assertIn('/sys/fs/cgroup/cpu.stat',capacity_observer.REMOTE)
        self.assertIn('timeout=8',capacity_observer.REMOTE)

    def test_clock_integrity_requires_samples_and_both_clocks(self):
        clean=dict(samples=10,backward_wall_offset_changes=0,monotonic_backwards=0)
        self.assertTrue(clock_experiment.clock_is_stable(clean))
        self.assertFalse(clock_experiment.clock_is_stable({}))
        self.assertFalse(clock_experiment.clock_is_stable(dict(clean,samples=0)))
        self.assertFalse(clock_experiment.clock_is_stable(dict(clean,backward_wall_offset_changes=1)))
        self.assertFalse(clock_experiment.clock_is_stable(dict(clean,monotonic_backwards=1)))

    def test_clock_probe_detects_backwards_offset_without_changing_time(self):
        with patch.object(clock.time,'monotonic_ns',side_effect=[0,1,2,3,4,5,6,10**9,10**9]), \
             patch.object(clock.time,'time_ns',side_effect=[10_000_000,8_000_000]), \
             patch.object(clock.time,'sleep'), patch('builtins.print'):
            result=clock.observe(1)
        self.assertEqual(result['samples'],2)
        self.assertEqual(result['backward_wall_offset_changes'],1)
        self.assertEqual(result['monotonic_backwards'],0)

    def test_clock_probe_does_not_confuse_read_scheduling_with_proven_step(self):
        with patch.object(clock.time,'monotonic_ns',side_effect=[0,1,2,1_000_000,1_000_001,1_000_002,2_000_000,10**9,10**9]), \
             patch.object(clock.time,'time_ns',side_effect=[10_000_000,11_000_000]), \
             patch.object(clock.time,'sleep'), patch('builtins.print'):
            result=clock.observe(1)
        self.assertEqual(result['samples'],2)
        self.assertEqual(result['backward_wall_offset_changes'],0)

    def test_native_negative_receiving_invalidates_even_positive_total_duration(self):
        metrics={'http_req_duration':{'min':2},'http_req_receiving':{'min':-.5}}
        self.assertFalse(load.timing_integrity(metrics)['valid'])
        metrics['http_req_receiving']['min']=0
        self.assertTrue(load.timing_integrity(metrics)['valid'])
        metrics['http_req_duration']['min']=float('nan')
        self.assertFalse(load.timing_integrity(metrics)['valid'])

    def test_laptop_throughput_requires_both_ordered_pairs_and_nonzero_baseline(self):
        self.assertEqual(laptop.compare([100,220,210,100]),([2.2,2.1],True))
        self.assertFalse(laptop.compare([100,220,190,100])[1])
        self.assertFalse(laptop.compare([0,220,210,100])[1])

    def test_routing_review_counts_only_business_route_and_keeps_rejections(self):
        def sample(node,path,status,count):
            return {'metric':{'node':node,'path':path,'status':status},'value':[0,str(count)]}
        case={'request_counter_increases':[
            sample('A','GET /internal/events','200',20),sample('A','GET /internal/events','429',10),
            sample('B','GET /internal/events','200',30),sample('A','GET /ready','200',1000)]}
        result=routing.traffic_summary(case)
        self.assertEqual(result['A']['all'],30)
        self.assertEqual(result['A']['429'],10)
        self.assertEqual(result['A']['share_percent'],50)
        self.assertEqual(result['B']['share_percent'],50)
        compile(routing.REMOTE,'<routing-review>','exec')

    def test_pause_identity_distinguishes_restart_from_stale_process_resume(self):
        original={'status':{'containerStatuses':[{'name':'aggregator','containerID':'containerd://old','restartCount':0}]}}
        restarted={'status':{'containerStatuses':[{'name':'aggregator','containerID':'containerd://new','restartCount':1}]}}
        self.assertNotEqual(control.task_identity(original),control.task_identity(restarted))
        self.assertIsNone(control.task_identity({})['container_id'])

    def test_worker_owner_setup_does_not_force_existing_worker_leadership(self):
        test=object.__new__(control.Test)
        test.server={'name':'A'}
        test.pods=Mock(return_value=[{'metadata':{'name':'owner'},'spec':{'nodeName':'B'}}])
        test.leases=Mock(return_value=[{'source':'BMKG','holder_id':'owner'}])
        test.kube=Mock()
        test.worker_owner()
        test.kube.assert_not_called()

    def test_worker_owner_setup_records_normal_handoff_instead_of_editing_leases(self):
        test=object.__new__(control.Test)
        test.server={'name':'A'}
        test.pods=Mock(return_value=[{'metadata':{'name':'owner'},'spec':{'nodeName':'A'}}])
        before={'source':'BMKG','holder_id':'owner'}
        test.leases=Mock(return_value=[before])
        test.kube=Mock()
        test.mark=Mock()
        test.wait_successor=Mock(return_value={'seconds_to_completed_poll':12})
        test.worker_owner()
        self.assertEqual(test.kube.call_args_list[0].args,('delete','pod','owner','--grace-period=0','--force','--wait=false'))
        self.assertEqual(test.wait_successor.call_args.args[0],before)
        self.assertEqual(test.mark.call_args.args[0]['case'],'worker-owner-setup')
        self.assertTrue(test.mark.call_args.args[0]['passed'])

    def test_profile_capture_is_bounded_and_private(self):
        compile(profiles.REMOTE,'<profile-capture>','exec')
        self.assertIn('127.0.0.1:6060/', profiles.REMOTE)
        self.assertIn('cpu?seconds=30', profiles.REMOTE)
        self.assertIn('timeout=38', profiles.REMOTE)

    def test_profile_capture_collects_samples_and_binary_without_a_cluster(self):
        import base64
        with tempfile.TemporaryDirectory() as directory:
            executable=Path(directory)/'k3s'
            executable.write_text('#!'+sys.executable+'\nimport sys,json\n'
                'if sys.argv[-1].endswith("runtime"): print(json.dumps({"gomaxprocs":2,"cpu_stat":"nr_periods 10"}))\n'
                'else: sys.stdout.buffer.write(b"fixture-profile")\n')
            executable.chmod(0o755)
            result=subprocess.run([sys.executable,'-c',profiles.REMOTE.replace('time.sleep(3)','time.sleep(.001)'),
                json.dumps(dict(namespace='fixture',pod='fixture',container='aggregator-gateway'))],
                env=dict(os.environ,PATH=directory+os.pathsep+os.environ['PATH']),
                capture_output=True,text=True,timeout=5,check=True)
            capture=json.loads(result.stdout)
            self.assertEqual(len(capture['samples']),12)
            self.assertEqual(base64.b64decode(capture['cpu_profile_base64']),b'fixture-profile')

    def test_remote_log_capture_filters_real_json_lines_without_a_cluster(self):
        compile(capture.REMOTE,'<log-capture>','exec')
        with tempfile.TemporaryDirectory() as directory:
            executable=Path(directory)/'k3s'
            executable.write_text('#!'+sys.executable+'\nimport json\n'
                'print(json.dumps({"level":"INFO","msg":"http_request"}),flush=True)\n'
                'print(json.dumps({"level":"WARN","msg":"canonical_read_partial"}),flush=True)\n'
                'print(json.dumps({"level":"INFO","msg":"http_request","status":500}),flush=True)\n')
            executable.chmod(0o755)
            cfg=dict(namespace='fixture',pod='fixture',stop=str(Path(directory)/'stop'))
            result=subprocess.run([sys.executable,'-c',capture.REMOTE,json.dumps(cfg)],
                env=dict(os.environ,PATH=directory+os.pathsep+os.environ['PATH']),
                capture_output=True,text=True,timeout=5,check=True)
            lines=[json.loads(line) for line in result.stdout.splitlines()]
            self.assertEqual([line['msg'] for line in lines],['canonical_read_partial','http_request'])

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

    def test_collector_preserves_postgres_short_fraction_on_python310(self):
        code=collector.render({'poll_id':49291,'database_time':'2026-11-19T12:20:14.52875+00:00'},'/opt/aat/repo/.e2e/test')
        compile(code,'<collector>','exec')
        self.assertIn('2026-11-19T12:20:14.528750+00:00',code)
        self.assertNotIn('START',code)

    def test_rolling_requires_observed_ready_binaries_not_only_deployment_tag(self):
        sample={'desired':3,'pods':[dict(ready=True,image='new',image_id='sha256:1') for _ in range(3)]}
        self.assertTrue(load.rollout_observed([sample],'new'))
        self.assertFalse(load.rollout_observed([sample],'old'))
        sample['pods'][0]['ready']=False
        self.assertFalse(load.rollout_observed([sample],'new'))
        self.assertFalse(load.rollout_observed([{'desired':3,'pods':[]}],'new'))

    def test_gateway_rolling_requires_both_ready_binaries(self):
        sample=dict(desired=2,pods=[dict(ready=True,image='new',image_id='sha256:fixture') for _ in range(2)])
        self.assertFalse(load.rollout_observed([sample],'new'))
        self.assertTrue(load.rollout_observed([sample],'new',minimum_replicas=2))
        sample['pods'][0]['ready']=False
        self.assertFalse(load.rollout_observed([sample],'new',minimum_replicas=2))

    def test_scale_never_hides_gateway_oom_or_replacement(self):
        sample=dict(gateways=[dict(uid=uid,containers=[dict(restartCount=0)]) for uid in ('a','b')])
        self.assertTrue(load.gateways_stable([sample],{'a':0,'b':0}))
        self.assertFalse(load.gateways_stable([],{}))
        self.assertFalse(load.gateways_stable([{}],{'a':0,'b':0}))
        self.assertFalse(load.gateways_stable([sample],{'a':0,'old':0}))
        sample['gateways'][0]['containers'][0]['restartCount']=1
        self.assertFalse(load.gateways_stable([sample],{'a':0,'b':0}))

    def test_429_cannot_dilute_non_rejected_semantic_failures(self):
        metrics={name:dict(count=value) for name,value in dict(responses_complete=90,responses_partial=5,responses_malformed=2,responses_other=3,responses_429=100000).items()}
        self.assertEqual(load.non_rejected_error_rate(metrics),.1)
        self.assertEqual(load.non_rejected_error_rate({'responses_429':{'count':100000}}),1)
        self.assertEqual(load.non_rejected_error_rate({'responses_complete':{'values':{'count':100}}}),0)


if __name__=='__main__':unittest.main()
