"""Safety-controller checks; no Kubernetes faults or HTTP load are injected."""
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import threading
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('direct_load', Path(__file__).with_name('run-direct-load.py'))
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class FakeProcess:
    pid = os.getpid()

    def __init__(self):
        self.signals = []

    def poll(self):
        return None

    def send_signal(self, value):
        self.signals.append(value)


class SafetyTests(unittest.TestCase):
    def test_previous_summary_cannot_masquerade_as_current_run(self):
        with tempfile.TemporaryDirectory(prefix='aat-evidence-test-') as directory:
            evidence = Path(directory)
            summary = evidence / 'case-100-summary.json'
            summary.write_text('{"old":true}')
            unrelated = evidence / 'case-1000-summary.json'
            unrelated.write_text('{"unrelated":true}')
            controller.archive_previous_run(evidence, 'case-100')
            self.assertFalse(summary.exists())
            self.assertTrue(unrelated.exists())
            archived = list((evidence / 'runs').glob('*/case-100-summary.json'))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_text(), '{"old":true}')

    def test_low_linux_memory_stops_only_generator(self):
        process = FakeProcess()
        result = {'peak_rss_mib': 0}
        with patch.object(controller, 'available_mib', return_value=100):
            controller.monitor_generator(process, SimpleNamespace(max_rss_mib=4096, min_free_mib=512), result, threading.Event(), None)
        self.assertEqual(process.signals, [signal.SIGINT])
        self.assertIn('Linux memory', result['safety_stop_reason'])

    def test_stale_host_observation_fails_closed(self):
        process = FakeProcess()
        result = {'peak_rss_mib': 0}
        host = {'observed_at': time.monotonic()-15, 'available_mib': 4096}
        with patch.object(controller, 'available_mib', return_value=4096):
            controller.monitor_generator(process, SimpleNamespace(max_rss_mib=4096, min_free_mib=512), result, threading.Event(), host)
        self.assertEqual(process.signals, [signal.SIGINT])
        self.assertIn('stale', result['safety_stop_reason'])

    def test_low_windows_physical_memory_stops_only_generator(self):
        process = FakeProcess()
        result = {'peak_rss_mib': 0}
        host = dict(observed_at=time.monotonic(), available_mib=4096, physical_available_mib=600)
        with patch.object(controller, 'available_mib', return_value=4096):
            controller.monitor_generator(process, SimpleNamespace(max_rss_mib=4096, min_free_mib=512), result, threading.Event(), host)
        self.assertEqual(process.signals, [signal.SIGINT])
        self.assertIn('physical memory', result['safety_stop_reason'])
        self.assertEqual(result['lowest_windows_physical_available_mib'], 600)

    @unittest.skipUnless(Path('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe').exists(), 'WSL Windows interop required')
    def test_native_host_monitor_starts_and_exits_cleanly(self):
        script = subprocess.check_output(['wslpath', '-w', str(Path(__file__).with_name('windows-memory-monitor.ps1'))], text=True).strip()
        helper = subprocess.Popen(['/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe',
            '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', script],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        state = {}
        reader = threading.Thread(target=controller.read_host_memory, args=(helper, state), daemon=True)
        reader.start()
        try:
            deadline = time.monotonic()+20
            while 'observed_at' not in state and helper.poll() is None and time.monotonic() < deadline:
                time.sleep(0.1)
            self.assertIn('observed_at', state, 'native monitor produced no sample')
            self.assertGreater(state['available_mib'], 0)
            self.assertGreater(state['physical_available_mib'], 0)
            helper.stdin.write('\n')
            helper.stdin.flush()
            self.assertEqual(helper.wait(timeout=5), 0)
        finally:
            if helper.poll() is None:
                try:
                    helper.stdin.write('\n')
                    helper.stdin.flush()
                    helper.wait(timeout=5)
                except (BrokenPipeError, subprocess.TimeoutExpired):
                    helper.terminate()
                    helper.wait(timeout=5)
            reader.join(timeout=2)
            helper.stdin.close()
            helper.stdout.close()
            helper.stderr.close()


if __name__ == '__main__':
    unittest.main()
