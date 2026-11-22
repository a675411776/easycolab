"""Unit tests for Windows client 1:1 parity (keepalive_daemon, wake lifecycle, &touch=1, sleep assertion) and v1.2.0 invariants."""

from __future__ import annotations

import ast
import ctypes
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Provide lightweight headless PySide6 stub if PySide6 is not installed in the bridge venv
try:
    from PySide6.QtCore import QCoreApplication, QObject, QTimer, Signal  # noqa: F401
except ImportError:
    class _BoundStubSignal:
        def __init__(self):
            self._slots = []

        def connect(self, fn):
            self._slots.append(fn)

        def emit(self, *args):
            for fn in list(self._slots):
                fn(*args)

    class _StubSignal:
        def __init__(self, *types_spec):
            self._attr_key = f"_sig_{id(self)}"

        def __get__(self, instance, owner=None):
            if instance is None:
                return self
            sig = instance.__dict__.get(self._attr_key)
            if sig is None:
                sig = _BoundStubSignal()
                instance.__dict__[self._attr_key] = sig
            return sig

    class _StubQObject:
        def __init__(self, parent=None):
            self._parent = parent

    class _StubQTimer:
        def __init__(self, parent=None):
            self.timeout = _BoundStubSignal()
            self._interval = 0
            self._active = False

        def setInterval(self, ms: int):
            self._interval = ms

        def start(self):
            self._active = True

        def stop(self):
            self._active = False

        @staticmethod
        def singleShot(_ms: int, fn):
            if callable(fn):
                fn()

    class _StubQCoreApplication:
        _inst = None

        @classmethod
        def instance(cls):
            return cls._inst

    qtcore_mod = types.ModuleType("PySide6.QtCore")
    qtcore_mod.QObject = _StubQObject
    qtcore_mod.QTimer = _StubQTimer
    qtcore_mod.Signal = _StubSignal
    qtcore_mod.QCoreApplication = _StubQCoreApplication

    pyside_mod = types.ModuleType("PySide6")
    pyside_mod.QtCore = qtcore_mod
    sys.modules["PySide6"] = pyside_mod
    sys.modules["PySide6.QtCore"] = qtcore_mod

import colab_bridge
from windows_app.viewmodel import EasyColabViewModel

APP_PY_PATH = os.path.join(REPO_ROOT, "app.py")
BRIDGE_PY_PATH = os.path.join(REPO_ROOT, "colab_bridge.py")


class WindowsViewModelParityTest(unittest.TestCase):
    def _make_vm(self, tmp_dir: str) -> EasyColabViewModel:
        cfg_dir = Path(tmp_dir) / "cfg"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        save_dir = Path(tmp_dir) / "media"
        save_dir.mkdir(parents=True, exist_ok=True)
        with (
            mock.patch("windows_app.viewmodel.get_app_config_dir", return_value=cfg_dir),
            mock.patch("windows_app.viewmodel.get_local_save_dir", return_value=save_dir),
            mock.patch("windows_app.viewmodel_services.get_app_config_dir", return_value=cfg_dir),
            mock.patch("windows_app.viewmodel_services.get_local_save_dir", return_value=save_dir),
        ):
            vm = EasyColabViewModel()
        # Execute UI callbacks synchronously in headless unit tests
        vm._dispatch_ui = lambda fn: fn()
        return vm

    def test_keepalive_daemon_streams_json_and_exits_promptly_on_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            self.addCleanup(vm.stop_keepalive_daemon)
            vm.cli_logged_in = True
            vm.is_connected = True
            vm.is_waking_colab = False

            daemon_script = Path(tmp) / "fake_daemon.py"
            daemon_script.write_text(
                "import json, sys, time\n"
                "payload = {'ok': True, 'logged_in': True, 'ws_connected': True, "
                "'paid_balance': 87.5, 'burn_rate_hourly': 5.31, 'active_assignments': 1}\n"
                "print('KEEPALIVE_JSON:' + json.dumps(payload), flush=True)\n"
                "sys.stdin.read()\n",
                encoding="utf-8",
            )

            # _resolve_python_for_bridge + ["keepalive_daemon"] -> run fake_daemon.py
            with mock.patch.object(
                vm,
                "_resolve_python_for_bridge",
                return_value=[sys.executable, str(daemon_script)],
            ):
                vm.ensure_keepalive_daemon_running()

                deadline = time.time() + 3.0
                while time.time() < deadline and not vm.keepalive_proxy_ws_connected:
                    time.sleep(0.03)

                self.assertTrue(vm._is_keepalive_daemon_alive())
                self.assertTrue(vm.keepalive_proxy_ws_connected)
                self.assertAlmostEqual(vm.official_cu_balance or 0.0, 87.5)
                self.assertAlmostEqual(vm.total_budget_cu, 87.5)
                self.assertAlmostEqual(vm.official_burn_rate or 0.0, 5.31)
                self.assertEqual(vm.active_vm_count, 1)

                proc = vm._keepalive_daemon_proc
                self.assertIsNotNone(proc)

                t0 = time.time()
                vm.stop_keepalive_daemon()
                elapsed = time.time() - t0
                self.assertLess(elapsed, 1.5)
                self.assertFalse(vm.keepalive_proxy_ws_connected)
                self.assertIsNone(vm._keepalive_daemon_proc)
                self.assertIsNotNone(proc.poll())

    def test_wake_passes_idle_timeout_filters_noise_and_guards_stale_url(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            vm.cli_logged_in = True
            vm.idle_timeout_minutes = 45
            vm.wake_gpu_choice = "G4"
            # Pre-populate a stale URL from a previous session
            vm.colab_server_url = "https://stale-tunnel.trycloudflare.com"

            captured_envs = []
            status_checked = []
            daemon_started = []

            class _FakeFailedWakeProc:
                def __init__(self, cmd, **kwargs):
                    captured_envs.append(dict(kwargs.get("env") or {}))
                    self.stdout = io.StringIO(
                        "[colab] Pruned 1 stale local session(s).\n"
                        "urllib3/connectionpool.py: InsecureRequestWarning\n"
                        "Assignment request failed: retrying\n"
                        "WAKE_LOG:正在对账云端实例...\n"
                        "WAKE_ERROR:当前 Google Colab G4 算力池库存不足 (HTTP 503)\n"
                    )

                def poll(self):
                    return 0

                def wait(self, timeout=None):
                    return 0

            with (
                mock.patch.object(vm, "_resolve_python_for_bridge", return_value=[sys.executable, BRIDGE_PY_PATH]),
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=_FakeFailedWakeProc),
                mock.patch.object(vm, "refresh_official_colab_account_usage"),
                mock.patch.object(vm, "check_colab_status", side_effect=lambda **kw: status_checked.append(kw)),
                mock.patch.object(vm, "ensure_keepalive_daemon_running", side_effect=lambda: daemon_started.append(True)),
            ):
                vm.wake_colab_headless()
                deadline = time.time() + 2.0
                while time.time() < deadline and vm.is_waking_colab:
                    time.sleep(0.02)

            self.assertEqual(len(captured_envs), 1)
            self.assertEqual(captured_envs[0].get("EASYCOLAB_IDLE_TIMEOUT_MINUTES"), "45")
            self.assertFalse(vm._wake_obtained_url_this_run)
            # Because no WAKE_URL: was emitted in this run, stale URL must NOT be checked or daemon started!
            self.assertEqual(status_checked, [])
            self.assertEqual(daemon_started, [])
            joined_logs = "\n".join(vm.live_console_lines)
            self.assertNotIn("Pruned 1 stale", joined_logs)
            self.assertNotIn("urllib3", joined_logs)
            self.assertNotIn("Assignment request failed", joined_logs)
            self.assertIn("503", joined_logs)

            # Now simulate a successful WAKE_URL emission
            class _FakeSuccessWakeProc:
                def __init__(self, cmd, **kwargs):
                    self.stdout = io.StringIO(
                        "WAKE_LOG:分配成功\n"
                        "WAKE_URL:https://fresh-g4-tunnel.trycloudflare.com\n"
                    )

                def poll(self):
                    return 0

                def wait(self, timeout=None):
                    return 0

            with (
                mock.patch.object(vm, "_resolve_python_for_bridge", return_value=[sys.executable, BRIDGE_PY_PATH]),
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=_FakeSuccessWakeProc),
                mock.patch.object(vm, "refresh_official_colab_account_usage"),
                mock.patch.object(vm, "check_colab_status", side_effect=lambda **kw: status_checked.append(kw)),
                mock.patch.object(vm, "ensure_keepalive_daemon_running", side_effect=lambda: daemon_started.append(True)),
            ):
                vm.wake_colab_headless()
                deadline = time.time() + 2.0
                while time.time() < deadline and vm.is_waking_colab:
                    time.sleep(0.02)

            self.assertTrue(vm._wake_obtained_url_this_run)
            self.assertEqual(vm.normalized_base_url, "https://fresh-g4-tunnel.trycloudflare.com")
            self.assertEqual(len(status_checked), 1)
            self.assertEqual(status_checked[0].get("silent"), False)
            self.assertEqual(len(daemon_started), 1)

    def test_check_colab_status_touch_parameter_throttling(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            vm.set_colab_server_url("https://test-touch.trycloudflare.com")
            vm.set_admin_password("pw_test_123456")

            captured_params = []

            def fake_admin_req(method, url, **kwargs):
                captured_params.append(dict(kwargs.get("params") or {}))
                resp = mock.MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"gpu_name": "NVIDIA L4", "idle_timeout_minutes": vm.idle_timeout_minutes}
                return resp

            # Isolate background status sync calls
            with (
                mock.patch.object(vm, "_admin_request", side_effect=fake_admin_req),
                mock.patch.object(vm, "ensure_keepalive_daemon_running"),
                mock.patch.object(vm, "sync_api_key_to_colab"),
                mock.patch.object(vm, "sync_context_length_to_colab"),
                mock.patch.object(vm, "update_idle_timeout_config"),
            ):
                # 1. Explicit non-silent check -> includes touch=1
                vm._last_local_interaction_ts = time.time() - 120.0
                vm._last_remote_touch_sent_ts = 0.0
                vm.check_colab_status(silent=False)
                deadline = time.time() + 1.5
                while time.time() < deadline and vm._status_check_inflight:
                    time.sleep(0.01)
                self.assertEqual(captured_params[-1].get("touch"), 1)

                # 2. Silent check with stale local interaction (>30s ago) -> omits touch
                vm._last_local_interaction_ts = time.time() - 120.0
                vm.check_colab_status(silent=True)
                deadline = time.time() + 1.5
                while time.time() < deadline and vm._status_check_inflight:
                    time.sleep(0.01)
                self.assertNotIn("touch", captured_params[-1])

                # 3. Silent check with fresh local interaction (<30s) AND >=20s since last touch -> includes touch=1
                vm.record_local_interaction()
                vm._last_remote_touch_sent_ts = time.time() - 25.0
                vm.check_colab_status(silent=True)
                deadline = time.time() + 1.5
                while time.time() < deadline and vm._status_check_inflight:
                    time.sleep(0.01)
                self.assertEqual(captured_params[-1].get("touch"), 1)

                # 4. Immediate follow-up silent check (<20s cooldown) -> omits touch
                vm.check_colab_status(silent=True)
                deadline = time.time() + 1.5
                while time.time() < deadline and vm._status_check_inflight:
                    time.sleep(0.01)
                self.assertNotIn("touch", captured_params[-1])

    def test_windows_sleep_assertion_toggles_execution_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            recorded_flags = []
            fake_kernel32 = types.SimpleNamespace(
                SetThreadExecutionState=lambda arg: recorded_flags.append(int(arg.value))
            )
            fake_windll = types.SimpleNamespace(kernel32=fake_kernel32)

            with (
                mock.patch.object(sys, "platform", "win32"),
                mock.patch.object(ctypes, "windll", fake_windll, create=True),
            ):
                vm.is_connected = True
                vm.sync_windows_runtime_activity_assertion()
                self.assertTrue(vm._win_sleep_assertion_active)
                self.assertEqual(recorded_flags[-1], 0x80000001)

                vm.sync_windows_runtime_activity_assertion(force_clear=True)
                self.assertFalse(vm._win_sleep_assertion_active)
                self.assertEqual(recorded_flags[-1], 0x80000000)

    def test_custom_model_modality_prefers_image_over_minimax_substring(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            vm._register_custom_hf_model_offline("MiniMaxAI/MiniMax-Image-01", "", "", "auto")
            self.assertEqual(vm.custom_models[-1].modality.value, "image")

    def test_easycolab_win_bridge_cli_dispatches_keepalive_daemon_and_null_stdin_safe(self):
        # Verify colab_bridge._is_piped_stdin_available() handles sys.stdin=None (Windows GUI subsystem)
        with mock.patch.object(sys, "stdin", None):
            self.assertFalse(colab_bridge._is_piped_stdin_available())

        # Verify easycolab_win.py --bridge-cli keepalive_daemon routes straight to colab_bridge.run_cli(["keepalive_daemon"])
        win_entry = os.path.join(REPO_ROOT, "windows_app", "easycolab_win.py")
        code = (
            "import runpy, sys, unittest.mock as m, colab_bridge\n"
            "called = []\n"
            "m.patch.object(colab_bridge, 'run_cli', side_effect=lambda a: called.append(a)).start()\n"
            f"sys.argv = [{win_entry!r}, '--bridge-cli', 'keepalive_daemon']\n"
            "try:\n"
            f"    runpy.run_path({win_entry!r}, run_name='__main__')\n"
            "except SystemExit as e:\n"
            "    assert e.code in (0, None), f'unexpected exit code: {e.code}'\n"
            "assert called == [['keepalive_daemon']], f'unexpected: {called}'\n"
            "print('DISPATCH_OK')\n"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("DISPATCH_OK", proc.stdout)

    def test_is_releasing_colab_blocks_daemon_respawn_and_stale_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            vm.cli_logged_in = True
            vm.is_connected = True
            vm.is_releasing_colab = True

            spawned = []
            status_polls = []
            bridge_calls = []

            with (
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=lambda *a, **kw: spawned.append(a)),
                mock.patch.object(vm, "check_colab_status", side_effect=lambda **kw: status_polls.append(kw)),
                mock.patch.object(vm, "_run_bridge_json", side_effect=lambda *a, **kw: bridge_calls.append(a) or {}),
            ):
                vm.ensure_keepalive_daemon_running()
                vm.keepalive_colab_runtime()
                vm._tick_counter = 19  # next tick is 20
                vm._on_background_tick()
                vm._apply_status_response({"gpu_name": "NVIDIA G4", "idle_timeout_minutes": 30})
                time.sleep(0.05)

            self.assertEqual(spawned, [])
            self.assertEqual(status_polls, [])
            self.assertEqual(bridge_calls, [])

    def test_keepalive_daemon_start_stop_toctou_epoch_prevents_orphan(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            self.addCleanup(vm.stop_keepalive_daemon)
            vm.cli_logged_in = True
            vm.is_connected = True

            daemon_script = Path(tmp) / "fake_daemon_race.py"
            daemon_script.write_text(
                "import sys\nsys.stdin.read()\n",
                encoding="utf-8",
            )

            # Sub-case A: stop_keepalive_daemon() runs while _resolve_python_for_bridge is in-flight
            entered_resolve = threading.Event()
            release_resolve = threading.Event()
            popen_calls = []

            def slow_resolve(install_if_missing: bool = False):
                entered_resolve.set()
                release_resolve.wait(timeout=2.0)
                return [sys.executable, str(daemon_script)]

            with (
                mock.patch.object(vm, "_resolve_python_for_bridge", side_effect=slow_resolve),
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=lambda *a, **kw: popen_calls.append(a)),
            ):
                vm.ensure_keepalive_daemon_running()
                self.assertTrue(entered_resolve.wait(timeout=1.5))
                vm.stop_keepalive_daemon()
                release_resolve.set()
                time.sleep(0.08)

            self.assertEqual(popen_calls, [])
            self.assertIsNone(vm._keepalive_daemon_proc)

            # Sub-case B: stop_keepalive_daemon() runs concurrently right as subprocess.Popen spawns child
            real_popen = subprocess.Popen
            spawned_procs = []

            def racing_popen(cmd, **kwargs):
                p = real_popen([sys.executable, str(daemon_script)], **kwargs)
                spawned_procs.append(p)
                # Simulate UI thread calling stop_keepalive_daemon() before daemon_worker re-acquires _keepalive_lock
                vm.stop_keepalive_daemon()
                return p

            with (
                mock.patch.object(vm, "_resolve_python_for_bridge", return_value=[sys.executable, str(daemon_script)]),
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=racing_popen),
            ):
                vm.ensure_keepalive_daemon_running()
                deadline = time.time() + 2.0
                while time.time() < deadline and not spawned_procs:
                    time.sleep(0.02)
                self.assertEqual(len(spawned_procs), 1)
                child = spawned_procs[0]
                rc = child.wait(timeout=2.0)
                self.assertEqual(rc, 0)

            self.assertIsNone(vm._keepalive_daemon_proc)
            self.assertFalse(vm.keepalive_proxy_ws_connected)

    def test_wake_cancellation_token_prevents_aborted_worker_clobber(self):
        with tempfile.TemporaryDirectory() as tmp:
            vm = self._make_vm(tmp)
            vm.cli_logged_in = True
            vm.colab_server_url = "https://existing.trycloudflare.com"

            entered_resolve = threading.Event()
            release_resolve = threading.Event()
            popen_calls = []
            status_calls = []

            def slow_wake_resolve(install_if_missing: bool = True):
                entered_resolve.set()
                release_resolve.wait(timeout=2.0)
                return [sys.executable, BRIDGE_PY_PATH]

            with (
                mock.patch.object(vm, "_resolve_python_for_bridge", side_effect=slow_wake_resolve),
                mock.patch("windows_app.viewmodel_services.subprocess.Popen", side_effect=lambda *a, **kw: popen_calls.append(a)),
                mock.patch.object(vm, "check_colab_status", side_effect=lambda **kw: status_calls.append(kw)),
            ):
                vm.wake_colab_headless()
                self.assertTrue(entered_resolve.wait(timeout=1.5))
                self.assertTrue(vm.is_waking_colab)

                # Cancel wake while worker is still inside _resolve_python_for_bridge
                vm.stop_active_wake_process()
                self.assertFalse(vm.is_waking_colab)

                # Simulate a new wake starting state before old worker unblocks
                vm.is_waking_colab = True
                release_resolve.set()
                time.sleep(0.08)

            self.assertEqual(popen_calls, [])
            self.assertEqual(status_calls, [])
            # Aborted old worker's finish_ui must NOT clobber the newer is_waking_colab flag!
            self.assertTrue(vm.is_waking_colab)
            vm.stop_active_wake_process()


class V120CustomizationsIntactTest(unittest.TestCase):
    def test_app_py_and_colab_bridge_preserve_all_v120_features(self):
        with open(APP_PY_PATH, "r", encoding="utf-8") as f:
            app_src = f.read()
        ast.parse(app_src, filename=APP_PY_PATH)

        # Verify upstream ports AND our v1.2.0 invariants coexist in app.py
        self.assertIn("GGUF_RW_LOCK.is_busy()", app_src)
        self.assertIn("_ActiveApiStreamGuard", app_src)
        self.assertIn("_safe_shm_available_bytes", app_src)
        self.assertIn("_iter_streamer_with_keepalive", app_src)
        self.assertIn("_local_jupyter_ws_keepalive_loop", app_src)
        self.assertIn("_persist_generated_output_to_drive", app_src)
        # Verify our quantized ComfyUI + Heretic MiniMax-H3-Image engine was NOT overwritten
        self.assertIn("Comfy-Org/MiniMax-H3-Image", app_src)
        self.assertIn("minimax_h3_image_pruned_int8_convrot.safetensors", app_src)
        self.assertIn("p-h3-image_prompt_upsampler_heretic_w4a8.safetensors", app_src)

        with open(BRIDGE_PY_PATH, "r", encoding="utf-8") as f:
            bridge_src = f.read()
        ast.parse(bridge_src, filename=BRIDGE_PY_PATH)

        self.assertIn("def cmd_keepalive_daemon", bridge_src)
        self.assertIn("sageattention", bridge_src)
        self.assertIn("start_new_session=True", bridge_src)


if __name__ == "__main__":
    unittest.main()
