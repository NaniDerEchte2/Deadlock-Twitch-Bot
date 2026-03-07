import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from bot.runtime_lock import RuntimeInstanceLockError, runtime_pid_lock

REPO_ROOT = Path(__file__).resolve().parents[1]


class RuntimePidLockTests(unittest.TestCase):
    def test_lock_writes_pid_metadata_and_allows_reacquire_after_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict("os.environ", {"TWITCH_RUNTIME_PID_LOCK_DIR": tmpdir}, clear=False):
                with runtime_pid_lock("twitch_worker", port=8776) as lock:
                    payload = json.loads(Path(lock.path).read_text(encoding="utf-8"))
                    self.assertEqual(payload["pid"], os.getpid())
                    self.assertEqual(payload["service"], "twitch_worker")
                    self.assertEqual(payload["port"], 8776)

                with runtime_pid_lock("twitch_worker", port=8776):
                    pass

    def test_same_process_duplicate_acquire_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict("os.environ", {"TWITCH_RUNTIME_PID_LOCK_DIR": tmpdir}, clear=False):
                with runtime_pid_lock("dashboard_service", port=8765):
                    with self.assertRaises(RuntimeInstanceLockError) as ctx:
                        with runtime_pid_lock("dashboard_service", port=8765):
                            pass

        self.assertIn("already holds runtime lock", str(ctx.exception).lower())

    def test_existing_stale_file_is_overwritten_on_acquire(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "twitch_worker-8776.pidlock"
            lock_path.write_text("stale-garbage", encoding="utf-8")

            with patch.dict("os.environ", {"TWITCH_RUNTIME_PID_LOCK_DIR": tmpdir}, clear=False):
                with runtime_pid_lock("twitch_worker", port=8776):
                    payload = json.loads(lock_path.read_text(encoding="utf-8"))

            self.assertEqual(payload["pid"], os.getpid())
            self.assertEqual(payload["port"], 8776)

    def test_second_process_reports_owner_pid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = os.environ.copy()
            env["TWITCH_RUNTIME_PID_LOCK_DIR"] = tmpdir
            env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
            ready_file = Path(tmpdir) / "runtime-lock.ready"
            script = textwrap.dedent(
                f"""
                import time
                from pathlib import Path
                from bot.runtime_lock import runtime_pid_lock

                with runtime_pid_lock("twitch_worker", port=8776):
                    Path(r"{ready_file}").write_text("ready", encoding="utf-8")
                    time.sleep(30)
                """
            )
            proc = subprocess.Popen(
                [sys.executable, "-c", script],
                cwd=str(REPO_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self._wait_for_subprocess_lock(proc, ready_file)
                with patch.dict("os.environ", {"TWITCH_RUNTIME_PID_LOCK_DIR": tmpdir}, clear=False):
                    with self.assertRaises(RuntimeInstanceLockError) as ctx:
                        with runtime_pid_lock("twitch_worker", port=8776):
                            pass
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)

            self.assertIn(str(proc.pid), str(ctx.exception))

    def _wait_for_subprocess_lock(self, proc: subprocess.Popen[str], ready_file: Path) -> None:
        deadline = time.time() + 10
        while time.time() < deadline:
            if ready_file.exists():
                return
            if proc.poll() is not None:
                stdout, stderr = proc.communicate(timeout=1)
                self.fail(
                    "Lock subprocess exited before acquiring the lock: "
                    f"stdout={stdout!r}, stderr={stderr!r}"
                )
            time.sleep(0.1)
        self.fail("Timed out waiting for subprocess to acquire runtime PID lock")


if __name__ == "__main__":
    unittest.main()
