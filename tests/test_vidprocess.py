"""Offline tests using synthetic media and local stubs; SSH never contacts a host."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
BASH = os.environ.get("BASH_EXE") or (
    r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else shutil.which("bash")
)


def shell_path(path):
    value = Path(path).resolve().as_posix()
    if os.name == "nt":
        return "/" + value[0].lower() + value[2:]
    return value


ENCODER = r'''#!/usr/bin/env bash
set -eu
source=''
while (( $# )); do
    if [[ $1 == -i ]]; then source=$2; shift 2; else output=$1; shift; fi
done
case "$OSTYPE" in msys*|cygwin*) source=$(cygpath -u -- "$source");; esac
printf '%s\0' "$source" >> "$TEST_CALLS"
trap 'printf stopped > "$TEST_STOPPED"; exit 143' TERM
if [[ ${TEST_CANCEL:-0} == 1 ]]; then
    touch "$TEST_READY"
    while :; do sleep 0.05; done
fi
if [[ ${TEST_FAIL:-} == all || ( ${TEST_FAIL:-} == bad && $source == */bad.mp4 ) ]]; then
    printf 'partial' > "$output"
    echo 'synthetic encoder failure' >&2
    exit 9
fi
if [[ ${TEST_FAIL_ONCE:-0} == 1 && ! -f "$TEST_ONCE" ]]; then
    touch "$TEST_ONCE"
    echo 'synthetic transient failure' >&2
    exit 9
fi
if [[ ${TEST_EMPTY:-0} == 1 ]]; then : > "$output"; exit 0; fi
if [[ ${TEST_SLEEP:-0} != 0 ]]; then sleep "$TEST_SLEEP"; fi
printf 'encoded:' > "$output"
cat -- "$source" >> "$output"
'''

SSH = r'''#!/usr/bin/env bash
set -eu
printf 'ssh\n' >> "$TEST_SSH_CALLS"
[[ ${TEST_SSH_FAIL:-0} != 1 ]] || exit 255
if [[ ${TEST_SSH_WORKER_FAIL:-0} == 1 && $6 == *--internal-worker* ]]; then exit 255; fi
# Parse only the script's fixed options; never call an actual SSH executable.
[[ $1 == -o && $2 == BatchMode=yes && $3 == -o && $4 == ConnectTimeout=10 ]] || exit 88
[[ $5 == "${TEST_EXPECT_TARGET:-worker}" ]] || exit 89
exec bash -c "$6"
'''


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="batch test '")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.inputs = self.base / "input files"
        self.inputs.mkdir()
        self.output = self.inputs / "transcoded"
        self.bin = self.base / "bin"
        self.bin.mkdir()
        for name, source in (("ffmpeg", ENCODER), ("ssh", SSH)):
            path = self.bin / name
            path.write_text(source, encoding="utf-8", newline="\n")
            path.chmod(0o755)
        self.calls = self.base / "calls"
        self.ssh_calls = self.base / "ssh-calls"
        self.env = dict(os.environ, TEST_CALLS=shell_path(self.calls),
                        TEST_SSH_CALLS=shell_path(self.ssh_calls),
                        TEST_READY=shell_path(self.base / "ready"),
                        TEST_STOPPED=shell_path(self.base / "stopped"),
                        TEST_ONCE=shell_path(self.base / "once"))

    def add(self, name, content="media"):
        path = self.inputs / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def run_batch(self, *args, **environment):
        env = dict(self.env, **environment)
        command = [BASH, "-c", 'export PATH="$1:/usr/bin:/bin:$PATH"; shift; exec bash "$@"',
                   "test", shell_path(self.bin), shell_path(ROOT / "vidprocess.sh"),
                   "--input", shell_path(self.inputs), "--jobs", "2", *args]
        return subprocess.run(command, env=env, cwd=self.base, capture_output=True,
                              text=True, timeout=30)

    def count_calls(self):
        return self.calls.read_bytes().count(b"\0") if self.calls.exists() else 0

    def assert_ok(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_zero_through_ten_inputs_assigned_once(self):
        for count in range(11):
            with self.subTest(count=count):
                self.inputs = self.base / str(count)
                self.inputs.mkdir()
                self.output = self.inputs / "transcoded"
                before = self.count_calls()
                for i in range(count):
                    self.add(f"clip{i}.mp4")
                result = self.run_batch("--host", "localhost", "--host", "localhost", "--host", "localhost")
                self.assert_ok(result)
                self.assertEqual(self.count_calls() - before, count)
                self.assertEqual(len(list(self.output.rglob("*.mp4"))), count)

    def test_duplicate_names_and_mixed_extensions(self):
        for name in ("a/clip.mp4", "b/clip.mp4", "a/clip.mov"):
            self.add(name, name)
        self.assert_ok(self.run_batch())
        for name in ("a/clip.mp4", "b/clip.mp4", "a/clip.mov"):
            self.assertEqual((self.output / (name + ".mp4")).read_text(), "encoded:" + name)

    def test_verified_rerun_skips_and_excludes_output_tree(self):
        self.add("one.MP4")
        self.assert_ok(self.run_batch())
        result = self.run_batch()
        self.assert_ok(result)
        self.assertIn("requested=1 success=0 skipped=1 failed=0", result.stdout)
        self.assertEqual(self.count_calls(), 1)

    def test_permissive_policy_receipt_is_not_trusted(self):
        source = self.add("one.mp4")
        self.assert_ok(self.run_batch("--filter", "null"))
        target = self.output / "one.mp4.mp4"
        before = target.read_bytes()
        receipt = next((self.output / ".vidprocess/receipts").iterdir())
        input_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        old_signature = hashlib.sha256(f"preset-v1|{input_hash}|null".encode()).hexdigest()
        receipt.write_text(old_signature + "\n" + hashlib.sha256(before).hexdigest() + "\n")
        result = self.run_batch("--filter", "null")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(target.read_bytes(), before)
        self.assertEqual(self.count_calls(), 1)
        self.assertIn("skipped=0 failed=1", result.stdout)

    def test_real_truncated_video_retries_then_accepts_repaired_source(self):
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("FFmpeg required for real synthetic video check")
        (self.bin / "ffmpeg").unlink()
        reference = self.base / "valid.mp4"
        subprocess.run([ffmpeg, "-v", "error", "-f", "lavfi", "-i",
                        "testsrc2=size=128x96:rate=25:duration=2", "-c:v", "libx264",
                        "-movflags", "+faststart", "-y", str(reference)],
                       check=True, capture_output=True, timeout=30)
        source = self.inputs / "clip.mp4"
        data = reference.read_bytes()
        source.write_bytes(data[:len(data)//2])
        for _ in range(2):
            result = self.run_batch("--filter", "null")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("success=0 skipped=0 failed=1", result.stdout)
            self.assertFalse((self.output / "clip.mp4.mp4").exists())
            self.assertEqual(list((self.output / ".vidprocess/receipts").iterdir()), [])
            self.assertFalse(list(self.output.rglob(".vidprocess.*.mp4")))
        source.write_bytes(data)
        repaired = self.run_batch("--filter", "null")
        logs = "\n".join(p.read_text() for p in self.output.glob(".vidprocess/runs/*/*.log"))
        self.assertEqual(repaired.returncode, 0, repaired.stdout + repaired.stderr + logs)
        result = self.run_batch("--filter", "null")
        self.assert_ok(result)
        self.assertIn("success=0 skipped=1 failed=0", result.stdout)

    def test_unrelated_existing_output_is_preserved(self):
        self.add("one.mp4")
        self.output.mkdir()
        target = self.output / "one.mp4.mp4"
        target.write_text("keep me")
        result = self.run_batch()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(target.read_text(), "keep me")
        self.assertEqual(self.count_calls(), 0)

    def test_changed_input_or_output_is_not_silently_skipped(self):
        source = self.add("one.mp4", "original")
        self.assert_ok(self.run_batch())
        target = self.output / "one.mp4.mp4"
        original = target.read_text()
        source.write_text("changed")
        self.assertEqual(self.run_batch().returncode, 1)
        self.assertEqual(target.read_text(), original)
        source.write_text("original")
        target.write_text("modified output")
        self.assertEqual(self.run_batch().returncode, 1)
        self.assertEqual(target.read_text(), "modified output")

    def test_failure_is_not_success_and_logs_survive(self):
        self.add("good.mp4")
        bad = self.add("bad.mp4")
        result = self.run_batch(TEST_FAIL="bad")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("success=1 skipped=0 failed=1", result.stdout)
        self.assertFalse((self.output / "bad.mp4.mp4").exists())
        runs = list((self.output / ".vidprocess/runs").iterdir())
        self.assertEqual((runs[0] / "retry.nul").read_bytes(), shell_path(bad).encode() + b"\0")
        self.assertTrue(any("synthetic encoder failure" in p.read_text() for p in runs[0].glob("*.log")))
        self.assertFalse(list(self.output.rglob(".vidprocess.*.mp4")))
        self.assert_ok(self.run_batch())
        self.assertEqual(self.count_calls(), 3)

    def test_single_file_bounded_retry(self):
        self.add("one.mp4")
        result = self.run_batch("--retries", "1", TEST_FAIL_ONCE="1")
        self.assert_ok(result)
        self.assertEqual(self.count_calls(), 2)
        self.assertIn("success=1 skipped=0 failed=0", result.stdout)

    def test_permanent_failure_exhausts_retries(self):
        self.add("one.mp4")
        self.assertEqual(self.run_batch("--retries", "2", TEST_FAIL="all").returncode, 1)
        self.assertEqual(self.count_calls(), 3)

    def test_empty_encoder_output_is_failure(self):
        self.add("one.mp4")
        self.assertEqual(self.run_batch(TEST_EMPTY="1").returncode, 1)
        self.assertFalse((self.output / "one.mp4.mp4").exists())

    def test_dry_run_is_read_only_and_does_not_connect(self):
        self.add("one.mp4")
        self.assert_ok(self.run_batch("--dry-run", "--host", "worker"))
        self.assertFalse(self.output.exists())
        self.assertEqual(self.count_calls(), 0)
        self.assertFalse(self.ssh_calls.exists())

    def test_remote_stub_handles_spaces_apostrophes_and_username(self):
        self.add("Bob's clip.MOV")
        result = self.run_batch("--host", "worker", "--user", "media", TEST_EXPECT_TARGET="media@worker")
        self.assert_ok(result)
        self.assertEqual(self.count_calls(), 1)
        self.assertEqual(self.ssh_calls.read_text().count("ssh"), 2)

    @unittest.skipIf(os.name == "nt", "Windows filesystem forbids newline filenames")
    def test_newline_filename(self):
        self.add("line\nbreak.mp4")
        self.assert_ok(self.run_batch())
        self.assertTrue((self.output / "line\nbreak.mp4.mp4").exists())

    def test_remote_preflight_failure_does_not_encode(self):
        self.add("one.mp4")
        result = self.run_batch("--host", "worker", TEST_SSH_FAIL="1")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.count_calls(), 0)

    def test_remote_worker_failure_is_reported(self):
        self.add("one.mp4")
        result = self.run_batch("--host", "worker", TEST_SSH_WORKER_FAIL="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("success=0 skipped=0 failed=1", result.stdout)
        self.assertEqual(self.count_calls(), 0)

    def test_brackets_in_output_path_are_pruned_on_rerun(self):
        self.add("one.mp4")
        output = self.inputs / "converted [2026]"
        self.assert_ok(self.run_batch("--output", shell_path(output)))
        result = self.run_batch("--output", shell_path(output))
        self.assert_ok(result)
        self.assertIn("requested=1 success=0 skipped=1 failed=0", result.stdout)
        self.assertEqual(self.count_calls(), 1)

    def test_term_stops_local_encoder_and_preserves_retry_state(self):
        self.add("one.mp4")
        wrapper = '''export PATH="$1:/usr/bin:/bin:$PATH"; shift
bash "$@" & batch=$!
for ((i=0;i<200;i++)); do
    [[ ! -f "$TEST_READY" ]] || break
    sleep 0.05
done
[[ -f "$TEST_READY" ]] || { kill -TERM "$batch"; wait "$batch"; exit 99; }
kill -TERM "$batch"
wait "$batch"
'''
        result = subprocess.run([BASH, "-c", wrapper, "cancel-test", shell_path(self.bin),
                                 shell_path(ROOT / "vidprocess.sh"), "--input", shell_path(self.inputs)],
                                env=dict(self.env, TEST_CANCEL="1"), cwd=self.base,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 130, result.stdout + result.stderr)
        self.assertTrue((self.base / "stopped").exists())
        self.assertFalse((self.output / "one.mp4.mp4").exists())
        self.assertFalse((self.output / ".vidprocess/lock").exists())
        self.assertFalse(list((self.output / ".vidprocess/locks").iterdir()))
        self.assertFalse(list(self.output.rglob(".vidprocess.*.mp4")))
        self.assertTrue(list((self.output / ".vidprocess/runs").glob("*/files.nul")))
        self.assert_ok(self.run_batch())

    def test_existing_lock_is_preserved(self):
        self.add("one.mp4")
        lock = self.output / ".vidprocess/lock"
        lock.mkdir(parents=True)
        self.assertEqual(self.run_batch().returncode, 2)
        self.assertTrue(lock.exists())
        self.assertEqual(self.count_calls(), 0)

    def test_reserved_state_namespace_is_rejected(self):
        self.add(".vidprocess/lock/one.mp4")
        result = self.run_batch()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("reserved output state", result.stderr)
        self.assertEqual(self.count_calls(), 0)
        self.assertFalse((self.output / ".vidprocess/lock").exists())

    def test_input_and_output_overlap_is_rejected(self):
        self.add("one.mp4")
        self.assertEqual(self.run_batch("--output", shell_path(self.inputs)).returncode, 2)
        self.assertEqual(self.run_batch("--output", shell_path(self.base)).returncode, 2)

    def test_invalid_options(self):
        for args in (("--jobs", "0"), ("--retries", "-1"), ("--host", "-bad"), ("--input",)):
            with self.subTest(args=args):
                self.assertEqual(self.run_batch(*args).returncode, 2)


if __name__ == "__main__":
    unittest.main()
