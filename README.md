# FFmpeg batch processing

Transcode videos locally or across SSH workers sharing storage. The default
preset preserves this project's original use case: crop a specific letterboxed
1920×1080 capture to `1584:1072:158:6`, then scale to 640×480. Outputs use H.264
(`libx264`, medium, CRF 23) and AAC (48 kHz, stereo, 128 kb/s) in MP4 containers.
For other source dimensions, choose a suitable `--filter`.

## Requirements

- Bash 4.4 or newer, GNU core utilities and GNU find, including `realpath`,
  `sha256sum`, `mktemp`, and `ln -T`.
- FFmpeg with the `libx264` and AAC encoders on every worker.
- An output filesystem supporting hard links. This allows atomic publication
  without replacing an existing destination. Preflight fails if hard links are
  unavailable; there is no overwrite fallback.
- Remote workers need SSH public-key authentication, a POSIX login shell, and
  the **same absolute input/output paths on shared storage**. Preflight checks a
  unique shared run token and write access. The script is sent over SSH stdin;
  no remote script installation is required.

The offline suite has been exercised with Git Bash 5.2 on Windows. GNU/Linux is
the intended deployment environment; check actual SSH/shared-storage behavior
on a small sample before scheduling a large batch.

## Start locally

```bash
# Inspect the plan: no writes, encodes, or SSH connections.
bash vidprocess.sh --input /media/captures --dry-run

# Default: localhost, two concurrent jobs, no automatic retries.
bash vidprocess.sh --input /media/captures

# Separate output root and one extra attempt per failed job.
bash vidprocess.sh --input /media/captures --output /media/converted \
  --jobs 4 --retries 1

# Material that does not need the original crop.
bash vidprocess.sh --input /media/captures --filter 'scale=640:-2'
```

`--help` lists all options. Zero matching files is a successful no-op. Discovery
is recursive, case-insensitive for supported extensions, NUL-delimited, and does
not follow directory symlinks. The selected output tree is excluded. Launch the
script from any working directory.

Outputs preserve relative source paths and **append `.mp4` to the full filename**:

| Input relative path | Output relative path |
| --- | --- |
| `camera-a/clip.mov` | `camera-a/clip.mov.mp4` |
| `camera-b/clip.mov` | `camera-b/clip.mov.mp4` |
| `camera-a/clip.mp4` | `camera-a/clip.mp4.mp4` |

The default output is `INPUT/transcoded`. Input/output cannot be filesystem root,
and output cannot equal or contain input. `.vidprocess` is reserved at the output
root; source files beneath an input `.vidprocess` subtree are rejected because
their mapped destinations would collide with this state namespace.

## Shared-storage workers

```bash
bash vidprocess.sh --input /mnt/shared/captures \
  --host localhost --host encode-a --host encode-b --user media --jobs 2
```

Each `--host` adds a worker with `--jobs` slots. This single list drives assignment,
including small batches. If no hosts are specified, only localhost is used.
Omit `--user` to use SSH's configured username. SSH uses batch mode and a
ten-second connection timeout. Concurrency limits FFmpeg processes, not their
internal threads. Scheduling uses bounded round-robin slots, not duration balancing.

## Results and recovery

Reports separate successful, verified-skipped, and failed jobs. Exit status is
`0` for a completed run without failures, `1` for failed jobs, `2` for configuration
or preflight errors, and `130` for handled interruption. No retry prompt requires
interactive input.

State is retained under `OUTPUT/.vidprocess`:

- `runs/run.*/files.nul`: exact input snapshot, NUL-delimited.
- `runs/run.*/manifest.tsv`: job ID, shell-escaped source/destination, and host.
- `runs/run.*/*.log`: stdout/stderr for each job attempt.
- `runs/run.*/*.result`: final job status.
- `runs/run.*/retry.nul`: failed paths after a completed run, if any.
- `receipts/`: source/preset and output-content verification hashes.
- `lock` and `locks/`: coordinator and individual-output reservations.

Resume by rerunning the same command. An output is skipped only when its receipt
matches source contents, filter/preset, and current output contents. Failed or
unfinished files are attempted again, with additional attempts controlled by
`--retries`. Logs and manifests remain available after the run. Remove old run
directories manually only when no workers use them; keep receipts for future
verified skipping.

FFmpeg runs with `-xerror`: reported decoding/demuxing errors fail the job before
publication or receipt creation. Receipts from the earlier permissive decoder
policy are not accepted; existing outputs are preserved as conflicts. Inspect
them and use a new output root or move them aside before retrying. This does not
detect missing content that the media container/decoder reports as valid.

Verification reads entire source and output files. Successful encoding also
checks the source again before publication to detect changes during processing.
This adds disk/network I/O. Keep source media stable while processing it.

**Existing output conflicts are preserved and reported as failures.** This
includes files without matching receipts, modified files, and files made with a
different filter. Choose a new output root or move a conflicting output aside
after inspecting it; there is no overwrite flag. A crash between output and
receipt publication can leave a complete file without a receipt. It is
deliberately treated as a conflict: inspect it and move it aside or choose a new
output root before retrying.

Unique partial files are created beside the final destination. Handled local
cancellation stops active encoders and removes owned partials/reservations while
retaining logs/manifests. SSH disconnection cannot guarantee remote process
termination. Confirm remote jobs have stopped before removing stale locks.
After an unhandled crash or machine failure, inspect worker processes and run
records before manually removing stale reservations or abandoned
`.vidprocess.*.mp4` partials. The script never recursively deletes the working
directory or clears previous run logs.

## Offline regression tests

```bash
python -m unittest discover -s tests -v
bash -n vidprocess.sh
```

Tests require Python 3.10+ and use temporary fixtures with local FFmpeg/SSH stubs;
they never contact remote hosts. Set `BASH_EXE` if needed (Windows defaults to the
standard Git Bash installation). Coverage includes zero-to-ten-file batches,
duplicate names, verified reruns, changed inputs/outputs, overwrite refusal,
bounded retries, empty/failed encodes, remote quoting/failures, cancellation,
and dry-run behavior. Real synthetic FFmpeg controls also cover truncated input,
retry after source repair, and valid media paths containing spaces/apostrophes
(explicitly converted for native Windows FFmpeg). Newline filenames are skipped on Windows because its
filesystem forbids them.
