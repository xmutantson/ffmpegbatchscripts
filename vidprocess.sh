#!/usr/bin/env bash
# Shared-storage batch transcoding. Requires Bash 4.4+ and GNU core utilities.
set -uo pipefail

die() { printf 'Error: %s\n' "$*" >&2; exit 2; }
(( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )) || die 'Bash 4.4 or newer is required'
hash_file() { sha256sum < "$1" | cut -d ' ' -f 1; }
hash_text() { printf '%s' "$1" | sha256sum | cut -d ' ' -f 1; }

usage() {
    cat <<'EOF'
Usage: bash vidprocess.sh --input DIR [options]
  --output DIR       Output root (default: INPUT/transcoded)
  --host HOST        Worker host; repeat for multiple hosts (default: localhost)
  --user USER        SSH username (default: SSH configuration)
  --jobs N           Concurrent encodes per host (default: 2)
  --retries N        Additional attempts per failed job (default: 0)
  --filter FILTER    FFmpeg video filter (default: original crop/scale preset)
  --dry-run          List planned files without writes or SSH connections
  --help            Show this help

All hosts must share the same absolute input/output paths and have Bash 4.4+,
GNU core utilities and FFmpeg with libx264/AAC. Outputs preserve relative paths
and append .mp4 to the complete input filename. Existing files are never replaced.
EOF
}

check_tools() {
    local tool
    for tool in ffmpeg sha256sum cut mktemp mv ln mkdir rm rmdir realpath dirname cat; do
        command -v "$tool" >/dev/null || die "Required command unavailable: $tool"
    done
}

worker() {
    local source=$1 destination=$2 state=$3 run=$4 id=$5 filter=$6 attempt=$7
    local receipt="$state/receipts/$id"
    # Cleanup variables outlive this function until the worker shell exits.
    lock="$state/locks/$id"
    temporary='' child='' receipt_tmp=''
    local input_hash output_hash signature old_signature old_output
    worker_cleanup() {
        if [[ -n "$child" ]]; then kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :; fi
        [[ -z "$temporary" ]] || rm -f -- "$temporary"
        [[ -z "$receipt_tmp" ]] || rm -f -- "$receipt_tmp"
        rmdir -- "$lock" 2>/dev/null || :
    }
    worker_stop() { exit 130; }
    check_tools
    mkdir -- "$lock" 2>/dev/null || { printf 'Output is locked: %s\n' "$destination" >&2; return 1; }
    trap worker_cleanup EXIT
    trap worker_stop INT TERM
    input_hash=$(hash_file "$source") || return 1
    signature=$(hash_text "preset-v2-strict-decode|$input_hash|$filter") || return 1
    if [[ -e "$destination" || -L "$destination" ]]; then
        if [[ -f "$destination" && ! -L "$destination" && -f "$receipt" ]]; then
            { IFS= read -r old_signature; IFS= read -r old_output; } < "$receipt" || return 1
            output_hash=$(hash_file "$destination") || return 1
            if [[ "$signature" == "$old_signature" && "$output_hash" == "$old_output" ]]; then
                printf 'skipped\n' > "$run/$id.result"
                return 0
            fi
        fi
        printf 'Existing output has no matching verified receipt; preserving it: %s\n' "$destination" >&2
        return 1
    fi
    local parent
    parent=$(dirname -- "$destination")
    # Check prospective paths before creating directories, then verify again.
    local actual_parent output_root
    actual_parent=$(realpath -m -- "$parent") || return 1
    output_root=$(cd -- "$state/.." && pwd -P) || return 1
    [[ "$actual_parent/" == "$output_root/"* ]] || { printf 'Output escapes output root\n' >&2; return 1; }
    mkdir -p -- "$parent" || return 1
    actual_parent=$(cd -- "$parent" && pwd -P) || return 1
    [[ "$actual_parent/" == "$output_root/"* ]] || return 1
    temporary=$(mktemp "$parent/.vidprocess.XXXXXXXX.mp4") || return 1
    # Native Windows FFmpeg needs explicit conversion for paths containing quotes.
    local media_source=$source media_output=$temporary
    case "$OSTYPE" in
        msys*|cygwin*)
            media_source=$(cygpath -am -- "$source") || return 1
            media_output=$(cygpath -am -- "$temporary") || return 1
            ;;
    esac
    ffmpeg -nostdin -hide_banner -v error -xerror -i "$media_source" \
        -vf "$filter" -c:v libx264 -preset medium -crf 23 \
        -c:a aac -ar 48000 -ac 2 -b:a 128k -f mp4 -y "$media_output" &
    child=$!
    local encode_status=0
    wait "$child" || encode_status=$?
    child=''
    (( encode_status == 0 )) || return "$encode_status"
    [[ -s "$temporary" ]] || { printf 'Encoder produced an empty output\n' >&2; return 1; }
    [[ "$(hash_file "$source")" == "$input_hash" ]] || { printf 'Input changed during encoding\n' >&2; return 1; }
    output_hash=$(hash_file "$temporary") || return 1
    # A hard link atomically publishes a complete file and fails if the name exists.
    # No replace/force flag: an unrelated writer's destination is always preserved.
    ln -T -- "$temporary" "$destination" || return 1
    rm -- "$temporary" || return 1
    temporary=''
    receipt_tmp=$(mktemp "$state/receipts/.receipt.XXXXXXXX") || return 1
    printf '%s\n%s\n' "$signature" "$output_hash" > "$receipt_tmp" || return 1
    mv -- "$receipt_tmp" "$receipt" || return 1
    receipt_tmp=''
    printf 'success\n' > "$run/$id.result"
}

# Internal modes are also used by the script sent to remote workers over stdin.
if [[ ${1:-} == --internal-worker ]]; then
    shift
    (( $# == 7 )) || die 'Invalid worker arguments'
    worker "$@"
    exit $?
fi
if [[ ${1:-} == --internal-check ]]; then
    shift
    (( $# == 2 )) || die 'Invalid preflight arguments'
    check_tools
    [[ -f "$1" && "$(cat -- "$1")" == "$2" ]] || die 'Worker cannot see shared run state'
    probe=$(mktemp "$(dirname -- "$1")/.probe.XXXXXXXX") || exit 2
    link="$probe.link"
    ln -T -- "$probe" "$link" || { rm -f -- "$probe"; die 'Output filesystem must support hard links'; }
    rm -- "$link" "$probe" || exit 2
    exit 0
fi

input='' output='' user='' jobs=2 retries=0 dry_run=0
filter='crop=1584:1072:158:6,scale=640:480'
hosts=()
while (( $# )); do
    case $1 in
        --help|-h) usage; exit 0 ;;
        --dry-run) dry_run=1; shift ;;
        --input|--output|--user|--jobs|--retries|--filter|--host)
            (( $# >= 2 )) || die "Missing value for $1"
            case $1 in
                --input) input=$2 ;; --output) output=$2 ;; --user) user=$2 ;;
                --jobs) jobs=$2 ;; --retries) retries=$2 ;; --filter) filter=$2 ;;
                --host) hosts+=("$2") ;;
            esac
            shift 2 ;;
        *) die "Unknown option: $1" ;;
    esac
done
[[ -n "$input" && -d "$input" ]] || die '--input must name an existing directory'
[[ "$jobs" =~ ^[1-9][0-9]*$ && ${#jobs} -le 3 ]] || die '--jobs must be an integer from 1 to 999'
[[ "$retries" =~ ^[0-9]+$ && ${#retries} -le 2 ]] || die '--retries must be an integer from 0 to 99'
retries=$((10#$retries))
[[ -n "$filter" ]] || die '--filter must not be empty'
(( ${#hosts[@]} )) || hosts=(localhost)
for host in "${hosts[@]}"; do
    [[ "$host" =~ ^[a-zA-Z0-9][a-zA-Z0-9._:-]*$ ]] || die "Invalid host: $host"
done
[[ -z "$user" || "$user" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$ ]] || die 'Invalid SSH username'
input=$(cd -- "$input" && pwd -P) || exit 2
output=${output:-"$input/transcoded"}
command -v realpath >/dev/null || die 'GNU realpath is required'
output=$(realpath -m -- "$output") || exit 2
[[ "$input" != / && "$output" != / && "$input" != "$output" && "$input/" != "$output/"* ]] || die 'Input/output cannot be filesystem root; output must not equal or contain input'
state="$output/.vidprocess"
script=$(realpath -- "${BASH_SOURCE[0]}") || exit 2

# Snapshot discovery using NUL delimiters, including uppercase extensions. Prune
# generated outputs and state; do not follow symlinks into other directory trees.
files=()
output_pattern=${output//\\/\\\\}
output_pattern=${output_pattern//\*/\\*}
output_pattern=${output_pattern//\?/\\?}
output_pattern=${output_pattern//\[/\\[}
discover() {
    find "$input" -path "$output_pattern" -prune -o -type f \
        \( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.avi' -o -iname '*.mkv' \
        -o -iname '*.flv' -o -iname '*.wmv' -o -iname '*.mpeg' -o -iname '*.mpg' \
        -o -iname '*.webm' -o -iname '*.3gp' -o -iname '*.ogg' -o -iname '*.ogv' -o -iname '*.ts' \) -print0
}
if (( dry_run )); then
    # pipefail propagates discovery failures; dry-run creates no directories/files.
    discover | while IFS= read -r -d '' file; do printf '%q -> %q\n' "$file" "$output/${file#"$input/"}.mp4"; done
    exit $?
fi

mkdir -p -- "$state" || exit 2
[[ ! -L "$state" ]] || die 'State directory must not be a symlink'
mkdir -- "$state/lock" 2>/dev/null || die "Run lock exists: $state/lock (ensure no workers remain before manually removing a stale lock)"
pids=() slot_ids=() slot_hosts=() interrupted=0
cleanup() {
    local pid
    for pid in "${pids[@]}"; do [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || :; done
    for pid in "${pids[@]}"; do [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || :; done
    rmdir -- "$state/lock" 2>/dev/null || :
}
stop() { interrupted=1; exit 130; }
trap cleanup EXIT
trap stop INT TERM
mkdir -p -- "$state/runs" "$state/receipts" "$state/locks" || exit 2
for dir in "$state/runs" "$state/receipts" "$state/locks"; do [[ ! -L "$dir" ]] || die "State path must not be a symlink: $dir"; done
run=$(mktemp -d "$state/runs/run.XXXXXXXX") || exit 2
printf 'Run state: %s\n' "$run"
discover > "$run/files.nul" || die 'Input discovery failed; inspect directory permissions'
while IFS= read -r -d '' file; do files+=("$file"); done < "$run/files.nul"
for file in "${files[@]}"; do
    [[ "$file" != "$input/.vidprocess/"* ]] || die 'Input .vidprocess subtree conflicts with reserved output state; select a narrower input directory'
done
printf 'Requested: %s\n' "${#files[@]}"
if (( ${#files[@]} == 0 )); then printf 'No matching inputs.\n'; exit 0; fi
token=$(hash_text "$run|$$|$RANDOM") || exit 2
printf '%s' "$token" > "$run/shared-token"

remote() (
    local host=$1; shift
    local command='bash -s --' argument quoted target=$host
    [[ -z "$user" ]] || target="$user@$host"
    for argument in "$@"; do
        # POSIX single-quote escaping also supports apostrophes/newlines in paths.
        quoted=${argument//\'/\'\\\'\'}
        command+=" '$quoted'"
    done
    exec ssh -o BatchMode=yes -o ConnectTimeout=10 "$target" "$command" < "$script"
)
for host in "${hosts[@]}"; do
    if [[ "$host" == localhost ]]; then
        bash "$script" --internal-check "$run/shared-token" "$token" || die 'Local preflight failed'
    else
        command -v ssh >/dev/null || die 'SSH is required for remote workers'
        remote "$host" --internal-check "$run/shared-token" "$token" || die "Preflight failed on $host"
    fi
    for ((j=0; j<jobs; j++)); do slot_hosts+=("$host"); pids+=(''); slot_ids+=(''); done
done

run_job() {
    trap - EXIT
    local host=$1 file=$2 destination=$3 id=$4 attempt status=1 worker_pid=''
    job_stop() { [[ -z "$worker_pid" ]] || kill "$worker_pid" 2>/dev/null || :; [[ -z "$worker_pid" ]] || wait "$worker_pid" 2>/dev/null || :; exit 130; }
    trap job_stop INT TERM
    for ((attempt=0; attempt<=retries; attempt++)); do
        if [[ "$host" == localhost ]]; then
            bash "$script" --internal-worker "$file" "$destination" "$state" "$run" "$id" "$filter" "$attempt" > "$run/$id.$attempt.log" 2>&1 &
        else
            remote "$host" --internal-worker "$file" "$destination" "$state" "$run" "$id" "$filter" "$attempt" > "$run/$id.$attempt.log" 2>&1 &
        fi
        worker_pid=$!
        status=0
        wait "$worker_pid" || status=$?
        worker_pid=''
        (( status != 0 )) || return 0
    done
    printf 'failed\n' > "$run/$id.result"
    return 1
}

success=0 skipped=0 failed=0 finished=0
reap() {
    local slot=$1 result='failed' status=0 id=${slot_ids[$1]}
    [[ -n "${pids[$slot]}" ]] || return 0
    wait "${pids[$slot]}" || status=$?
    pids[$slot]=''
    if (( status == 0 )) && [[ -f "$run/$id.result" ]]; then IFS= read -r result < "$run/$id.result" || result=failed; fi
    case $result in success) ((success+=1)) ;; skipped) ((skipped+=1)) ;; *) ((failed+=1)); printf '%s\0' "${job_sources[$id]}" >> "$run/retry.nul" ;; esac
    ((finished+=1))
    printf 'Completed %s/%s: success=%s skipped=%s failed=%s\n' "$finished" "${#files[@]}" "$success" "$skipped" "$failed"
}
declare -A job_sources=()
index=0
for file in "${files[@]}"; do
    relative=${file#"$input/"}
    id=$(hash_text "$relative") || exit 2
    job_sources[$id]=$file
    destination="$output/$relative.mp4"
    slot=$((index % ${#slot_hosts[@]}))
    reap "$slot"
    printf '%s\t%q\t%q\t%s\n' "$id" "$file" "$destination" "${slot_hosts[$slot]}" >> "$run/manifest.tsv" || die 'Could not write run manifest'
    run_job "${slot_hosts[$slot]}" "$file" "$destination" "$id" &
    pids[$slot]=$!
    slot_ids[$slot]=$id
    ((index+=1))
done
for ((slot=0; slot<${#pids[@]}; slot++)); do reap "$slot"; done
printf 'Summary: requested=%s success=%s skipped=%s failed=%s\nLogs and manifests: %s\n' "${#files[@]}" "$success" "$skipped" "$failed" "$run"
(( failed == 0 ))
