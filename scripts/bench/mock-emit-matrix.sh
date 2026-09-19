#!/usr/bin/env bash
# Knobs are assigned indirectly by the knob table.
# shellcheck disable=SC2153
# Emission-policy experiment on the Linux benchmark host: brisk-mock's
# emission policies (--emit-policy full-spin, and fixed at each commit window
# of COMMITS) under the load generator's selfcheck, over plaintext and TLS
# and at several target concurrencies (the selfcheck runs at twice the
# target). It shows the trade-off of the commit window: a longer window
# lowers the mock write lag, a shorter one lets fewer inbound requests queue
# (request slip, TTFT, mock read lag). full-spin is the reference column. The
# selfcheck judges the write lag against WRITE_LAG_LIMIT_US.
#
# Usage: scripts/bench/mock-emit-matrix.sh [--<knob> <value> | --<knob>=<value>]...
#        scripts/bench/mock-emit-matrix.sh --print-config [--<knob> <value>]...
#        scripts/bench/mock-emit-matrix.sh --help
#
# Every knob is an environment variable, and a flag overrides it:
# `--concurrencies "100 500"` sets CONCURRENCIES="100 500". --help lists them.
# Run it detached (tmux, or systemd-run --user --scope) so a dropped ssh
# connection does not end it.
#
# A variant is full-spin or fixed at one commit window (POLICIES=fixed
# COMMITS="5 10 20" gives fixed-5us, fixed-10us, fixed-20us). Every cell
# starts a fresh mock with the cell's variant, runs one selfcheck against it
# and stops it. A group is one (TLS mode, concurrency) pair of a repetition;
# its cells run the variants back to back and share the schedule seed
# (SEED + repetition - 1), so they see the same offered load. With ROTATE=1
# (default) the variant order of group g (counted within its repetition) in
# repetition r starts at offset (g + r) mod n, n being the number of
# variants: it rotates by one from group to group (ABC, BCA, CAB, ...) and
# every repetition starts at a different offset, so a drift across a group
# does not always favour the same variant, even when the number of groups is
# a multiple of n. ROTATE=0 keeps ABC in every group. The whole matrix
# repeats REPS times, which exposes drift between repetitions.
#
# The mock statistics are reset at the end of the loadgen warmup and read at
# the end of the measurement window, before the load generator tears its
# streams down, and once more after the run. CPU usage of the mock and the
# load generator is sampled over the same window. The defaults follow the
# 8-vCPU core plan: mock 2 shards on CPUs 2-3, loadgen 2 shards on CPUs 0-1,
# and this script with its helpers on CPU 4, one of floor's cores, which
# idle here.
#
# Output in $RESULTS_ROOT/$RUN_ID/: cells.jsonl (one line per cell),
# table.txt, session.log, results/ (loadgen result files), stats/ (raw mock
# statistics), logs/, host/, bin/. The script holds $RESULTS_ROOT/.run-m0.lock
# for its whole life, like run-m0.sh, so neither a sync nor an M0 session can
# run alongside. The exit status is 1 if a cell left no result file or its
# mock exited during the run; a selfcheck FAIL is a result, not a failure.

set -euo pipefail

usage() {
    # The leading comment block after the shellcheck directive and its note.
    awk 'NR < 4 { next } /^#/ { print; next } { exit }' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
    echo "Knobs (default, meaning):"
    local entry name default meaning
    for entry in "${knob_table[@]}"; do
        IFS='|' read -r name default meaning <<<"$entry"
        printf '  %-22s %-24s %s\n' "$name" "${default:-(none)}" "$meaning"
    done
}

# NAME|default|meaning.
knob_table=(
    "RUN_ID|mock-emit-$(date -u +%Y%m%dT%H%M%SZ)|session directory name"
    "RESULTS_ROOT|$HOME/bench-results|parent of the session directory"
    "BIN_DIR|$HOME/brisk-target/release|directory of brisk-mock and brisk-loadgen"
    "POLICIES|full-spin fixed|emission policies, in group order (full-spin, fixed)"
    "COMMITS|5 10 15 20 30|commit windows of the fixed policy, one variant each, microseconds"
    "WRITE_LAG_LIMIT_US|20|mock write lag p99 limit of the selfcheck, microseconds (the M0 default, see run-m0.sh)"
    "TLS_MODES|plain tls|transports (plain, tls)"
    "CONCURRENCIES|100 250 500|selfcheck target concurrencies; each runs at twice the value"
    "REPS|2|repetitions of the whole matrix"
    "ROTATE|1|1 rotates the variant order from group to group and repetition to repetition"
    "SEED|1|schedule seed of repetition 1; repetition r uses SEED+r-1"
    "WARMUP_S|10|selfcheck warmup, seconds"
    "MEASURE_S|30|selfcheck measurement, seconds"
    "SPIN_US|50|busy-wait window of mock and loadgen, microseconds"
    "TTFT_US|0|mock TTFT of the selfcheck streams, microseconds"
    "MOCK_CPUS|2-3|CPUs of brisk-mock"
    "MOCK_SHARDS|2|mock shard threads"
    "LOADGEN_CPUS|0-1|CPUs of brisk-loadgen"
    "LOADGEN_SHARDS|2|loadgen shard threads"
    "HARNESS_CPUS|4|CPUs of this script and its helpers"
    "PORT|19090|port of the mock"
    "RESERVED_PORTS|8317|ports of other services on the host, never used"
    "ALLOW_UNPREPARED_HOST|0|1 runs even if setup-host.sh --check finds settings to change"
)

log() {
    printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}

knob_names=()
declare -A knob_known=()
for entry in "${knob_table[@]}"; do
    name="${entry%%|*}"
    knob_names+=("$name")
    knob_known[$name]=1
done

print_config=0
declare -A overrides=()
while (($# > 0)); do
    case "$1" in
        -h | --help) usage; exit 0 ;;
        --print-config) print_config=1; shift; continue ;;
        --*=*) flag="${1%%=*}"; value="${1#*=}"; shift ;;
        --*)
            flag="$1"
            (($# >= 2)) || { echo "mock-emit-matrix: $flag needs a value" >&2; exit 2; }
            value="$2"
            shift 2
            ;;
        *) echo "mock-emit-matrix: unexpected argument: $1" >&2; exit 2 ;;
    esac
    name="${flag#--}"
    name="${name//-/_}"
    name="${name^^}"
    if [[ -z "${knob_known[$name]:-}" ]]; then
        echo "mock-emit-matrix: unknown option $flag (see --help)" >&2
        exit 2
    fi
    overrides[$name]="$value"
done

# Precedence: flag, then environment, then default.
for entry in "${knob_table[@]}"; do
    IFS='|' read -r name default _ <<<"$entry"
    if [[ -n "${overrides[$name]+x}" ]]; then
        printf -v "$name" '%s' "${overrides[$name]}"
    elif [[ -z "${!name+x}" ]]; then
        printf -v "$name" '%s' "$default"
    fi
done

if ((print_config)); then
    for name in "${knob_names[@]}"; do
        printf '%s=%s\n' "$name" "${!name}"
    done
    exit 0
fi

# ---------------------------------------------------------------- checks

for name in REPS SEED WARMUP_S MEASURE_S SPIN_US TTFT_US MOCK_SHARDS LOADGEN_SHARDS PORT; do
    [[ "${!name}" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer, got '${!name}'"
done
for name in ROTATE ALLOW_UNPREPARED_HOST; do
    [[ "${!name}" == 0 || "${!name}" == 1 ]] || die "$name must be 0 or 1, got '${!name}'"
done
((REPS >= 1)) || die "REPS must be at least 1"
((MEASURE_S >= 3)) || die "MEASURE_S must be at least 3 (the sampled window is MEASURE_S - 2 seconds)"
if [[ ! "$WRITE_LAG_LIMIT_US" =~ ^[0-9]+(\.[0-9]+)?$ ]] ||
    ! awk -v v="$WRITE_LAG_LIMIT_US" 'BEGIN { exit !(v > 0) }'; then
    die "WRITE_LAG_LIMIT_US must be a positive number, got '$WRITE_LAG_LIMIT_US'"
fi
read -r -a policies <<<"$POLICIES"
read -r -a commits <<<"$COMMITS"
read -r -a tls_modes <<<"$TLS_MODES"
read -r -a concurrencies <<<"$CONCURRENCIES"
((${#policies[@]} > 0 && ${#tls_modes[@]} > 0 && ${#concurrencies[@]} > 0)) ||
    die "POLICIES, TLS_MODES and CONCURRENCIES must not be empty"
for p in "${policies[@]}"; do
    [[ "$p" == full-spin || "$p" == fixed ]] || die "POLICIES may contain full-spin and fixed only, got '$p'"
done
(($(printf '%s\n' "${policies[@]}" | sort -u | wc -l) == ${#policies[@]})) || die "POLICIES names a policy twice"
for c in "${commits[@]}"; do
    [[ "$c" =~ ^[0-9]+$ ]] || die "COMMITS must be non-negative integers, got '$c'"
done
# A variant is "<policy> <commit window, or - for full-spin>"; its name tags
# the cells.
variants=()
for p in "${policies[@]}"; do
    if [[ "$p" == full-spin ]]; then
        variants+=("full-spin -")
    else
        ((${#commits[@]} > 0)) || die "COMMITS must not be empty when POLICIES has fixed"
        for c in "${commits[@]}"; do
            variants+=("fixed $((10#$c))")
        done
    fi
done
(($(printf '%s\n' "${variants[@]}" | sort -u | wc -l) == ${#variants[@]})) || die "COMMITS names a window twice"
for t in "${tls_modes[@]}"; do
    [[ "$t" == plain || "$t" == tls ]] || die "TLS_MODES may contain plain and tls only, got '$t'"
done
for c in "${concurrencies[@]}"; do
    [[ "$c" =~ ^[1-9][0-9]*$ ]] || die "CONCURRENCIES must be positive integers, got '$c'"
done
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "RUN_ID may contain letters, digits, '.', '_' and '-' only"
[[ "$RESERVED_PORTS" =~ ^[0-9]*([ ,]+[0-9]+)*$ ]] ||
    die "RESERVED_PORTS must be a list of port numbers, got '$RESERVED_PORTS'"
read -r -a reserved_ports <<<"${RESERVED_PORTS//,/ }"

# Prints the CPUs of a list such as 0-3,6 one per word.
expand_cpus() {
    local part lo hi c
    local -a parts cpu_ids=()
    IFS=, read -r -a parts <<<"$1"
    for part in "${parts[@]}"; do
        if [[ "$part" == *-* ]]; then
            lo=$((10#${part%-*}))
            hi=$((10#${part#*-}))
            ((lo <= hi)) || die "CPU range $part is reversed"
            for ((c = lo; c <= hi; c++)); do
                cpu_ids+=("$c")
            done
        else
            cpu_ids+=("$((10#$part))")
        fi
    done
    echo "${cpu_ids[*]}"
}

cpu_count() {
    local -a cpu_ids
    read -r -a cpu_ids <<<"$(expand_cpus "$1")"
    echo "${#cpu_ids[@]}"
}

[[ -r /sys/devices/system/cpu/online ]] || die "/sys/devices/system/cpu/online is not readable"
online=" $(expand_cpus "$(</sys/devices/system/cpu/online)") "
for name in MOCK_CPUS LOADGEN_CPUS HARNESS_CPUS; do
    [[ "${!name}" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] ||
        die "$name must be a CPU list like 0-1 or 2,3, got '${!name}'"
    # An assignment, so that a failed expansion stops the script.
    expanded="$(expand_cpus "${!name}")"
    for c in $expanded; do
        [[ "$online" == *" $c "* ]] || die "$name includes CPU $c, which is not online (online:$online)"
    done
done

# A shared core would show up only as a quietly worse lag in one of the
# measured processes.
disjoint() {
    local a="$1" b="$2" c
    local bset
    bset=" $(expand_cpus "${!b}") "
    for c in $(expand_cpus "${!a}"); do
        [[ "$bset" != *" $c "* ]] || die "$a (${!a}) and $b (${!b}) share CPU $c"
    done
}
disjoint MOCK_CPUS LOADGEN_CPUS
disjoint HARNESS_CPUS MOCK_CPUS
disjoint HARNESS_CPUS LOADGEN_CPUS
for pair in MOCK_SHARDS:MOCK_CPUS LOADGEN_SHARDS:LOADGEN_CPUS; do
    shards_knob="${pair%%:*}" cpus_knob="${pair#*:}"
    ((${!shards_knob} >= 1)) || die "$shards_knob must be at least 1"
    ((${!shards_knob} <= $(cpu_count "${!cpus_knob}"))) ||
        die "$shards_knob (${!shards_knob}) exceeds the $(cpu_count "${!cpus_knob}") CPU(s) of $cpus_knob (${!cpus_knob})"
done

for tool in jq curl taskset sha256sum ss getconf flock lscpu; do
    command -v "$tool" >/dev/null || die "$tool is required"
done
for bin in brisk-mock brisk-loadgen; do
    [[ -x "$BIN_DIR/$bin" ]] || die "$BIN_DIR/$bin not found; run scripts/bench/sync.sh first"
done

# A port inside the ephemeral range can be taken by any outgoing connection
# between the check and the bind.
read -r ephemeral_low _ </proc/sys/net/ipv4/ip_local_port_range
((PORT >= 1024 && PORT < ephemeral_low)) ||
    die "PORT $PORT must lie in 1024-$((ephemeral_low - 1)), below the ephemeral range"
for reserved in "${reserved_ports[@]}"; do
    ((PORT != reserved)) || die "port $PORT is reserved (RESERVED_PORTS) for another service"
done

# A listening port belongs to someone else; never bind next to it or send
# load to it.
port_free() {
    [[ -z "$(ss -Hltn "sport = :$PORT")" ]]
}
port_free || die "port $PORT is already in use on this host"

# Everything forked from here on (tee, the sampler, jq, curl) inherits this;
# the measured processes are pinned explicitly.
taskset -pc "$HARNESS_CPUS" $$ >/dev/null || die "pinning the harness to CPUs $HARNESS_CPUS failed"

mkdir -p "$RESULTS_ROOT"
# The lock of run-m0.sh: an M0 session would compete for the same cores, and
# sync.sh refuses to replace the binaries while it is held. Children inherit
# fd 9, so a leftover process keeps the lock, which is intended.
lock_file="$RESULTS_ROOT/.run-m0.lock"
exec 9>>"$lock_file"
flock -n 9 || die "a run-m0.sh or mock-emit-matrix.sh session (or a process left over from one) holds $lock_file"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$RESULTS_ROOT/$RUN_ID"
if [[ -e "$run_dir" ]] && [[ -n "$(ls -A "$run_dir")" ]]; then
    die "$run_dir already exists and is not empty; choose another RUN_ID"
fi
mkdir -p "$run_dir"/{results,stats,logs,host,bin}
: >"$run_dir/cells.jsonl"
for name in "${knob_names[@]}"; do
    printf '%s=%s\n' "$name" "${!name}"
done >"$run_dir/host/knobs.env"
# tee ignores the hangup of a closed terminal and keeps writing the log.
exec > >(trap '' INT HUP; exec tee -a --output-error=warn "$run_dir/session.log") 2>&1

# ---------------------------------------------------------------- processes

declare -A pids=()
sampler_pid=""
certs_dir=""
failed_cells=0

# Starts a process pinned with taskset in the background.
start_proc() {
    local name="$1" cpus="$2" logfile="$3"
    shift 3
    taskset -c "$cpus" "$@" >"$logfile" 2>&1 &
    pids[$name]=$!
}

# SIGTERM, then SIGKILL after 10 s.
stop_proc() {
    local name="$1" pid="${pids[$1]:-}" i
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for ((i = 0; i < 100; i++)); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            log "$name did not stop within 10 s; killing it"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
    unset "pids[$name]"
}

alive() {
    local pid="${pids[$1]:-}"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

cleanup() {
    local status=$?
    # A second Ctrl-C, a TERM or a vanished log reader must not cut the
    # clean-up short and leave a mock spinning on its core.
    trap '' INT TERM HUP PIPE
    trap - EXIT
    set +e
    if [[ -n "$sampler_pid" ]]; then
        kill "$sampler_pid" 2>/dev/null
        wait "$sampler_pid" 2>/dev/null
    fi
    local name
    for name in loadgen mock; do
        stop_proc "$name"
    done
    for name in "${!pids[@]}"; do
        stop_proc "$name"
    done
    [[ -n "$certs_dir" ]] && rm -rf "$certs_dir"
    if ((status != 0)); then
        log "session ended with status $status; results so far in $run_dir"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

# ---------------------------------------------------------------- evidence

clk_tck="$(getconf CLK_TCK)"

# Wall-clock time, per-CPU idle and total ticks, and the CPU ticks
# (utime + stime) of the given pids, as "proc <name> <ticks>" lines. Busy
# time is taken as wall clock minus idle because /proc/stat accounts idle
# exactly on a tickless kernel but samples the busy states at the tick; a
# process's utime and stime are scaled to its exact runtime.
cpu_snapshot() {
    local out="$1" entry pid stat rest
    local -a f
    shift
    {
        printf 'time %s\n' "$(date +%s.%N)"
        awk '/^cpu[0-9]+ / { print "cpu", $1, $5 + $6, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 }' /proc/stat
        for entry in "$@"; do
            pid="${entry#*=}"
            [[ -r "/proc/$pid/stat" ]] || continue
            stat="$(<"/proc/$pid/stat")"
            # Fields after "pid (comm) "; utime and stime are fields 14 and 15.
            rest="${stat##*) }"
            read -r -a f <<<"$rest"
            printf 'proc %s %s\n' "${entry%%=*}" "$((f[11] + f[12]))"
        done
    } >"$out"
}

# Utilisation between two snapshots as JSON: percent of one CPU per process,
# busy percent of the wall clock per CPU.
cpu_usage() {
    awk -v hz="$clk_tck" '
        FNR == NR {
            if ($1 == "time") t0 = $2
            else if ($1 == "proc") p0[$2] = $3
            else if ($1 == "cpu") i0[$2] = $3
            next
        }
        {
            if ($1 == "time") t1 = $2
            else if ($1 == "proc") { p1[$2] = $3; porder[++m] = $2 }
            else if ($1 == "cpu") { i1[$2] = $3; corder[++n] = $2 }
        }
        END {
            dt = t1 - t0
            w = dt * hz
            printf "{\"window_s\": %.1f, \"busy_pct\": {", dt
            sep = ""
            for (i = 1; i <= n && w > 0; i++) {
                c = corder[i]
                printf "%s\"%s\": %.1f", sep, c, 100 * (1 - (i1[c] - i0[c]) / w)
                sep = ", "
            }
            printf "}, \"procs_pct\": {"
            sep = ""
            for (j = 1; j <= m; j++) {
                p = porder[j]
                if (!(p in p0) || dt <= 0) continue
                printf "%s\"%s\": %.1f", sep, p, 100 * (p1[p] - p0[p]) / hz / dt
                sep = ", "
            }
            printf "}}\n"
        }' "$1" "$2"
}

# GET (or, with -X POST among the extra arguments, POST) on the mock of the
# current cell.
mock_request() {
    local tls="$1" path="$2"
    shift 2
    if [[ "$tls" == tls ]]; then
        curl -sf --max-time 5 --cacert "$certs_dir/ca.pem" "$@" "https://127.0.0.1:$PORT$path"
    else
        curl -sf --max-time 5 "$@" "http://127.0.0.1:$PORT$path"
    fi
}

# Waits until the mock answers /v1/models.
wait_mock_ready() {
    local tls="$1" i
    for ((i = 0; i < 100; i++)); do
        alive mock || die "brisk-mock exited during startup; see its log in $run_dir/logs"
        if mock_request "$tls" /v1/models -o /dev/null --max-time 1; then
            return 0
        fi
        sleep 0.1
    done
    die "brisk-mock not ready on port $PORT after 10 s"
}

# Background sampler of one cell: at the end of the warmup it resets the
# mock statistics and takes the first CPU snapshot, at the end of the
# measurement window the second one and the mock statistics. Its sleeps are
# waited on so that a TERM ends them too.
sample_cell() {
    local tag="$1" tls="$2" nap_pid=""
    shift 2
    trap 'if [[ -n "$nap_pid" ]]; then kill "$nap_pid" 2>/dev/null; fi; exit 0' TERM
    sleep "$((WARMUP_S + 1))" &
    nap_pid=$!
    wait "$nap_pid"
    mock_request "$tls" /__bench/reset -X POST -o /dev/null ||
        echo "reset failed" >"$run_dir/logs/$tag.reset-failed"
    cpu_snapshot "$run_dir/logs/$tag.cpu-a" "$@"
    sleep "$((MEASURE_S - 2))" &
    nap_pid=$!
    wait "$nap_pid"
    cpu_snapshot "$run_dir/logs/$tag.cpu-b" "$@"
    mock_request "$tls" /__bench/stats >"$run_dir/stats/$tag.window.json" ||
        rm -f "$run_dir/stats/$tag.window.json"
}

# Appends the identity and per-thread CPU affinity of a running process.
fingerprint_proc() {
    local name="$1" tag="$2" pid="${pids[$1]:-}" threads="" task
    [[ -n "$pid" && -d "/proc/$pid" ]] || return 0
    for task in /proc/"$pid"/task/*; do
        threads+="${task##*/}"$'\t'"$(<"$task/comm")"$'\t'"$(awk '/^Cpus_allowed_list:/ {print $2}' "$task/status")"$'\n'
    done
    jq -nc --arg name "$name" --arg tag "$tag" --argjson pid "$pid" \
        --arg cmdline "$(tr '\0' ' ' <"/proc/$pid/cmdline")" --arg threads "$threads" \
        '{name: $name, tag: $tag, pid: $pid, cmdline: ($cmdline | rtrimstr(" ")),
          threads: ($threads | split("\n") | map(select(length > 0) | split("\t")
                    | {tid: (.[0] | tonumber), comm: .[1], cpus: .[2]}))}' \
        >>"$run_dir/logs/processes.jsonl"
}

# jq: the fields of a loadgen result file that a cell line keeps.
# shellcheck disable=SC2016
jq_result='
def us: if . == null then null
        else {count, p50_us: (.p50_ns / 1000), p99_us: (.p99_ns / 1000),
              p999_us: (.p999_ns / 1000), max_us: (.max_ns / 1000)}
        end;
{valid: .validity.valid, reasons: .validity.reasons,
 selfcheck_pass: .loadgen.selfcheck.pass, criteria: .loadgen.selfcheck.criteria,
 mock_write_lag: (.summary.mock_write_lag | us),
 request_slip: (.loadgen.diagnostics.request_slip | us),
 ttft: (.summary.ttft | us), emit_lag: (.summary.emit_lag | us),
 chunk_latency: (.summary.chunk_latency | us),
 diagnostics: (.loadgen.diagnostics | del(.request_slip, .fresh_conn_ttft)),
 fresh_conn_ttft: (.loadgen.diagnostics.fresh_conn_ttft | us),
 load_check: .loadgen.load_check, counters: .loadgen.counters,
 duration_s: .loadgen.duration_s}'

# jq: a /__bench/stats document reduced to its numbers, the emission settings
# the mock reports (emit: policy, spin and commit window) and the per-shard
# counters, which show whether the shards share the load evenly.
# shellcheck disable=SC2016
jq_mock='
. as $s
| {requests: $s.requests, accepts: $s.accepts, chunks: $s.chunks, write_blocked: $s.write_blocked,
   errors: $s.errors,
   read_lag: ($s.read_lag | if . == null then null
                            else {samples, p50_us: (.p50_ns / 1000), p99_us: (.p99_ns / 1000),
                                  p999_us: (.p999_ns / 1000), max_us: (.max_ns / 1000), clock_steps}
                            end),
   emit: $s.emit,
   shards: $s.shards}'

# ---------------------------------------------------------------- session

log "session $RUN_ID in $run_dir"
log "mock $MOCK_SHARDS shard(s) on CPUs $MOCK_CPUS, loadgen $LOADGEN_SHARDS shard(s) on $LOADGEN_CPUS, harness $HARNESS_CPUS"
if [[ -f "$repo_root/.sync-info" ]]; then
    cp "$repo_root/.sync-info" "$run_dir/host/sync-info"
elif git -C "$repo_root" rev-parse HEAD >/dev/null 2>&1; then
    echo "commit=$(git -C "$repo_root" rev-parse HEAD)" >"$run_dir/host/sync-info"
else
    echo "commit=unknown" >"$run_dir/host/sync-info"
fi

# A sync during the session would replace the binaries under BIN_DIR; the
# session runs these copies.
for bin in brisk-mock brisk-loadgen; do
    cp -p "$BIN_DIR/$bin" "$run_dir/bin/$bin"
done
mock="$run_dir/bin/brisk-mock"
loadgen="$run_dir/bin/brisk-loadgen"
for bin in "$mock" "$loadgen"; do
    printf '%s  %s  %s\n' "$(sha256sum "$bin" | cut -d' ' -f1)" "$("$bin" --version)" "$bin"
done >"$run_dir/host/binaries.txt"
serve_help="$("$mock" serve --help)"
for flag in --emit-policy --commit-us; do
    [[ "$serve_help" == *"$flag"* ]] || die "$BIN_DIR/brisk-mock serve has no $flag; sync a build that has it"
done
[[ "$("$loadgen" selfcheck --help)" == *--max-mock-write-lag-us* ]] ||
    die "$BIN_DIR/brisk-loadgen selfcheck has no --max-mock-write-lag-us; sync a build that has it"

host_rc=0
bash "$repo_root/scripts/bench/setup-host.sh" --check >"$run_dir/host/setup-host.txt" 2>&1 || host_rc=$?
case "$host_rc" in
    0) ;;
    3)
        grep -F '[ ]' "$run_dir/host/setup-host.txt" || true
        ((ALLOW_UNPREPARED_HOST)) ||
            die "host not prepared (see host/setup-host.txt); run scripts/bench/setup-host.sh --apply, or set ALLOW_UNPREPARED_HOST=1"
        log "WARNING: host not prepared; continuing because ALLOW_UNPREPARED_HOST=1"
        ;;
    *)
        ((ALLOW_UNPREPARED_HOST)) ||
            die "setup-host.sh --check failed with status $host_rc; see host/setup-host.txt"
        log "WARNING: setup-host.sh --check failed with status $host_rc; continuing because ALLOW_UNPREPARED_HOST=1"
        ;;
esac
lscpu >"$run_dir/host/lscpu.txt"
uname -a >"$run_dir/host/uname.txt"

certs_dir="$(mktemp -d)"
"$mock" gen-cert --out-dir "$certs_dir" --san 127.0.0.1 localhost >"$run_dir/logs/gen-cert.log" 2>&1 ||
    die "gen-cert failed; see logs/gen-cert.log"

# Prints the name of a variant: full-spin, or fixed-<commit>us.
variant_name() {
    if [[ "$2" == - ]]; then echo "$1"; else echo "$1-${2}us"; fi
}

n_variants=${#variants[@]}
n_cells=$((REPS * ${#tls_modes[@]} * ${#concurrencies[@]} * n_variants))
cell_s=$((WARMUP_S + MEASURE_S + 8))
variant_names=()
for v in "${variants[@]}"; do
    read -r p c <<<"$v"
    variant_names+=("$(variant_name "$p" "$c")")
done
log "variants: ${variant_names[*]}"
log "$n_cells cell(s), about $((n_cells * cell_s / 60)) min"

# One cell: a fresh mock with the variant, one selfcheck, one line in
# cells.jsonl.
run_cell() {
    local rep="$1" group="$2" position="$3" policy="$4" commit="$5" tls="$6" conc="$7" variant
    variant="$(variant_name "$policy" "$commit")"
    local tag="r$rep-$tls-c$conc-$variant" scheme=http seed=$((SEED + rep - 1))
    local -a mock_args=(serve --listen "127.0.0.1:$PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS"
        --spin-us "$SPIN_US" --emit-policy "$policy")
    local -a lg_tls=()
    if [[ "$commit" == - ]]; then
        commit=null
    else
        mock_args+=(--commit-us "$commit")
    fi
    if [[ "$tls" == tls ]]; then
        scheme=https
        mock_args+=(--tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key")
        lg_tls=(--tls-ca "$certs_dir/ca.pem")
    fi
    log "== cell $tag (repetition $rep of $REPS, group $group, position $position)"
    # The previous cell's mock is gone; anything listening now is foreign.
    port_free || die "port $PORT is in use by another process"
    start_proc mock "$MOCK_CPUS" "$run_dir/logs/$tag.mock.log" "$mock" "${mock_args[@]}"
    wait_mock_ready "$tls"
    fingerprint_proc mock "$tag"
    mock_request "$tls" /__bench/reset -X POST -o /dev/null || die "resetting the mock statistics failed"

    local result="$run_dir/results/$tag.json" started rc
    local -a lg_args=(selfcheck "$scheme://127.0.0.1:$PORT" --concurrency "$conc" --ttft-us "$TTFT_US"
        --warmup-s "$WARMUP_S" --measure-s "$MEASURE_S" --label "$variant-$tls" --out "$result"
        --seed "$seed" --pair-id "$tls-c$conc-r$rep" --shards "$LOADGEN_SHARDS"
        --cpu-list "$LOADGEN_CPUS" --spin-us "$SPIN_US" --max-mock-write-lag-us "$WRITE_LAG_LIMIT_US"
        "${lg_tls[@]}")
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    log "  loadgen ${lg_args[*]}"
    taskset -c "$LOADGEN_CPUS" "$loadgen" "${lg_args[@]}" >"$run_dir/logs/$tag.out" 2>"$run_dir/logs/$tag.err" &
    pids[loadgen]=$!
    sample_cell "$tag" "$tls" "mock=${pids[mock]}" "loadgen=${pids[loadgen]}" &
    sampler_pid=$!
    if wait "${pids[loadgen]}"; then rc=0; else rc=$?; fi
    unset "pids[loadgen]"
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""

    local mock_alive=true after="$run_dir/stats/$tag.after.json" window="$run_dir/stats/$tag.window.json"
    alive mock || mock_alive=false
    if [[ "$mock_alive" == true ]]; then
        mock_request "$tls" /__bench/stats >"$after" || rm -f "$after"
    fi
    stop_proc mock

    local cpu=null res=null stats_window=null stats_after=null
    if [[ -f "$run_dir/logs/$tag.cpu-a" && -f "$run_dir/logs/$tag.cpu-b" ]]; then
        cpu="$(cpu_usage "$run_dir/logs/$tag.cpu-a" "$run_dir/logs/$tag.cpu-b")"
    fi
    [[ -f "$result" ]] && res="$(jq -c "$jq_result" "$result")"
    [[ -s "$window" ]] && stats_window="$(jq -c "$jq_mock" "$window")"
    [[ -s "$after" ]] && stats_after="$(jq -c "$jq_mock" "$after")"
    local reset_ok=true
    [[ ! -f "$run_dir/logs/$tag.reset-failed" ]] || reset_ok=false

    jq -nc --argjson rep "$rep" --argjson group "$group" --argjson position "$position" \
        --arg policy "$policy" --arg variant "$variant" --argjson commit "$commit" \
        --arg tls "$tls" --argjson conc "$conc" --argjson lag_limit "$WRITE_LAG_LIMIT_US" \
        --argjson seed "$seed" --arg tag "$tag" --arg started "$started" --argjson rc "$rc" \
        --argjson mock_alive "$mock_alive" --argjson reset_ok "$reset_ok" \
        --argjson res "$res" --argjson cpu "$cpu" \
        --argjson window "$stats_window" --argjson after "$stats_after" \
        --arg mock_cpus "$MOCK_CPUS" --arg loadgen_cpus "$LOADGEN_CPUS" \
        '{rep: $rep, group: $group, position: $position, policy: $policy, commit_us: $commit,
          variant: $variant, write_lag_limit_us: $lag_limit,
          tls: $tls, concurrency: $conc, streams: (2 * $conc), seed: $seed, tag: $tag,
          started: $started, loadgen_rc: $rc, mock_alive: $mock_alive,
          result: (if $res == null then null else "results/\($tag).json" end),
          selfcheck_pass: $res.selfcheck_pass, criteria: $res.criteria,
          valid: $res.valid, reasons: $res.reasons,
          mock_write_lag: $res.mock_write_lag, request_slip: $res.request_slip,
          ttft: $res.ttft, emit_lag: $res.emit_lag, chunk_latency: $res.chunk_latency,
          fresh_conn_ttft: $res.fresh_conn_ttft, diagnostics: $res.diagnostics,
          load_check: $res.load_check, loadgen_counters: $res.counters,
          mock_stats_reset_at_warmup_end: $reset_ok,
          mock: $window, mock_after_run: $after,
          mock_cpu_pct: $cpu.procs_pct.mock, loadgen_cpu_pct: $cpu.procs_pct.loadgen,
          cpu_busy_pct: $cpu.busy_pct, cpu_window_s: $cpu.window_s,
          mock_cpus: $mock_cpus, loadgen_cpus: $loadgen_cpus}' >>"$run_dir/cells.jsonl"

    if [[ "$res" == null ]]; then
        failed_cells=$((failed_cells + 1))
        log "  $tag: NO RESULT (loadgen exit status $rc); see logs/$tag.err"
    else
        sed -n '/^validity/,$p' "$run_dir/logs/$tag.out" | sed 's/^/    /'
        log "  $tag: selfcheck $(jq -r 'if .selfcheck_pass then "PASS" else "FAIL" end' <<<"$res")"
    fi
    if [[ "$mock_alive" != true ]]; then
        failed_cells=$((failed_cells + 1))
        log "  $tag: brisk-mock exited during the run; see logs/$tag.mock.log"
    fi
}

# group numbers the groups of the whole session (the cells' order);
# rep_group counts them within the repetition and sets the rotation offset.
group=0
for ((rep = 1; rep <= REPS; rep++)); do
    rep_group=0
    for tls in "${tls_modes[@]}"; do
        for conc in "${concurrencies[@]}"; do
            for ((i = 0; i < n_variants; i++)); do
                j=$i
                ((ROTATE)) && j=$(((i + rep_group + rep) % n_variants))
                read -r p c <<<"${variants[$j]}"
                run_cell "$rep" "$group" "$i" "$p" "$c" "$tls" "$conc"
            done
            group=$((group + 1))
            rep_group=$((rep_group + 1))
        done
    done
done

# ---------------------------------------------------------------- table

# jq: fixed-width columns and the table of every cell, then one line per
# variant, transport and concurrency over the repetitions.
# shellcheck disable=SC2016
jq_table='
def pad($n): tostring | ($n - length) as $k | if $k > 0 then (" " * $k) + . else . end;
def rpad($n): tostring | ($n - length) as $k | if $k > 0 then . + (" " * $k) else . end;
def num: if . == null then "-" else (. * 100 | round / 100) end;
def median: sort | length as $n
    | if $n == 0 then null
      elif $n % 2 == 1 then .[($n - 1) / 2]
      else (.[$n / 2 - 1] + .[$n / 2]) / 2
      end;
def row: [(.rep | pad(3)), (.tls | rpad(5)), (.concurrency | pad(5)), (.variant | rpad(10)),
          (if .selfcheck_pass == true then "PASS" elif .selfcheck_pass == false then "FAIL" else "NONE" end),
          (.mock_write_lag.p50_us | num | pad(7)), (.mock_write_lag.p99_us | num | pad(7)),
          (.mock_write_lag.p999_us | num | pad(8)), (.mock_write_lag.max_us | num | pad(8)),
          (.request_slip.p50_us | num | pad(8)), (.request_slip.p99_us | num | pad(8)),
          (.request_slip.max_us | num | pad(9)),
          (.ttft.p50_us | num | pad(9)), (.ttft.p99_us | num | pad(9)),
          (.mock.read_lag.p99_us | num | pad(8)), (.mock.read_lag.max_us | num | pad(9)),
          (.mock_cpu_pct | num | pad(6))] | join(" ");
"Per cell (microseconds; wl = MockWriteLag, slip = request slip, rl = mock read lag, window after warmup):",
([("rep" | pad(3)), ("tls" | rpad(5)), ("conc" | pad(5)), ("variant" | rpad(10)), "sc  ",
  ("wl p50" | pad(7)), ("wl p99" | pad(7)), ("wl p999" | pad(8)), ("wl max" | pad(8)),
  ("slip p50" | pad(8)), ("slip p99" | pad(8)), ("slip max" | pad(9)),
  ("ttft p50" | pad(9)), ("ttft p99" | pad(9)), ("rl p99" | pad(8)), ("rl max" | pad(9)),
  ("cpu%" | pad(6))] | join(" ")),
(sort_by(.group, .position)[] | row),
"",
"Over repetitions (median of the per-cell p99, worst max, selfcheck passes):",
([("tls" | rpad(5)), ("conc" | pad(5)), ("variant" | rpad(10)), ("pass" | pad(5)),
  ("wl p99" | pad(7)), ("wl max" | pad(8)), ("slip p99" | pad(8)), ("slip max" | pad(9)),
  ("ttft p99" | pad(9)), ("rl p99" | pad(8))] | join(" ")),
(group_by([.tls, .concurrency, .policy, .commit_us])[]
 | . as $g
 | [($g[0].tls | rpad(5)), ($g[0].concurrency | pad(5)), ($g[0].variant | rpad(10)),
    ("\([$g[] | select(.selfcheck_pass == true)] | length)/\($g | length)" | pad(5)),
    ([$g[].mock_write_lag.p99_us | numbers] | median | num | pad(7)),
    ([$g[].mock_write_lag.max_us | numbers] | max | num | pad(8)),
    ([$g[].request_slip.p99_us | numbers] | median | num | pad(8)),
    ([$g[].request_slip.max_us | numbers] | max | num | pad(9)),
    ([$g[].ttft.p99_us | numbers] | median | num | pad(9)),
    ([$g[].mock.read_lag.p99_us | numbers] | median | num | pad(8))] | join(" "))'

jq -rs "$jq_table" "$run_dir/cells.jsonl" >"$run_dir/table.txt" || die "building the table failed"
cat "$run_dir/table.txt"
log "session $RUN_ID done: $n_cells cell(s), $failed_cells failure(s); results in $run_dir"
((failed_cells == 0)) || exit 1
