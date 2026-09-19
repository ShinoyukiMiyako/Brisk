#!/usr/bin/env bash
# Knobs such as LAYOUT and S3_SIZES are assigned indirectly by the knob table.
# shellcheck disable=SC2153
# M0 measurement session on the Linux benchmark host: the direct baseline
# (arm A) against floor-A (arm B) in alternating order, under config P and
# config T, for the selfcheck (sc) and the scenarios S1 (streams), S2
# (non-streaming) and S3 (large request bodies, one run per size), followed
# by the bootstrap comparisons.
#
# Usage: scripts/bench/run-m0.sh [--<knob> <value> | --<knob>=<value>]...
#        scripts/bench/run-m0.sh --print-config [--<knob> <value>]...
#        scripts/bench/run-m0.sh --help
#
# Every knob is an environment variable, and a flag overrides it:
# `--s1-concurrency 500` sets S1_CONCURRENCY=500. --help lists the knobs.
# A session takes hours: run it detached (tmux, or systemd-run --user
# --scope), so a dropped ssh connection neither ends it nor sends terminal
# output over the network during the measurement.
#
#   config P  A: loadgen -http->  mock          B: loadgen -http->  floor -https-> mock
#   config T  A: loadgen -https-> mock          B: loadgen -https-> floor -https-> mock
#
# Core plan: LAYOUT=8vcpu (default) puts floor on CPUs 4-7, the mocks on 2-3
# (2 shards) and loadgen on 0-1 (2 shards); LAYOUT=4vcpu is the earlier plan
# of floor 0-1, mock 2, loadgen 3 (1 shard each), which holds about 500 S1
# streams (add --s1-concurrency 500). The *_CPUS and *_SHARDS knobs override
# either preset. The CPU sets must be online and must not overlap.
#
# Repetitions alternate the arm order (direct first in odd repetitions,
# floor first in even ones) so a linear drift cancels between the arms. The
# two arms of a repetition share its seed and a pairing id
# (<scenario>-<config>[-<size>]-r<N>, passed as --pair-id), and the
# comparison resamples whole repetitions as pairs, so the run-to-run spread
# of the load enters the confidence intervals; each pair's own delta is
# reported too. A single repetition leaves only the within-run bootstrap,
# which understates that spread; use at least 3. A repetition with an invalid
# run is rerun, both arms with the same seed and pairing id, up to
# RETRY_INVALID times. The selfcheck runs the load generator at twice
# SC_CONCURRENCY against a mock with the scenario core plan (and again over
# TLS when CONFIGS has T); with SC_GATE=1 a FAIL ends the session.
# Comparisons use only repetitions whose two arms are both valid.
#
# The M0 baseline criterion (direct arm p99 95% CI half width below 5%) is
# judged per metric; only the gate metrics (GATE_METRICS for S1,
# S2_GATE_METRICS, S3_GATE_METRICS) decide the gate, the others are
# reported. chunk_wire, the preferred per-chunk overhead metric, is reported
# but not gated by default.
#
# The session refuses to start unless setup-host.sh --check passes
# (ALLOW_UNPREPARED_HOST=1 overrides), holds $RESULTS_ROOT/.run-m0.lock for
# its whole life (sync.sh checks it) and runs its own copies of the
# binaries, taken from BIN_DIR at the start.
#
# Output in $RESULTS_ROOT/$RUN_ID/: manifest.json, runs.jsonl,
# compares.jsonl, processes.jsonl, session.log, results/ (RunResult files),
# compare/ (JSON and text), logs/, host/, bin/. Every process started here
# is stopped on exit, failures included. The exit status is 1 if the
# selfcheck fails, a load generator or floor run fails, a comparison is
# missing or has fewer valid repetitions than planned, or a comparison's
# gate fails (the direct baseline's p99 CI half width reaches 5% on a gate
# metric).

set -euo pipefail

usage() {
    # The leading comment block after the shellcheck directive and its note.
    awk 'NR < 4 { next } /^#/ { print; next } { exit }' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
    echo "Knobs (default, meaning):"
    local entry name default meaning
    for entry in "${knob_table[@]}"; do
        IFS='|' read -r name default meaning <<<"$entry"
        printf '  %-22s %-22s %s\n' "$name" "${default:-(none)}" "$meaning"
    done
}

# NAME|default|meaning. Defaults follow the M0 contract; an empty default
# means "not passed" or a fallback named in the meaning. Two exceptions give
# the mock a TTFT above 0, because the mock takes t0 when the request is
# complete, before it handles it, so at TTFT 0 its handling time lands in the
# mock write lag: in S3 the scan of a 100 KiB to 10 MiB body fails the 10 us
# validity limit outright, and in S2 the parsing and response set-up (about
# 2.5 us at p50) leaves too little room for one scheduler tick. The offset
# is the same in both arms and cancels in the delta.
knob_table=(
    "RUN_ID|m0-$(date -u +%Y%m%dT%H%M%SZ)|session directory name"
    "RESULTS_ROOT|$HOME/bench-results|parent of the session directory"
    "BIN_DIR|$HOME/brisk-target/release|directory of the three binaries"
    "CONFIGS|P T|configs to run, in order (P, T)"
    "SCENARIOS|sc s1 s2 s3|scenarios to run (sc, s1, s2, s3)"
    "REPS||repetitions of every scenario (default: S1 5, S2 3, S3 3)"
    "RETRY_INVALID|1|reruns of a repetition that has an invalid run"
    "SEED|1|schedule seed of repetition 1; repetition r uses SEED+r-1"
    "SPIN_US|50|busy-wait window of mock and loadgen, microseconds"
    "LAYOUT|8vcpu|core plan preset: 8vcpu or 4vcpu (see above)"
    "FLOOR_CPUS||CPUs of brisk-floor (default: from LAYOUT)"
    "FLOOR_WORKERS||floor worker threads (default: one per FLOOR_CPUS entry)"
    "MOCK_CPUS||CPUs of the two scenario mocks (default: from LAYOUT)"
    "MOCK_SHARDS||shards per scenario mock (default: from LAYOUT)"
    "LOADGEN_CPUS||CPUs of brisk-loadgen (default: from LAYOUT)"
    "LOADGEN_SHARDS||loadgen shard threads (default: from LAYOUT)"
    "HARNESS_CPUS||CPUs of this script and its helpers (default: first LOADGEN_CPUS entry)"
    "MOCK_PLAIN_PORT|19080|plaintext mock (direct-P)"
    "MOCK_TLS_PORT|19443|TLS mock (direct-T and floor upstream)"
    "FLOOR_P_PORT|19180|floor, plaintext inbound"
    "FLOOR_T_PORT|19543|floor, TLS inbound"
    "SC_PORT|19090|selfcheck mock"
    "RESERVED_PORTS|8317|ports of other services on the host, never used and sampled for CPU"
    "ALLOW_UNPREPARED_HOST|0|1 runs even if setup-host.sh --check finds settings to change"
    "SC_GATE|1|1 ends the session when the selfcheck fails"
    "SC_CONCURRENCY||selfcheck target, run at twice this (default: S1_CONCURRENCY)"
    "SC_WARMUP_S|10|selfcheck warmup, seconds"
    "SC_MEASURE_S|30|selfcheck measurement, seconds"
    "SC_MOCK_CPUS||CPUs of the selfcheck mock (default: MOCK_CPUS)"
    "SC_MOCK_SHARDS||shards of the selfcheck mock (default: MOCK_SHARDS)"
    "SC_LOADGEN_CPUS||CPUs of the selfcheck loadgen (default: LOADGEN_CPUS)"
    "SC_LOADGEN_SHARDS||selfcheck loadgen shard threads (default: LOADGEN_SHARDS)"
    "S1_REPS||S1 repetitions (default: REPS, else 5)"
    "S1_CONCURRENCY|1000|S1 target concurrent streams"
    "S1_CHUNK_RATE|30|S1 chunks per second per stream"
    "S1_DUR_MEDIAN|5|S1 median stream duration, seconds"
    "S1_DUR_P99|60|S1 p99 stream duration, seconds"
    "S1_DUR_MAX|120|S1 stream duration cap, seconds"
    "S1_TTFT_US|0|S1 mock TTFT, microseconds"
    "S1_CHUNK_BYTES|128|S1 content bytes per chunk"
    "S1_WARMUP_S|150|S1 warmup, seconds"
    "S1_MEASURE_S|300|S1 measurement, seconds"
    "S2_REPS||S2 repetitions (default: REPS, else 3)"
    "S2_RATE|2000|S2 fixed request rate per second"
    "S2_TTFT_US|200|S2 mock delay, microseconds (see above)"
    "S2_RESP_BYTES|1024|S2 response content bytes"
    "S2_PROMPT_BYTES|1024|S2 user message bytes"
    "S2_WARMUP_S|30|S2 warmup, seconds"
    "S2_MEASURE_S|300|S2 measurement, seconds"
    "S3_REPS||S3 repetitions (default: REPS, else 3)"
    "S3_SIZES|100k,1m,10m|S3 request body sizes, one loadgen run each"
    "S3_RATE|5|S3 request rate per second"
    "S3_TTFT_US|2000|S3 mock TTFT, microseconds (see above)"
    "S3_CHUNKS|1|S3 content chunks per response"
    "S3_SPIN_US|500|S3 loadgen busy-wait window, microseconds; at 5 req/s the wake-up from idle can exceed SPIN_US"
    "S3_WARMUP_S|30|S3 warmup per size, seconds"
    "S3_MEASURE_S|300|S3 measurement per size, seconds"
    "COMPARE_QUANTILES|50,99,99.9|compared quantiles of S1 and S2, percent"
    "S3_COMPARE_QUANTILES|50,99|compared quantiles of S3; its sample count leaves p99.9 at a handful of samples"
    "COMPARE_RESAMPLES|2000|bootstrap resamples"
    "GATE_METRICS|chunk_latency|S1 metrics whose baseline p99 CI decides the gate (of ttft, chunk_latency, chunk_wire); ttft is left out because its tail varies with the seed-driven schedule itself and would need about 25 repetitions to reach the 5% bound"
    "S2_GATE_METRICS|request_latency|S2 gate metrics (of request_latency)"
    "S3_GATE_METRICS|ttft|S3 gate metrics (of ttft, chunk_latency)"
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
            (($# >= 2)) || { echo "run-m0: $flag needs a value" >&2; exit 2; }
            value="$2"
            shift 2
            ;;
        *) echo "run-m0: unexpected argument: $1" >&2; exit 2 ;;
    esac
    name="${flag#--}"
    name="${name//-/_}"
    name="${name^^}"
    if [[ -z "${knob_known[$name]:-}" ]]; then
        echo "run-m0: unknown option $flag (see --help)" >&2
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

# 8vcpu: floor's four cores kept apart from the tools and away from CPU 0,
# which tends to take housekeeping work and device interrupts. The harness
# shares a loadgen core, whose emit-lag check would catch its interference,
# rather than a floor core, where nothing would.
case "$LAYOUT" in
    8vcpu) layout=(4-7 2-3 2 0-1 2) ;;
    4vcpu) layout=(0-1 2 1 3 1) ;;
    *) die "LAYOUT must be 8vcpu or 4vcpu, got '$LAYOUT'" ;;
esac
: "${FLOOR_CPUS:=${layout[0]}}" "${MOCK_CPUS:=${layout[1]}}" "${MOCK_SHARDS:=${layout[2]}}"
: "${LOADGEN_CPUS:=${layout[3]}}" "${LOADGEN_SHARDS:=${layout[4]}}"
: "${HARNESS_CPUS:=${LOADGEN_CPUS%%[,-]*}}"
# The selfcheck proves headroom only for the core plan it runs on.
: "${SC_MOCK_CPUS:=$MOCK_CPUS}" "${SC_MOCK_SHARDS:=$MOCK_SHARDS}"
: "${SC_LOADGEN_CPUS:=$LOADGEN_CPUS}" "${SC_LOADGEN_SHARDS:=$LOADGEN_SHARDS}"
: "${SC_CONCURRENCY:=$S1_CONCURRENCY}"
: "${S1_REPS:=${REPS:-5}}" "${S2_REPS:=${REPS:-3}}" "${S3_REPS:=${REPS:-3}}"

if ((print_config)); then
    for name in "${knob_names[@]}"; do
        printf '%s=%s\n' "$name" "${!name}"
    done
    exit 0
fi

# ---------------------------------------------------------------- checks

[[ -z "$REPS" || "$REPS" =~ ^[0-9]+$ ]] || die "REPS must be a non-negative integer, got '$REPS'"
for name in S1_REPS S2_REPS S3_REPS RETRY_INVALID SEED SPIN_US S3_SPIN_US \
    MOCK_SHARDS LOADGEN_SHARDS MOCK_PLAIN_PORT MOCK_TLS_PORT FLOOR_P_PORT FLOOR_T_PORT SC_PORT \
    SC_CONCURRENCY SC_WARMUP_S SC_MEASURE_S SC_MOCK_SHARDS SC_LOADGEN_SHARDS \
    S1_CONCURRENCY S1_TTFT_US S1_CHUNK_BYTES S1_WARMUP_S S1_MEASURE_S \
    S2_TTFT_US S2_RESP_BYTES S2_PROMPT_BYTES S2_WARMUP_S S2_MEASURE_S \
    S3_TTFT_US S3_CHUNKS S3_WARMUP_S S3_MEASURE_S COMPARE_RESAMPLES; do
    [[ "${!name}" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer, got '${!name}'"
done
for name in SC_GATE ALLOW_UNPREPARED_HOST; do
    [[ "${!name}" == 0 || "${!name}" == 1 ]] || die "$name must be 0 or 1, got '${!name}'"
done
[[ -z "$FLOOR_WORKERS" || "$FLOOR_WORKERS" =~ ^[1-9][0-9]*$ ]] ||
    die "FLOOR_WORKERS must be a positive integer, got '$FLOOR_WORKERS'"
for c in $CONFIGS; do
    [[ "$c" == P || "$c" == T ]] || die "CONFIGS may contain P and T only, got '$c'"
done
for s in $SCENARIOS; do
    [[ "$s" =~ ^(sc|s1|s2|s3)$ ]] || die "SCENARIOS may contain sc, s1, s2 and s3 only, got '$s'"
done
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "RUN_ID may contain letters, digits, '.', '_' and '-' only"
[[ "$RESERVED_PORTS" =~ ^[0-9]*([ ,]+[0-9]+)*$ ]] ||
    die "RESERVED_PORTS must be a list of port numbers, got '$RESERVED_PORTS'"
read -r -a reserved_ports <<<"${RESERVED_PORTS//,/ }"

has_scenario() {
    [[ " $SCENARIOS " == *" $1 "* ]]
}

has_config() {
    [[ " $CONFIGS " == *" $1 "* ]]
}

# Prints the metrics a scenario compares; its gate metrics must be among
# them.
compared_metrics() {
    case "$1" in
        s1) echo ttft,chunk_latency,chunk_wire ;;
        s2) echo request_latency ;;
        s3) echo ttft,chunk_latency ;;
    esac
}

# Prints the gate metrics of a scenario.
gate_metrics() {
    case "$1" in
        s1) echo "$GATE_METRICS" ;;
        s2) echo "$S2_GATE_METRICS" ;;
        s3) echo "$S3_GATE_METRICS" ;;
    esac
}

# A gate metric the scenario does not compare would make the gate vacuous
# or refuse the comparison only at the end of an hours-long session.
for s in s1 s2 s3; do
    has_scenario "$s" || continue
    gate_var=GATE_METRICS
    [[ "$s" == s1 ]] || gate_var="${s^^}_GATE_METRICS"
    compared="$(compared_metrics "$s")"
    [[ "${!gate_var}" =~ ^[a-z_]+(,[a-z_]+)*$ ]] ||
        die "$gate_var must be a comma-separated list of metrics, got '${!gate_var}'"
    IFS=, read -r -a gate_list <<<"${!gate_var}"
    seen=","
    for metric in "${gate_list[@]}"; do
        [[ ",$compared," == *",$metric,"* ]] ||
            die "$gate_var names $metric, which $s does not compare (it compares $compared)"
        # A repeated metric would fail the post-compare consistency check,
        # hours into the session.
        [[ "$seen" != *",$metric,"* ]] || die "$gate_var names $metric twice"
        seen+="$metric,"
    done
done

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
for name in FLOOR_CPUS MOCK_CPUS LOADGEN_CPUS SC_MOCK_CPUS SC_LOADGEN_CPUS HARNESS_CPUS; do
    [[ "${!name}" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] ||
        die "$name must be a CPU list like 0-1 or 2,3, got '${!name}'"
    # An assignment, so that a failed expansion stops the script.
    expanded="$(expand_cpus "${!name}")"
    for c in $expanded; do
        [[ "$online" == *" $c "* ]] || die "$name includes CPU $c, which is not online (online:$online)"
    done
done

# Overlapping sets would let the tools and floor compete for a core without
# any error, which only shows up as a quietly skewed delta.
disjoint() {
    local a="$1" b="$2" c
    local bset
    bset=" $(expand_cpus "${!b}") "
    for c in $(expand_cpus "${!a}"); do
        [[ "$bset" != *" $c "* ]] || die "$a (${!a}) and $b (${!b}) share CPU $c"
    done
}
disjoint FLOOR_CPUS MOCK_CPUS
disjoint FLOOR_CPUS LOADGEN_CPUS
disjoint MOCK_CPUS LOADGEN_CPUS
disjoint SC_MOCK_CPUS SC_LOADGEN_CPUS
for pair in MOCK_SHARDS:MOCK_CPUS LOADGEN_SHARDS:LOADGEN_CPUS \
    SC_MOCK_SHARDS:SC_MOCK_CPUS SC_LOADGEN_SHARDS:SC_LOADGEN_CPUS; do
    shards_knob="${pair%%:*}" cpus_knob="${pair#*:}"
    ((${!shards_knob} >= 1)) || die "$shards_knob must be at least 1"
    ((${!shards_knob} <= $(cpu_count "${!cpus_knob}"))) ||
        die "$shards_knob (${!shards_knob}) exceeds the $(cpu_count "${!cpus_knob}") CPU(s) of $cpus_knob (${!cpus_knob})"
done

for tool in jq curl taskset nstat sha256sum ss getconf flock lscpu; do
    command -v "$tool" >/dev/null || die "$tool is required"
done
for bin in brisk-mock brisk-floor brisk-loadgen; do
    [[ -x "$BIN_DIR/$bin" ]] || die "$BIN_DIR/$bin not found; run scripts/bench/sync.sh first"
done

ports=("$MOCK_PLAIN_PORT" "$MOCK_TLS_PORT" "$FLOOR_P_PORT" "$FLOOR_T_PORT" "$SC_PORT")
if (($(printf '%s\n' "${ports[@]}" | sort -u | wc -l) != ${#ports[@]})); then
    die "the five ports must differ: ${ports[*]}"
fi
# A port inside the ephemeral range can be taken by any outgoing connection
# between the check below and the bind.
read -r ephemeral_low _ </proc/sys/net/ipv4/ip_local_port_range
for port in "${ports[@]}"; do
    ((port >= 1024 && port < ephemeral_low)) ||
        die "port $port must lie in 1024-$((ephemeral_low - 1)), below the ephemeral range"
    for reserved in "${reserved_ports[@]}"; do
        ((port != reserved)) || die "port $port is reserved (RESERVED_PORTS) for another service"
    done
    # A listening port belongs to someone else; never bind next to it or
    # send load to it.
    if [[ -n "$(ss -Hltn "sport = :$port")" ]]; then
        die "port $port is already in use on this host"
    fi
done

if has_scenario s3; then
    s3_samples="$(awk -v r="$S3_RATE" -v m="$S3_MEASURE_S" -v n="$S3_REPS" 'BEGIN { printf "%d", r * m * n }')"
fi

# Everything forked from here on (tee, the sampler, jq, curl) inherits this;
# the measured processes are pinned explicitly.
taskset -pc "$HARNESS_CPUS" $$ >/dev/null || die "pinning the harness to CPUs $HARNESS_CPUS failed"

mkdir -p "$RESULTS_ROOT"
# Children inherit fd 9, so a process left over from a crashed session keeps
# the lock, which is intended: it still holds ports and CPUs.
lock_file="$RESULTS_ROOT/.run-m0.lock"
exec 9>>"$lock_file"
flock -n 9 || die "another run-m0.sh session (or a process left over from one) holds $lock_file"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$RESULTS_ROOT/$RUN_ID"
if [[ -e "$run_dir" ]] && [[ -n "$(ls -A "$run_dir")" ]]; then
    die "$run_dir already exists and is not empty; choose another RUN_ID"
fi
mkdir -p "$run_dir"/{results,compare,logs,host,bin}
touch "$run_dir"/{runs,compares,processes}.jsonl
# write_manifest reads these; they exist from here on so that a session
# aborted during set-up still gets a manifest.
: >"$run_dir/host/sync-info"
: >"$run_dir/host/binaries.txt"
for name in "${knob_names[@]}"; do
    printf '%s=%s\n' "$name" "${!name}"
done >"$run_dir/host/knobs.env"
# tee ignores the hangup of a closed terminal and keeps writing the log.
exec > >(trap '' INT HUP; exec tee -a --output-error=warn "$run_dir/session.log") 2>&1

# ---------------------------------------------------------------- processes

declare -A pids=()
declare -A foreign_pids=()
lg_args=()
lg_out=""
lg_cpus=""
# Pairing id of the run in progress; empty for the selfcheck.
lg_pair_id=""
sampler_pid=""
certs_dir=""
sc_tls=0
run_ok=1
manifest_written=0
session_started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
invalid_runs=0
failed_runs=0
failed_compares=0
baseline_ci_failures=0
retries=0
host_ready=unknown
selfcheck_status=not_run
selfcheck_detail=""

# Starts a process pinned with taskset in the background.
start_proc() {
    local name="$1" cpus="$2" logfile="$3"
    shift 3
    taskset -c "$cpus" "$@" >"$logfile" 2>&1 &
    pids[$name]=$!
}

# SIGTERM, then a second SIGTERM (floor abandons its graceful drain on the
# second signal), then SIGKILL.
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
            log "$name did not stop within 10 s; signalling again"
            kill -TERM "$pid" 2>/dev/null || true
            sleep 2
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

# Waits until an HTTP GET on the URL succeeds; the extra arguments go to curl.
wait_ready() {
    local name="$1" url="$2" i
    shift 2
    for ((i = 0; i < 100; i++)); do
        alive "$name" || die "$name exited during startup; see $run_dir/logs/$name*.log"
        if curl -sf -o /dev/null --max-time 1 "$@" "$url"; then
            return 0
        fi
        sleep 0.1
    done
    die "$name not ready at $url after 10 s"
}

# Appends the identity and per-thread CPU affinity of a running process.
fingerprint_proc() {
    local name="$1" tag="$2" pid="${pids[$1]:-}" threads="" task
    [[ -n "$pid" && -d "/proc/$pid" ]] || return 0
    for task in /proc/"$pid"/task/*; do
        threads+="${task##*/}"$'\t'"$(<"$task/comm")"$'\t'"$(awk '/^Cpus_allowed_list:/ {print $2}' "$task/status")"$'\n'
    done
    jq -nc --arg name "$name" --arg tag "$tag" --argjson pid "$pid" \
        --arg cmdline "$(tr '\0' ' ' <"/proc/$pid/cmdline")" \
        --arg exe "$(readlink "/proc/$pid/exe")" --arg threads "$threads" \
        '{name: $name, tag: $tag, pid: $pid, cmdline: ($cmdline | rtrimstr(" ")), exe: $exe,
          threads: ($threads | split("\n") | map(select(length > 0) | split("\t")
                    | {tid: (.[0] | tonumber), comm: .[1], cpus: .[2]}))}' \
        >>"$run_dir/processes.jsonl"
}

cleanup() {
    local status=$?
    # A second Ctrl-C, a TERM or a vanished log reader must not cut the
    # clean-up short and leave floor or a mock running.
    trap '' INT TERM HUP PIPE
    trap - EXIT
    set +e
    if [[ -n "$sampler_pid" ]]; then
        kill "$sampler_pid" 2>/dev/null
        wait "$sampler_pid" 2>/dev/null
    fi
    local name
    for name in loadgen floor mock-sc mock-plain mock-tls; do
        stop_proc "$name"
    done
    for name in "${!pids[@]}"; do
        stop_proc "$name"
    done
    [[ -n "$certs_dir" ]] && rm -rf "$certs_dir"
    if ((!manifest_written)) && [[ -d "$run_dir" ]]; then
        write_manifest aborted
    fi
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

# Wall-clock time, per-CPU idle/steal/total ticks, per-CPU softirq event
# counts and per-process CPU ticks.
#
# /proc/stat accounts idle and iowait exactly on a tickless kernel, but
# user, system, irq and softirq only as samples at the scheduler tick.
# Short bursts on a fixed grid (S2, S3) phase-lock with the tick and are
# missed or overcounted, so busy time is taken as the wall clock minus idle,
# and softirq load as event counts from /proc/softirqs. A process's utime
# and stime are scaled to its exact runtime by the kernel and need no
# correction.
cpu_snapshot() {
    local out="$1" name pid stat rest
    local -a f
    shift
    {
        printf 'time %s\n' "$(date +%s.%N)"
        # user nice system idle iowait irq softirq steal ($2..$9); guest
        # time is already inside user and nice.
        awk '/^cpu[0-9]+ / { print "cpu", $1, $5 + $6, $9, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 }' /proc/stat
        awk 'NR == 1 { n = NF; for (i = 1; i <= NF; i++) cpu[i] = tolower($i); next }
             $1 == "NET_RX:" || $1 == "NET_TX:" || $1 == "TIMER:" {
                 k = tolower(substr($1, 1, length($1) - 1))
                 for (i = 1; i <= n; i++) print "sirq", cpu[i], k, $(i + 1)
             }' /proc/softirqs
        for name in "$@"; do
            pid="${pids[$name]:-}"
            [[ -n "$pid" && -r "/proc/$pid/stat" ]] || continue
            stat="$(<"/proc/$pid/stat")"
            # Fields after "pid (comm) "; utime and stime are fields 14 and 15.
            rest="${stat##*) }"
            read -r -a f <<<"$rest"
            printf 'proc %s %s\n' "$name" "$((f[11] + f[12]))"
        done
        for name in "${!foreign_pids[@]}"; do
            pid="${foreign_pids[$name]}"
            [[ -r "/proc/$pid/stat" ]] || continue
            stat="$(<"/proc/$pid/stat")"
            rest="${stat##*) }"
            read -r -a f <<<"$rest"
            printf 'proc %s %s\n' "$name" "$((f[11] + f[12]))"
        done
    } >"$out"
}

# Utilisation between two snapshots as JSON: percent of one CPU per process;
# per CPU the busy and steal percent of the wall clock, the softirq events
# per second, and stat_coverage, the tick total over the wall clock (far
# from 1 when tick sampling misread the window).
cpu_usage() {
    awk -v hz="$clk_tck" '
        FNR == NR {
            if ($1 == "time") t0 = $2
            else if ($1 == "proc") p0[$2] = $3
            else if ($1 == "cpu") { i0[$2] = $3; st0[$2] = $4; T0[$2] = $5 }
            else if ($1 == "sirq") q0[$2, $3] = $4
            next
        }
        {
            if ($1 == "time") t1 = $2
            else if ($1 == "proc") { p1[$2] = $3; porder[++m] = $2 }
            else if ($1 == "cpu") { i1[$2] = $3; st1[$2] = $4; T1[$2] = $5; corder[++n] = $2 }
            else if ($1 == "sirq") q1[$2, $3] = $4
        }
        END {
            dt = t1 - t0
            w = dt * hz
            printf "{\"window_s\": %.1f, \"cpus\": {", dt
            sep = ""
            for (i = 1; i <= n && w > 0; i++) {
                c = corder[i]
                printf "%s\"%s\": {\"busy_pct\": %.1f, \"steal_pct\": %.1f, \"stat_coverage\": %.2f", \
                    sep, c, 100 * (1 - (i1[c] - i0[c]) / w), 100 * (st1[c] - st0[c]) / w, (T1[c] - T0[c]) / w
                split("net_rx net_tx timer", kinds, " ")
                for (k = 1; k <= 3; k++) {
                    if ((c, kinds[k]) in q1 && (c, kinds[k]) in q0)
                        printf ", \"%s_per_s\": %.0f", kinds[k], (q1[c, kinds[k]] - q0[c, kinds[k]]) / dt
                }
                printf "}"
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

# Snapshots CPU usage and interrupt counts over [offset, offset + window]
# seconds from now and records the load generator's thread placement at the
# start of the window. Runs in the background; its sleeps are waited on so
# that a TERM from execute_run or cleanup ends them too, instead of leaving
# a sleep that holds the session log's pipe open.
sample_window() {
    local tag="$1" offset="$2" window="$3" nap_pid=""
    shift 3
    trap 'if [[ -n "$nap_pid" ]]; then kill "$nap_pid" 2>/dev/null; fi; exit 0' TERM
    sleep "$offset" &
    nap_pid=$!
    wait "$nap_pid"
    fingerprint_proc loadgen "$tag"
    cat /proc/interrupts >"$run_dir/logs/$tag.irq-a"
    cpu_snapshot "$run_dir/logs/$tag.cpu-a" "$@"
    sleep "$window" &
    nap_pid=$!
    wait "$nap_pid"
    cpu_snapshot "$run_dir/logs/$tag.cpu-b" "$@"
    cat /proc/interrupts >"$run_dir/logs/$tag.irq-b"
}

# Loopback trouble that would explain a tail: retransmissions, accept queue
# overflows, backlog drops.
net_snapshot() {
    {
        nstat -asz TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops TcpExtTCPTimeouts |
            awk '!/^#/ { print $1, $2 }'
        local drops=0 dropped
        while read -r _ dropped _; do
            drops=$((drops + 16#$dropped))
        done </proc/net/softnet_stat
        echo "SoftnetDrops $drops"
    } >"$1"
}

net_delta() {
    awk 'FNR == NR { a[$1] = $2; next } { printf "%s\"%s\": %d", (n++ ? ", " : "{"), $1, $2 - a[$1] } END { print (n ? "}" : "{}") }' "$1" "$2"
}

# GET (or, with -X POST among the extra curl arguments, POST) on a mock.
mock_get() {
    local name="$1" path="$2"
    shift 2
    case "$name" in
        mock-plain) curl -sf --max-time 5 "$@" "http://127.0.0.1:$MOCK_PLAIN_PORT$path" ;;
        mock-tls) curl -sf --max-time 5 --cacert "$certs_dir/ca.pem" "$@" "https://127.0.0.1:$MOCK_TLS_PORT$path" ;;
        mock-sc)
            if ((sc_tls)); then
                curl -sf --max-time 5 --cacert "$certs_dir/ca.pem" "$@" "https://127.0.0.1:$SC_PORT$path"
            else
                curl -sf --max-time 5 "$@" "http://127.0.0.1:$SC_PORT$path"
            fi
            ;;
        *) die "unknown mock $name" ;;
    esac
}

mock_reset() {
    local name
    for name in "$@"; do
        mock_get "$name" /__bench/reset -X POST >/dev/null || die "resetting the $name statistics failed"
    done
}

write_manifest() {
    local status="$1" patch=null
    [[ -f "$run_dir/host/sync-diff.patch" ]] && patch='"host/sync-diff.patch"'
    jq -n --arg status "$status" --arg run_id "$RUN_ID" --arg started "$session_started" \
        --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg host "$(hostname)" \
        --arg selfcheck "$selfcheck_status" --arg selfcheck_detail "$selfcheck_detail" \
        --arg host_ready "$host_ready" --argjson invalid "$invalid_runs" \
        --argjson failed "$failed_runs" --argjson failed_compares "$failed_compares" \
        --argjson baseline_ci_failures "$baseline_ci_failures" --argjson retries "$retries" \
        --argjson patch "$patch" \
        --rawfile knobs "$run_dir/host/knobs.env" --rawfile sync "$run_dir/host/sync-info" \
        --rawfile binaries "$run_dir/host/binaries.txt" \
        --slurpfile runs "$run_dir/runs.jsonl" --slurpfile compares "$run_dir/compares.jsonl" \
        'def kv: split("\n") | map(select(test("=")) | capture("^(?<k>[^=]+)=(?<v>.*)$") | {(.k): .v}) | add;
         {run_id: $run_id, status: $status, host: $host, started: $started, finished: $finished,
          host_ready: $host_ready,
          source: (($sync | kv) + {patch: $patch}), knobs: ($knobs | kv),
          binaries: ($binaries | split("\n") | map(select(length > 0) | split("  ")
                     | {sha256: .[0], version: .[1], path: .[2]})),
          cpu_method: "per CPU: busy = 1 - (idle + iowait) / wall clock, softirq as /proc/softirqs events; per process: utime + stime",
          selfcheck: $selfcheck, selfcheck_detail: $selfcheck_detail,
          invalid_runs: $invalid, retries: $retries, failed_runs: $failed,
          failed_compares: $failed_compares, baseline_ci_failures: $baseline_ci_failures,
          gate: [$compares[] | select(.gate_pass != null)
                 | {name, gate_pass, reps_used, within_run_fallback,
                    metrics: [.metrics[] | {metric, gate, pass, baseline_p99_ci_half_width_ratio,
                                            per_pair_delta_us: [.quantiles[] | {q, per_pair_delta_us}]}]}],
          runs: $runs, compares: $compares,
          processes: "processes.jsonl", session_log: "session.log"}' \
        >"$run_dir/manifest.json" 2>/dev/null || log "writing manifest.json failed"
    manifest_written=1
}

# ---------------------------------------------------------------- session

log "session $RUN_ID in $run_dir"
log "core plan: floor $FLOOR_CPUS, mocks $MOCK_CPUS ($MOCK_SHARDS shard(s)), loadgen $LOADGEN_CPUS ($LOADGEN_SHARDS shard(s)), harness $HARNESS_CPUS"
if [[ -f "$repo_root/.sync-info" ]]; then
    cp "$repo_root/.sync-info" "$run_dir/host/sync-info"
    if [[ -f "$repo_root/.sync-diff.patch" ]]; then
        cp "$repo_root/.sync-diff.patch" "$run_dir/host/sync-diff.patch"
    fi
elif git -C "$repo_root" rev-parse HEAD >/dev/null 2>&1; then
    {
        echo "commit=$(git -C "$repo_root" rev-parse HEAD)"
        echo "dirty=$([[ -n "$(git -C "$repo_root" status --porcelain -- .)" ]] && echo true || echo false)"
    } >"$run_dir/host/sync-info"
else
    echo "commit=unknown" >"$run_dir/host/sync-info"
fi

# A sync during the session would replace the binaries under BIN_DIR; the
# session runs these copies, and the recorded hashes are theirs.
for bin in brisk-mock brisk-floor brisk-loadgen; do
    cp -p "$BIN_DIR/$bin" "$run_dir/bin/$bin"
done
mock="$run_dir/bin/brisk-mock"
floor="$run_dir/bin/brisk-floor"
loadgen="$run_dir/bin/brisk-loadgen"
for bin in "$mock" "$floor" "$loadgen"; do
    printf '%s  %s  %s\n' "$(sha256sum "$bin" | cut -d' ' -f1)" "$("$bin" --version)" "$bin"
done >"$run_dir/host/binaries.txt"

host_rc=0
bash "$repo_root/scripts/bench/setup-host.sh" --check >"$run_dir/host/setup-host.txt" 2>&1 || host_rc=$?
case "$host_rc" in
    0) host_ready=true ;;
    3)
        host_ready=false
        grep -F '[ ]' "$run_dir/host/setup-host.txt" || true
        ((ALLOW_UNPREPARED_HOST)) ||
            die "host not prepared (see host/setup-host.txt); run scripts/bench/setup-host.sh --apply, or set ALLOW_UNPREPARED_HOST=1"
        log "WARNING: host not prepared; continuing because ALLOW_UNPREPARED_HOST=1"
        ;;
    *)
        host_ready=error
        ((ALLOW_UNPREPARED_HOST)) ||
            die "setup-host.sh --check failed with status $host_rc; see host/setup-host.txt"
        log "WARNING: setup-host.sh --check failed with status $host_rc; continuing because ALLOW_UNPREPARED_HOST=1"
        ;;
esac
lscpu >"$run_dir/host/lscpu.txt"
uname -a >"$run_dir/host/uname.txt"

# Other services' listeners are sampled alongside the measured processes, so
# contention from them shows in the per-run CPU evidence.
for port in "${reserved_ports[@]}"; do
    pid="$(ss -Hltnp "sport = :$port" | grep -o 'pid=[0-9]*' | head -n 1 | cut -d= -f2)" || pid=""
    if [[ -n "$pid" ]]; then
        foreign_pids["port-$port"]="$pid"
        log "sampling the listener on port $port (pid $pid) as port-$port"
    fi
done

# Planned duration, for the log only. Reruns of invalid repetitions and the
# comparisons at the end come on top.
run_overhead_s=10 # floor start, readiness polls, statistics and stop per run
planned_s=0
planned_detail=""
n_configs=$(wc -w <<<"$CONFIGS")
n_sizes=$(tr ',' ' ' <<<"$S3_SIZES" | wc -w)
n_selfchecks=1
has_config T && n_selfchecks=2
# Adds runs x (warmup + measure + overhead) seconds under a label.
plan() {
    local label="$1" runs="$2" run_s="$3" s
    s=$((runs * (run_s + run_overhead_s)))
    planned_s=$((planned_s + s))
    planned_detail+="${planned_detail:+, }$label $runs run(s) $((s / 60)) min"
}
has_scenario sc && plan sc "$n_selfchecks" $((SC_WARMUP_S + SC_MEASURE_S))
has_scenario s1 && plan s1 $((n_configs * S1_REPS * 2)) $((S1_WARMUP_S + S1_MEASURE_S))
has_scenario s2 && plan s2 $((n_configs * S2_REPS * 2)) $((S2_WARMUP_S + S2_MEASURE_S))
has_scenario s3 && plan s3 $((n_configs * S3_REPS * 2 * n_sizes)) $((S3_WARMUP_S + S3_MEASURE_S))
log "planned session time about $((planned_s / 3600)) h $((planned_s % 3600 / 60)) min (${planned_detail:-nothing to run}), plus reruns and comparisons"
log "repetitions: S1 $S1_REPS, S2 $S2_REPS, S3 $S3_REPS; gate metrics: S1 $GATE_METRICS, S2 $S2_GATE_METRICS, S3 $S3_GATE_METRICS"
for s in s1 s2 s3; do
    reps_var="${s^^}_REPS"
    has_scenario "$s" || continue
    if ((${!reps_var} < 2)); then
        log "WARNING: $reps_var=${!reps_var}; with one repetition the comparison falls back to the within-run bootstrap, which leaves out the run-to-run spread and understates the CI"
    elif ((${!reps_var} < 5)); then
        # Calibration of the paired percentile bootstrap on the pilot data:
        # at 3 pairs the nominal 95% CIs covered about 81% (baseline) and
        # 75-91% (delta), so a narrow baseline CI passes the gate too easily.
        log "WARNING: $reps_var=${!reps_var}; below 5 repetitions the percentile CIs undercover (about 81% for the baseline at 3), so the baseline gate is lenient"
    fi
done
if has_scenario s3 && ((s3_samples < 1000)); then
    log "WARNING: S3 pools $s3_samples requests per arm and size (S3_RATE x S3_MEASURE_S x S3_REPS); its p99 rests on a handful of samples"
fi

certs_dir="$(mktemp -d)"
"$mock" gen-cert --out-dir "$certs_dir" --san 127.0.0.1 localhost >"$run_dir/logs/gen-cert.log" 2>&1 ||
    die "gen-cert failed; see logs/gen-cert.log"
ca="$certs_dir/ca.pem"

# Common loadgen options of one run.
loadgen_common() {
    local label="$1" out="$2" run_seed="$3" cpus="$4" shards="$5" tls="$6" spin="$7"
    lg_args+=(--label "$label" --out "$out" --seed "$run_seed" --shards "$shards"
        --cpu-list "$cpus" --spin-us "$spin")
    if ((tls)); then
        lg_args+=(--tls-ca "$ca")
    fi
}

stream_shape() {
    lg_args+=(--chunk-rate "$S1_CHUNK_RATE" --dur-median "$S1_DUR_MEDIAN"
        --dur-p99 "$S1_DUR_P99" --dur-max "$S1_DUR_MAX" --ttft-us "$S1_TTFT_US"
        --chunk-bytes "$S1_CHUNK_BYTES")
}

# Appends the runs.jsonl record of one result file and clears run_ok unless
# the run is valid.
record_run() {
    local scen="$1" config="$2" arm="$3" rep="$4" attempt="$5" order="$6" size="$7" tag="$8"
    local rc="$9" started="${10}" cpu="${11}" net="${12}" mockstats="${13}" file="${14}"
    if [[ ! -f "$file" ]]; then
        jq -nc --arg scen "$scen" --arg config "$config" --arg arm "$arm" --argjson rep "$rep" \
            --argjson attempt "$attempt" --arg order "$order" \
            --arg tag "$tag" --arg size "$size" --argjson rc "$rc" --arg started "$started" \
            --arg pair "$lg_pair_id" \
            '{scen: $scen, config: $config, arm: $arm, rep: $rep, attempt: $attempt, order: $order,
              tag: $tag, size: $size, pair_id: (if $pair == "" then null else $pair end),
              file: null, valid: false,
              reasons: ["no result file (load generator exit status \($rc))"],
              loadgen_rc: $rc, started: $started}' >>"$run_dir/runs.jsonl"
        invalid_runs=$((invalid_runs + 1))
        run_ok=0
        log "  $tag: NO RESULT"
        return 0
    fi
    jq -c --arg scen "$scen" --arg config "$config" --arg arm "$arm" --argjson rep "$rep" \
        --argjson attempt "$attempt" --arg order "$order" \
        --arg tag "$tag" --arg size "$size" --argjson rc "$rc" --arg started "$started" \
        --arg file "results/${file##*/}" --argjson cpu "$cpu" --argjson net "$net" \
        --argjson mock "$mockstats" --arg pair "$lg_pair_id" '
        .warmup_intervals as $w
        | {scen: $scen, config: $config, arm: $arm, rep: $rep, attempt: $attempt, order: $order,
           tag: $tag, size: $size, pair_id: (if $pair == "" then null else $pair end),
           file: $file, label, scenario, valid: .validity.valid, reasons: .validity.reasons,
           loadgen_rc: $rc, started: $started, seed: .params.common.seed,
           duration_s: .loadgen.duration_s, counters: .loadgen.counters,
           errors: ([.intervals[] | select(.index >= $w) | .errors | to_entries[]]
                    | group_by(.key) | map({key: .[0].key, value: (map(.value) | add)})
                    | from_entries),
           load_check: .loadgen.load_check, selfcheck: .loadgen.selfcheck,
           summary_us: (.summary | map_values({count, p50: (.p50_ns / 1000),
                        p99: (.p99_ns / 1000), p999: (.p999_ns / 1000), max: (.max_ns / 1000)})),
           cpu: $cpu, net: $net, mock_stats: $mock}' "$file" >>"$run_dir/runs.jsonl"
    if jq -e '.validity.valid' "$file" >/dev/null; then
        log "  $tag: VALID"
    else
        invalid_runs=$((invalid_runs + 1))
        run_ok=0
        log "  $tag: INVALID: $(jq -r '.validity.reasons | join("; ")' "$file")"
    fi
}

# Runs the load generator (lg_args) with CPU sampling over its measurement
# window, then records the result file. Sets run_ok to 1 for a valid run
# with no failure of loadgen or floor, else to 0.
execute_run() {
    local scen="$1" config="$2" arm="$3" rep="$4" attempt="$5" order="$6" size="$7" tag="$8"
    local warmup="$9" measure="${10}" result="${11}"
    shift 11
    local -a sampled=("$@")
    local started rc cpu net mockstats floor_died=0
    run_ok=1
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    net_snapshot "$run_dir/logs/$tag.net-a"
    log "run $tag: ${lg_args[*]}"
    taskset -c "$lg_cpus" "$loadgen" "${lg_args[@]}" >"$run_dir/logs/$tag.out" 2>"$run_dir/logs/$tag.err" &
    pids[loadgen]=$!
    local window=$((measure > 2 ? measure - 2 : 1))
    sample_window "$tag" "$((warmup + 1))" "$window" "${sampled[@]}" &
    sampler_pid=$!
    if wait "${pids[loadgen]}"; then rc=0; else rc=$?; fi
    unset "pids[loadgen]"
    if kill -0 "$sampler_pid" 2>/dev/null; then
        kill "$sampler_pid" 2>/dev/null || true
    fi
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
    if [[ -f "$run_dir/logs/$tag.cpu-a" && -f "$run_dir/logs/$tag.cpu-b" ]]; then
        cpu="$(cpu_usage "$run_dir/logs/$tag.cpu-a" "$run_dir/logs/$tag.cpu-b")"
    else
        cpu=null
    fi
    net_snapshot "$run_dir/logs/$tag.net-b"
    net="$(net_delta "$run_dir/logs/$tag.net-a" "$run_dir/logs/$tag.net-b")"
    mockstats="{}"
    local name stats
    for name in mock-plain mock-tls mock-sc; do
        alive "$name" || continue
        stats="$(mock_get "$name" /__bench/stats)" || die "reading the $name statistics failed"
        mockstats="$(jq -c --arg n "$name" --argjson s "$stats" '. + {($n): $s}' <<<"$mockstats")"
    done
    if [[ "$arm" == floor ]]; then
        alive floor || floor_died=1
        stop_proc floor
    fi
    sed -n '/^validity/,$p' "$run_dir/logs/$tag.out" | sed 's/^/    /'
    # selfcheck exits non-zero for a FAIL verdict, which run_selfcheck
    # reports; only a missing result makes it a broken run.
    if ((rc != 0)) && [[ "$scen" != sc || ! -f "$result" ]]; then
        failed_runs=$((failed_runs + 1))
        log "  $tag: brisk-loadgen exited with status $rc; see logs/$tag.err"
    fi
    if ((floor_died)); then
        failed_runs=$((failed_runs + 1))
        log "  $tag: brisk-floor exited during the run; see logs/$tag.floor.log"
    fi
    local ok=1
    if ((rc != 0)) && [[ "$scen" != sc ]]; then
        ok=0
    fi
    ((!floor_died)) || ok=0
    record_run "$scen" "$config" "$arm" "$rep" "$attempt" "$order" "$size" "$tag" "$rc" "$started" \
        "${cpu:-null}" "$net" "$mockstats" "$result"
    ((ok)) || run_ok=0
}

floor_port() {
    if [[ "$1" == T ]]; then echo "$FLOOR_T_PORT"; else echo "$FLOOR_P_PORT"; fi
}

start_floor() {
    local config="$1" tag="$2"
    local -a args=(--listen "127.0.0.1:$(floor_port "$config")"
        --upstream "https://127.0.0.1:$MOCK_TLS_PORT" --upstream-ca "$ca" --cpu-list "$FLOOR_CPUS")
    [[ -n "$FLOOR_WORKERS" ]] && args+=(--workers "$FLOOR_WORKERS")
    [[ "$config" == T ]] && args+=(--tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key")
    start_proc floor "$FLOOR_CPUS" "$run_dir/logs/$tag.floor.log" "$floor" "${args[@]}"
    if [[ "$config" == T ]]; then
        wait_ready floor "https://127.0.0.1:$FLOOR_T_PORT/v1/models" --cacert "$ca"
    else
        wait_ready floor "http://127.0.0.1:$FLOOR_P_PORT/v1/models"
    fi
    fingerprint_proc floor "$tag"
}

target_url() {
    local config="$1" arm="$2" scheme=http port
    [[ "$config" == T ]] && scheme=https
    if [[ "$arm" == floor ]]; then
        port="$(floor_port "$config")"
    elif [[ "$config" == T ]]; then
        port="$MOCK_TLS_PORT"
    else
        port="$MOCK_PLAIN_PORT"
    fi
    echo "$scheme://127.0.0.1:$port"
}

# One arm of one repetition (of one S3 size); sets run_ok.
run_arm() {
    local scen="$1" config="$2" arm="$3" rep="$4" attempt="$5" order="$6" size="$7"
    local base="$scen-$config-$arm-r$rep" tls=0 rep_seed=$((SEED + rep - 1)) spin="$SPIN_US"
    # Shared by both arms and by every attempt of the repetition: the
    # comparison pairs the arms' runs by it.
    lg_pair_id="$scen-$config${size:+-$size}-r$rep"
    ((attempt == 0)) || base+="-a$attempt"
    local tag="$base${size:+-$size}"
    [[ "$config" == T ]] && tls=1
    [[ "$arm" == floor ]] && start_floor "$config" "$tag"
    mock_reset mock-plain mock-tls
    lg_out="$run_dir/results/$base.json"
    lg_cpus="$LOADGEN_CPUS"
    local url warmup measure result="$lg_out"
    url="$(target_url "$config" "$arm")"
    case "$scen" in
        s1)
            lg_args=(stream "$url" --concurrency "$S1_CONCURRENCY")
            stream_shape
            lg_args+=(--warmup-s "$S1_WARMUP_S" --measure-s "$S1_MEASURE_S")
            warmup="$S1_WARMUP_S" measure="$S1_MEASURE_S"
            ;;
        s2)
            lg_args=(nonstream "$url" --rate "$S2_RATE" --ttft-us "$S2_TTFT_US"
                --resp-bytes "$S2_RESP_BYTES" --prompt-bytes "$S2_PROMPT_BYTES"
                --warmup-s "$S2_WARMUP_S" --measure-s "$S2_MEASURE_S")
            warmup="$S2_WARMUP_S" measure="$S2_MEASURE_S"
            ;;
        s3)
            # One size per loadgen run, so the CPU window, network delta and
            # mock statistics each belong to that size alone.
            lg_args=(bigbody "$url" --sizes "$size" --rate "$S3_RATE" --ttft-us "$S3_TTFT_US"
                --chunks "$S3_CHUNKS" --warmup-s "$S3_WARMUP_S" --measure-s "$S3_MEASURE_S")
            warmup="$S3_WARMUP_S" measure="$S3_MEASURE_S" spin="$S3_SPIN_US"
            result="${lg_out%.json}-$size.json"
            ;;
    esac
    loadgen_common "$arm-$config" "$lg_out" "$rep_seed" "$LOADGEN_CPUS" "$LOADGEN_SHARDS" "$tls" "$spin"
    lg_args+=(--pair-id "$lg_pair_id")
    execute_run "$scen" "$config" "$arm" "$rep" "$attempt" "$order" "$size" "$tag" "$warmup" "$measure" \
        "$result" mock-plain mock-tls floor loadgen
}

# Both arms of one repetition, in alternating order; a repetition with an
# invalid or failed run is rerun as a pair with the same seed.
run_rep() {
    local scen="$1" config="$2" size="$3" rep="$4" reps="$5"
    local attempt=0 first=direct second=floor pair_ok
    if ((rep % 2 == 0)); then
        first=floor second=direct
    fi
    while true; do
        log "== config $config, $scen${size:+ $size}, repetition $rep of $reps$( ((attempt == 0)) || echo ", rerun $attempt")"
        pair_ok=1
        run_arm "$scen" "$config" "$first" "$rep" "$attempt" first "$size"
        ((run_ok)) || pair_ok=0
        run_arm "$scen" "$config" "$second" "$rep" "$attempt" second "$size"
        ((run_ok)) || pair_ok=0
        if ((pair_ok)); then
            return 0
        fi
        if ((attempt >= RETRY_INVALID)); then
            log "  repetition $rep still has an invalid run after $attempt rerun(s); comparisons will leave it out"
            return 0
        fi
        attempt=$((attempt + 1))
        retries=$((retries + 1))
        log "  rerunning repetition $rep, both arms, same seed"
    done
}

# One selfcheck against a mock with the given TLS setting; prints PASS or
# FAIL.
selfcheck_one() {
    local tls="$1" config=P scheme=http tag=selfcheck
    local -a mock_tls=() curl_ca=()
    if ((tls)); then
        config=T scheme=https tag=selfcheck-T
        mock_tls=(--tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key")
        curl_ca=(--cacert "$ca")
    fi
    sc_tls="$tls"
    lg_pair_id=""
    log "selfcheck $config: $SC_CONCURRENCY x 2 streams, mock $SC_MOCK_SHARDS shard(s) on CPUs $SC_MOCK_CPUS, loadgen $SC_LOADGEN_SHARDS shard(s) on $SC_LOADGEN_CPUS"
    start_proc mock-sc "$SC_MOCK_CPUS" "$run_dir/logs/$tag.mock.log" "$mock" serve \
        --listen "127.0.0.1:$SC_PORT" --shards "$SC_MOCK_SHARDS" --cpu-list "$SC_MOCK_CPUS" --spin-us "$SPIN_US" \
        "${mock_tls[@]}"
    wait_ready mock-sc "$scheme://127.0.0.1:$SC_PORT/v1/models" "${curl_ca[@]}"
    fingerprint_proc mock-sc "$tag"
    mock_reset mock-sc
    lg_out="$run_dir/results/$tag.json"
    lg_cpus="$SC_LOADGEN_CPUS"
    lg_args=(selfcheck "$scheme://127.0.0.1:$SC_PORT" --concurrency "$SC_CONCURRENCY")
    stream_shape
    lg_args+=(--warmup-s "$SC_WARMUP_S" --measure-s "$SC_MEASURE_S")
    loadgen_common "selfcheck-$config" "$lg_out" "$SEED" "$SC_LOADGEN_CPUS" "$SC_LOADGEN_SHARDS" "$tls" "$SPIN_US"
    execute_run sc "$config" direct 1 0 first "" "$tag" "$SC_WARMUP_S" "$SC_MEASURE_S" "$lg_out" mock-sc loadgen
    stop_proc mock-sc
    sc_tls=0
    if [[ -f "$lg_out" ]] && jq -e '.loadgen.selfcheck.pass' "$lg_out" >/dev/null; then
        sc_result=PASS
    else
        sc_result=FAIL
    fi
    log "selfcheck $config: $sc_result"
}

run_selfcheck() {
    selfcheck_status=PASS
    selfcheck_one 0
    selfcheck_detail="P: $sc_result"
    [[ "$sc_result" == PASS ]] || selfcheck_status=FAIL
    if has_config T; then
        selfcheck_one 1
        selfcheck_detail+=", T: $sc_result"
        [[ "$sc_result" == PASS ]] || selfcheck_status=FAIL
    fi
    log "selfcheck: $selfcheck_status ($selfcheck_detail)"
}

# jq helpers for reading a compare document. pair_deltas collects the
# per-pair deltas (ns) of quantile $i of comparison $c, from either
# comparison.per_pair[] (each pair with quantiles[].delta_ns, or deltas_ns
# in the order of comparison.quantiles) or comparison.quantiles[$i].per_pair
# (numbers or objects with delta_ns).
# shellcheck disable=SC2016
jq_compare_defs='
def signed: if . == null then "n/a"
            else (. * 100 | round / 100) as $v | (if $v >= 0 then "+" else "" end) + ($v | tostring)
            end;
def us: if . == null then "n/a" else . / 1000 | signed end;
def pct: if . == null then "n/a" else (. * 1000 | round / 10 | tostring) + "%" end;
def qlabel: "p" + (. * 100 * 1e6 | round / 1e6 | tostring);
def median: sort | length as $n
    | if $n == 0 then null
      elif $n % 2 == 1 then .[($n - 1) / 2]
      else (.[$n / 2 - 1] + .[$n / 2]) / 2
      end;
def pair_deltas($c; $i):
    $c.quantiles[$i] as $q
    | [if ($q.per_pair | type) == "array" then
           $q.per_pair[] | if type == "object" then .delta_ns else . end
       elif ($c.per_pair | type) == "array" then
           $c.per_pair[]
           | if (.quantiles | type) == "array" then .quantiles[] | select(.quantile == $q.quantile) | .delta_ns
             elif (.deltas_ns | type) == "array" then .deltas_ns[$i]
             else .delta_ns
             end
       else empty
       end
       | numbers];
def pair_summary($c; $i):
    pair_deltas($c; $i) as $d
    | if ($d | length) == 0 then null
      else {pairs: ($d | length), min_us: ($d | min / 1000), median_us: ($d | median / 1000),
            max_us: ($d | max / 1000)}
      end;
'

compare_one() {
    local scen="$1" config="$2" size="$3" metrics="$4" quantiles="$5"
    local name="$scen-$config${size:+-$size}" reps_var="${scen^^}_REPS"
    local expected="${!reps_var}" gated
    gated="$(gate_metrics "$scen")"
    # Per repetition, the latest attempt whose two arms are both valid (the
    # compared pairs) and, for diagnosis, the latest attempt whose two arms
    # both left a result file and that holds an invalid run, else the latest
    # such attempt: taking the latest alone would pick the valid rerun of an
    # invalid repetition and reduce the diagnostic comparison to the formal
    # one. A repetition without one is left out on both arms, so the A and B
    # lists stay aligned repetition by repetition, which is the order in
    # which compare pairs them.
    local selection
    selection="$(jq -sc --arg s "$scen" --arg c "$config" --arg z "$size" '
        def rep_pairs(ok; rank):
            group_by(.rep)
            | map(group_by(.attempt)
                  | map(select(length == 2
                               and all(.[]; ok and .file != null)
                               and (map(.arm) | sort) == ["direct", "floor"]))
                  | sort_by(rank)
                  | last)
            | map(select(. != null));
        [.[] | select(.scen == $s and .config == $c and .size == $z)] as $all
        | ($all | rep_pairs(.valid == true; .[0].attempt)) as $pairs
        | ($all | rep_pairs(true; [any(.[]; .valid != true), .[0].attempt])) as $with_files
        | {reps: ($pairs | length),
           pair_ids: [$pairs[] | .[0].pair_id],
           a: [$pairs[][] | select(.arm == "direct") | .file],
           b: [$pairs[][] | select(.arm == "floor") | .file],
           has_invalid: any($all[]; .valid != true),
           diag_has_invalid: any($with_files[][]; .valid != true),
           all_a: [$with_files[][] | select(.arm == "direct") | .file],
           all_b: [$with_files[][] | select(.arm == "floor") | .file]}' \
        "$run_dir/runs.jsonl")"
    local reps has_invalid diag_has_invalid
    reps="$(jq -r '.reps' <<<"$selection")"
    has_invalid="$(jq -r '.has_invalid' <<<"$selection")"
    diag_has_invalid="$(jq -r '.diag_has_invalid' <<<"$selection")"
    local -a a_files b_files
    mapfile -t a_files < <(jq -r '.a[]' <<<"$selection")
    mapfile -t b_files < <(jq -r '.b[]' <<<"$selection")

    # Numbers that include invalid runs are kept apart, for diagnosis only.
    local diagnostic=null
    if [[ "$has_invalid" == true && "$diag_has_invalid" != true ]]; then
        log "compare $name: no diagnostic comparison; no invalid run left a result file in an attempt with both arms"
    elif [[ "$has_invalid" == true ]]; then
        local -a all_a all_b
        mapfile -t all_a < <(jq -r '.all_a[]' <<<"$selection")
        mapfile -t all_b < <(jq -r '.all_b[]' <<<"$selection")
        if ((${#all_a[@]} > 0 && ${#all_b[@]} > 0)); then
            if "$loadgen" compare --a "${all_a[@]/#/$run_dir/}" --b "${all_b[@]/#/$run_dir/}" \
                --metric "$metrics" --gate-metrics "$gated" --quantiles "$quantiles" \
                --resamples "$COMPARE_RESAMPLES" \
                --allow-invalid --out "$run_dir/compare/$name.with-invalid.json" \
                >"$run_dir/compare/$name.with-invalid.txt" 2>&1; then
                diagnostic="\"compare/$name.with-invalid.json\""
                log "compare $name: diagnostic comparison including invalid runs in compare/$name.with-invalid.txt"
            else
                log "compare $name: the diagnostic comparison including invalid runs failed; see compare/$name.with-invalid.txt"
            fi
        fi
    fi

    if ((reps == 0)); then
        log "compare $name: skipped, no repetition has two valid arms"
        failed_compares=$((failed_compares + 1))
        jq -nc --arg name "$name" --argjson expected "$expected" --argjson diag "$diagnostic" \
            '{name: $name, status: "skipped", reason: "no repetition has two valid arms",
              reps_used: 0, reps_expected: $expected, diagnostic: $diag}' >>"$run_dir/compares.jsonl"
        return 0
    fi
    local status="done"
    if ((reps < expected)); then
        status=partial
        failed_compares=$((failed_compares + 1))
        log "compare $name: only $reps of $expected repetitions have two valid arms"
    fi
    if ((reps == 1)); then
        log "compare $name: one repetition only; the CIs come from the within-run bootstrap and leave out the run-to-run spread"
    fi
    local json="$run_dir/compare/$name.json"
    local -a args=(compare --a "${a_files[@]/#/$run_dir/}" --b "${b_files[@]/#/$run_dir/}"
        --metric "$metrics" --gate-metrics "$gated" --quantiles "$quantiles"
        --resamples "$COMPARE_RESAMPLES" --out "$json")
    local rc=0 reason=""
    "$loadgen" "${args[@]}" >"$run_dir/compare/$name.txt" 2>&1 || rc=$?
    sed 's/^/    /' "$run_dir/compare/$name.txt"
    if ((rc != 0)) || [[ ! -f "$json" ]]; then
        reason="brisk-loadgen compare exited with status $rc"
    elif ! jq -e '(.gate_pass | type) == "boolean"
                  and all(.comparisons[]; (.gate | type) == "boolean" and (.pass | type) == "boolean")' \
        "$json" >/dev/null; then
        reason="compare/$name.json lacks gate_pass or a per-metric gate or pass field"
    elif ! jq -e --arg g "$gated" \
        '([.comparisons[] | select(.gate) | .metric] | unique) == ($g | split(",") | unique)' \
        "$json" >/dev/null; then
        reason="compare/$name.json gates $(jq -r '[.comparisons[] | select(.gate) | .metric] | join(",")' "$json"), expected $gated"
    fi
    if [[ -n "$reason" ]]; then
        log "compare $name: FAILED: $reason"
        # A partial comparison is already counted.
        [[ "$status" == partial ]] || failed_compares=$((failed_compares + 1))
        jq -nc --arg name "$name" --arg reason "$reason" --argjson diag "$diagnostic" \
            '{name: $name, status: "failed", reason: $reason, diagnostic: $diag}' \
            >>"$run_dir/compares.jsonl"
        return 0
    fi

    local gate_pass
    gate_pass="$(jq -r '.gate_pass' "$json")"
    [[ "$gate_pass" == true ]] || baseline_ci_failures=$((baseline_ci_failures + 1))
    log "compare $name: gate $([[ "$gate_pass" == true ]] && echo PASS || echo FAIL) (gate metrics $gated, $reps pair(s))"
    # Per metric the baseline criterion, then per quantile the delta and the
    # spread of the per-pair deltas, which shows the run-to-run variation
    # the CI has to cover.
    jq -r "$jq_compare_defs"'
        .comparisons[] as $c
        | "  \($c.metric): baseline p99 CI half width \($c.baseline_p99_ci_half_width_ratio | pct) (limit 5%): \(if $c.pass then "PASS" else "FAIL" end)\(if $c.gate then "" else " (reported, not gated)" end)",
          (range(0; $c.quantiles | length) as $i
           | $c.quantiles[$i] as $q
           | pair_summary($c; $i) as $p
           | "    \($q.quantile | qlabel): delta \($q.delta_ns | us) us, 95% CI [\($q.delta_ci_low_ns | us), \($q.delta_ci_high_ns | us)] us; per pair "
             + (if $p == null then "n/a"
                else "\($p.min_us | signed) .. \($p.max_us | signed) us, median \($p.median_us | signed) us over \($p.pairs) pair(s)"
                end))' \
        "$json" | while IFS= read -r line; do log "$line"; done ||
        log "compare $name: printing the per-metric summary failed; see compare/$name.json"

    local ci_fail
    ci_fail="$(jq -c '[.comparisons[] | select(.gate and (.pass | not))
                       | {metric, ratio: .baseline_p99_ci_half_width_ratio}]' "$json")"
    jq -c --arg name "$name" --arg status "$status" --arg scen "$scen" --arg config "$config" \
        --arg size "$size" --argjson reps "$reps" --argjson expected "$expected" \
        --argjson ci_fail "$ci_fail" --argjson diag "$diagnostic" --arg gated "$gated" \
        --argjson pair_ids "$(jq -c '.pair_ids' <<<"$selection")" "$jq_compare_defs"'
        {name: $name, status: $status, scen: $scen, config: $config, size: $size,
         json: "compare/\($name).json", text: "compare/\($name).txt",
         reps_used: $reps, reps_expected: $expected, diagnostic: $diag,
         pair_ids: $pair_ids, within_run_fallback: ($reps == 1),
         invalid_runs, baseline_p99_ci_ok, gate_pass,
         gated_metrics: ($gated | split(",")), baseline_ci_fail: $ci_fail,
         compare_meta: del(.comparisons, .a_files, .b_files, .a_labels, .b_labels,
                           .invalid_runs, .baseline_p99_ci_ok, .gate_pass),
         metrics: [.comparisons[] as $c
                   | $c | {metric, gate, pass, a_intervals, b_intervals, a_count, b_count,
                           baseline_p99_ci_half_width_ratio, per_pair,
                           quantiles: [range(0; $c.quantiles | length) as $i | $c.quantiles[$i]
                                       | {q: .quantile, a_us: (.a_ns / 1000), b_us: (.b_ns / 1000),
                                          delta_us: (.delta_ns / 1000),
                                          delta_ci_us: [(.delta_ci_low_ns / 1000), (.delta_ci_high_ns / 1000)],
                                          a_ci_half_width_ratio,
                                          per_pair_delta_us: pair_summary($c; $i)}]}]}' \
        "$json" >>"$run_dir/compares.jsonl"
}

if has_scenario sc; then
    run_selfcheck
    if [[ "$selfcheck_status" == FAIL ]] && ((SC_GATE)); then
        log "selfcheck failed; the tools lack headroom on this core plan, so no scenario is run (SC_GATE=0 overrides)"
        write_manifest selfcheck_failed
        exit 1
    fi
fi

scenario_list=()
for s in s1 s2 s3; do
    has_scenario "$s" && scenario_list+=("$s")
done
mapfile -t s3_sizes < <(tr ',' '\n' <<<"${S3_SIZES,,}" | sed '/^$/d')

if ((${#scenario_list[@]} > 0)); then
    start_proc mock-plain "$MOCK_CPUS" "$run_dir/logs/mock-plain.log" "$mock" serve \
        --listen "127.0.0.1:$MOCK_PLAIN_PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS" --spin-us "$SPIN_US"
    start_proc mock-tls "$MOCK_CPUS" "$run_dir/logs/mock-tls.log" "$mock" serve \
        --listen "127.0.0.1:$MOCK_TLS_PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS" --spin-us "$SPIN_US" \
        --tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key"
    wait_ready mock-plain "http://127.0.0.1:$MOCK_PLAIN_PORT/v1/models"
    wait_ready mock-tls "https://127.0.0.1:$MOCK_TLS_PORT/v1/models" --cacert "$ca"
    fingerprint_proc mock-plain session
    fingerprint_proc mock-tls session

    for config in $CONFIGS; do
        for scen in "${scenario_list[@]}"; do
            reps_var="${scen^^}_REPS"
            sizes=("")
            [[ "$scen" == s3 ]] && sizes=("${s3_sizes[@]}")
            for size in "${sizes[@]}"; do
                for ((rep = 1; rep <= ${!reps_var}; rep++)); do
                    run_rep "$scen" "$config" "$size" "$rep" "${!reps_var}"
                done
            done
        done
    done
    stop_proc mock-plain
    stop_proc mock-tls

    log "== comparisons (A = direct, B = floor)"
    for config in $CONFIGS; do
        for scen in "${scenario_list[@]}"; do
            case "$scen" in
                s1) compare_one s1 "$config" "" "$(compared_metrics s1)" "$COMPARE_QUANTILES" ;;
                s2) compare_one s2 "$config" "" "$(compared_metrics s2)" "$COMPARE_QUANTILES" ;;
                s3)
                    for size in "${s3_sizes[@]}"; do
                        compare_one s3 "$config" "$size" "$(compared_metrics s3)" "$S3_COMPARE_QUANTILES"
                    done
                    ;;
            esac
        done
    done
fi

write_manifest complete
log "session $RUN_ID done: selfcheck $selfcheck_status, $invalid_runs invalid run(s) ($retries repetition rerun(s)), $failed_runs failed run(s), $failed_compares missing or partial comparison(s), $baseline_ci_failures comparison(s) failing the baseline CI gate"
# One line per compared metric, so the verdicts sit together at the end of
# the log.
jq -rs '.[] | select(.gate_pass != null) | .name as $n
        | .metrics[] | "  \($n) \(.metric): \(if .pass then "PASS" else "FAIL" end)\(if .gate then "" else " (not gated)" end)"' \
    "$run_dir/compares.jsonl" | while IFS= read -r line; do log "$line"; done ||
    log "printing the per-metric verdicts failed; see compares.jsonl"
log "results in $run_dir"
if [[ "$selfcheck_status" == FAIL ]] || ((failed_runs + failed_compares + baseline_ci_failures > 0)); then
    exit 1
fi
