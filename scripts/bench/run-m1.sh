#!/usr/bin/env bash
# Knobs are assigned indirectly by the knob table.
# shellcheck disable=SC2153
# M1 measurement sessions on the Linux benchmark host (contract 05, sections
# 6.1 to 6.3 and 7.5, with the M0 changes of contract 03, section 9). A
# session runs its arms repetition by repetition under configs P, T and N,
# compares them and judges the comparisons:
#
#   session  arms (baseline first)                scenarios          configs
#   gate     direct, floor-A, Brisk               sc s1 s2 ramp s3   P T N
#   e11      direct, floor-A, floor-B             sc s1 s2           P T N
#   e1       Brisk (router axum), Brisk (match)   sc s2 ramp s1      N
#   e2       Brisk (splice segments), (concat)    sc s3 s1           N
#   e4       Brisk (commit_hold 0s), (2s)         sc s1              N P
#
# Usage: scripts/bench/run-m1.sh --session <name> [--<knob> <value> | --<knob>=<value>]...
#        scripts/bench/run-m1.sh --session <name> --print-config [--<knob> <value>]...
#        scripts/bench/run-m1.sh --help
#
# Every knob is an environment variable, and a flag overrides it:
# `--s1-reps 7` sets S1_REPS=7; --help lists the knobs. A session takes many
# hours, gate about a day: run it detached (tmux, or systemd-run --user
# --scope), so a dropped ssh connection neither ends it nor sends terminal
# output over the network during the measurement.
#
#   config P  direct: loadgen -http->  mock   others: loadgen -http->  SUT -https-> mock
#   config T  direct: loadgen -https-> mock   others: loadgen -https-> SUT -https-> mock
#   config N  direct: loadgen -http->  mock   others: loadgen -http->  SUT -http->  mock
#
# The direct ramp that gives a ramp block its tool limit (see Ramp) reaches
# the mock the block's SUT arms reach, so in config P it runs loadgen -https->
# mock (arm direct-tls) rather than the plaintext direct path. It carries
# loadgen's TLS cost, which the SUT arms of P do not, so that limit errs low:
# it can turn a gated throughput comparison into a reported one, but never
# lets one pass that the TLS mock capped.
#
# The SUT (process under test) is brisk-floor --mode a (floor-A) or --mode b
# (floor-B), or brisk serve with a configuration rendered from
# brisk-bench.toml.in. Core plan (LAYOUT=8vcpu of run-m0.sh): the SUT on CPUs
# 4-7 with 4 workers (floor through --cpu-list and --workers, Brisk through
# taskset and server.workers; both confine every thread to the set without
# binding one thread per core), the mocks on 2-3 (2 shards each), loadgen on
# 0-1 (2 shards), this script on CPU 0.
#
# Scenarios: sc is the selfcheck of run-m0.sh; s1 1000 SSE streams at 30
# chunks/s; s2 non-streaming requests at a fixed 2000/s; ramp the maximum
# sustainable S2 throughput (below); s3 large request bodies, one block per
# size. In e4, S1 runs once per mock TTFT of E4_TTFTS_US. A block is one
# scenario under one config (and size or TTFT); its repetitions default to
# S1 5, S2 3, S3 3, ramp 3 pairs.
#
# Pairing: within a repetition the arms rotate as a Latin square (three arms:
# direct, floor-A, Brisk; floor-A, Brisk, direct; Brisk, direct, floor-A; two
# arms alternate), and all arms of a repetition share its seed and pairing id
# (<scen>-<config>[-<var>]-r<N>). The same repetitions thus give every
# comparison of the session: in gate Brisk against direct and against floor-A
# (and floor-A against direct), in e11 floor-A and floor-B against direct and
# floor-B against floor-A, in e1, e2 and e4 the second setting against the
# first. A repetition with an invalid run is rerun, all arms with the same
# seed and order, up to RETRY_INVALID times; comparisons use the repetitions
# whose arms are all valid. In the gate configs of PT_CONFIGS, S1 also runs
# Brisk with stream_usage = "passthrough" (brisk-pt), right after Brisk in odd
# repetitions and right before it in even ones, which leaves the three-arm
# square as it is; Brisk against brisk-pt is the cost of strip, reported only.
# brisk-pt stays out of the gated repetitions: once the gated arms of a
# repetition are valid, an invalid brisk-pt (or Brisk next to it) reruns only
# Brisk and brisk-pt, at their positions; the gated comparisons and the reuse
# verdict never read brisk-pt, and Brisk against brisk-pt uses the latest
# attempt of each repetition in which the two are valid together.
#
# Besides loadgen's validity, a run of S1, S2 or S3 is invalid when more of
# its requests failed after the warmup (stale retries aside) than a tenth of
# the tail above its highest compared quantile (COMPARE_QUANTILES,
# S3_COMPARE_QUANTILES; 0.01% at p99.9): brisk-loadgen compare refuses such
# a run, which would cost the block its comparisons, while loadgen's validity
# tolerates 0.1%.
#
# Brisk: at the start of the session `brisk keygen` makes a virtual key, read
# by its frozen output format (contract 05, section 1.5). Its digest goes into
# the rendered configurations (one per Brisk arm and config, validated with
# `brisk check-config` before the first run); the key reaches brisk-loadgen as
# --header "Authorization: Bearer ...", in every arm, so that all arms send
# the same bytes. The key stays out of every log and result: loadgen writes
# into a private staging directory, the header value is replaced before a
# result moves to results/, and the session fails if the key shows up under
# the session directory (it is visible in the process list of the host while
# a load generator runs). The upstream channel key comes from the
# environment; brisk-mock does not check it. Brisk is ready once GET /readyz
# returns 200, floor once GET /v1/models through it succeeds. The gated path
# is the product default: stream_usage = "inject" and loadgen without
# --include-usage, so Brisk injects and strips; S3 never asks for usage.
#
# Evidence per run, besides that of run-m0.sh (CPU per core, steal, network,
# mock shard balance):
# - the mock statistics are reset (POST /__bench/reset) at the start of the
#   measurement window, and the accepts and requests of the arm's mock at the
#   end give the upstream connection reuse 1 - accepts / requests (the
#   statistics request's own connection left out; Brisk's periodic warmup
#   requests count as requests); an S1, S2 or S3 run below MIN_REUSE is
#   invalid, a ramp's reuse is only recorded, since its load keeps rising;
# - the SUT's utime + stime over the window (/proc/<pid>/stat, CLK_TCK),
#   divided by the chunks (S1) or completed requests (S2, S3) loadgen
#   received in that window, its one-second intervals weighted by their
#   overlap with the window;
# - in S1, the SUT's VmRSS after IDLE_RSS_S idle seconds past readiness,
#   before the first request, and at the middle of the window; memory per 1k
#   streams = (middle - idle) / (S1_CONCURRENCY / 1000);
# - after the S2 repetitions of a config, one more S2 run per SUT arm with
#   perf stat counting HITM-related events on the SUT (PERF), or the reason
#   it could not.
#
# Ramp (contract 05, section 6.1): nonstream from RAMP_START req/s, up
# RAMP_STEP_PCT percent every RAMP_STEP_S seconds for at most RAMP_MAX_STEPS
# steps, stopping at the first step whose p99 exceeds RAMP_STOP_P99_MS, mock
# TTFT RAMP_TTFT_US. A ramp's sustainable rate is the last step of its
# leading run of steps with p99 within the limit and no error, in a valid
# run. Each ramp block starts with one direct ramp, the tool limit of mock and
# loadgen; when the baseline arm (floor-A, axum in e1) reaches
# RAMP_TOOL_LIMIT_SHARE of it, the throughput comparison is tool-limited and
# only reported. The final step of every ramp saturates something, in the
# direct ramp the mock itself, so ramps take their own mock write lag limit
# (RAMP_MOCK_WRITE_LAG_LIMIT_US).
#
# A ramp step is sustainable (6.1) when its p99 is within the limit, it had
# no error and loadgen's validity checks hold over the step's own intervals.
# loadgen checks validity once over the whole ramp, saturating last step
# included, so its emit lag, mock write lag, failure and stale retry rules
# are applied again per step from the intervals of the result, and only a
# reason no step can own (a clock step, say) voids a ramp. The saturation
# that ends a ramp thus ends its run of sustainable steps instead of voiding
# it; in the direct ramp, loadgen falling behind marks the tool limit.
#
# Verdicts (contract 05, 7.5 and 6.3) on the 95% t interval of each paired
# delta: FAIL when its low end exceeds the limit, PASS when the estimate and
# its high end are within it, else UNCERTAIN; one repetition is never PASS.
# They are gated in gate's config P and reported in T and N; a TTFT p99 check
# whose interval half width exceeds a quarter of its limit is reported only.
# While a gated verdict of a block is UNCERTAIN, the block gets one more
# repetition at a time, up to EXTEND_MAX_REPS (EXTEND_UNCERTAIN). The ramp
# ratio Brisk / floor-A over at least 3 pairs: all >= 0.85 PASS, mean < 0.85
# FAIL, else UNCERTAIN; a ramp without a sustainable step lies below
# RAMP_START, which bounds its pair's ratio, and a tool-limited block is
# only reported. The reuse limit (>= 99.9%) must hold in every run of the
# Brisk arms, the voided and rerun ones included. The e-sessions print their
# rules as MET or NOT_MET (TRIGGERED or not in e11) and a decision; a rule
# on the within-run interval of a single repetition is UNDECIDED (UNCERTAIN
# in e11) and changes no default. Memory, idle RSS, CPU per chunk
# and per request, chunk_wire, the strip cost and the HITM counts are
# reported only, with the contract's reference values where it names them.
#
# The session refuses to start unless setup-host.sh --check passes
# (ALLOW_UNPREPARED_HOST=1 overrides), holds $RESULTS_ROOT/.run-m0.lock for
# its whole life (the lock of run-m0.sh, which sync.sh checks, so M0 and M1
# sessions exclude each other) and runs its own copies of the binaries, taken
# from BIN_DIR at the start. sync.sh does not build brisk; build it into the
# same target directory (CARGO_TARGET_DIR=~/brisk-target cargo build --locked
# --release -p brisk).
#
# Output in $RESULTS_ROOT/$RUN_ID/: manifest.json, summary.txt, runs.jsonl,
# compares.jsonl and verdicts.jsonl (both append-only: the last record of a
# name or id counts), processes.jsonl, session.log, results/ (RunResult
# files), compare/ (JSON and text of every comparison), brisk/ (rendered
# configurations), logs/, host/, bin/. Every process started here is stopped
# on exit, failures included. Exit status: 0 when every gated verdict is PASS
# and nothing failed; 1 when the selfcheck, a run, a comparison or an
# evaluation failed, or a gated verdict is FAIL or MISSING; 3 when a gated
# verdict is still UNCERTAIN.

set -euo pipefail

usage() {
    # The leading comment block after the shellcheck directive.
    awk 'NR < 4 { next } /^#/ { print; next } { exit }' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
    echo "Knobs (default, meaning):"
    local entry name default meaning
    for entry in "${knob_table[@]}"; do
        IFS='|' read -r name default meaning <<<"$entry"
        printf '  %-29s %-24s %s\n' "$name" "${default:-(none)}" "$meaning"
    done
}

# ---------------------------------------------------------------- sessions

# Arms of each session, the baseline of its decision first.
declare -A session_arms=(
    [gate]="direct floor-a brisk"
    [e11]="direct floor-a floor-b"
    [e1]="brisk-axum brisk-match"
    [e2]="brisk-segments brisk-concat"
    [e4]="brisk-hold0s brisk-hold2s"
)
declare -A session_configs=([gate]="P T N" [e11]="P T N" [e1]="N" [e2]="N" [e4]="N P")
declare -A session_scenarios=(
    [gate]="sc s1 s2 ramp s3"
    [e11]="sc s1 s2"
    [e1]="sc s2 ramp s1"
    [e2]="sc s3 s1"
    [e4]="sc s1"
)
# Comparisons as A:B, whose delta is B - A. A comparison runs in every block
# that has both arms.
declare -A session_compares=(
    [gate]="direct:brisk floor-a:brisk direct:floor-a brisk-pt:brisk"
    [e11]="direct:floor-a direct:floor-b floor-a:floor-b"
    [e1]="brisk-axum:brisk-match"
    [e2]="brisk-segments:brisk-concat"
    [e4]="brisk-hold0s:brisk-hold2s"
)

# NAME|default|meaning. An empty default means "not passed" or a fallback
# named in the meaning. The scenario defaults follow run-m0.sh, whose knob
# table explains the mock TTFTs above 0 in S2 and S3 and the write lag limits.
knob_table=(
    "SESSION||session: gate, e11, e1, e2 or e4 (required; see above)"
    "RUN_ID||session directory name (default: m1-<SESSION>-<UTC time>)"
    "RESULTS_ROOT|$HOME/bench-results|parent of the session directory"
    "BIN_DIR|$HOME/brisk-target/release|directory of brisk, brisk-mock, brisk-floor and brisk-loadgen"
    "CONFIGS||configs to run, in order (N, P, T; default: per session, see above)"
    "SCENARIOS||scenarios to run, in order (sc, s1, s2, ramp, s3; default: per session)"
    "REPS||repetitions of every scenario (default: S1 5, S2 3, S3 3, ramp 3)"
    "S1_REPS||S1 repetitions per block (default: REPS, else 5)"
    "S2_REPS||S2 repetitions per block (default: REPS, else 3)"
    "S3_REPS||S3 repetitions per size (default: REPS, else 3)"
    "RAMP_REPS||ramp pairs of the two arms under test (default: REPS, else 3)"
    "RETRY_INVALID|1|reruns of a repetition that has an invalid run"
    "EXTEND_UNCERTAIN|1|1 adds repetitions to a block while one of its gated verdicts is UNCERTAIN"
    "EXTEND_MAX_REPS|10|repetitions a block is extended to at most"
    "SEED|1|schedule seed of repetition 1; repetition r uses SEED+r-1"
    "SPIN_US|50|busy-wait window of mock and loadgen, microseconds"
    "MOCK_EMIT_POLICY|fixed|emission policy of every mock (full-spin, fixed)"
    "MOCK_COMMIT_US|10|commit window of the fixed emission policy, microseconds"
    "MOCK_WRITE_LAG_LIMIT_US|20|mock write lag p99 limit of the selfcheck, S1 and S2 runs, microseconds"
    "SUT_CPUS|4-7|CPUs of the process under test (floor --cpu-list, Brisk taskset)"
    "SUT_WORKERS||workers of the process under test (default: one per SUT_CPUS entry; floor --workers, Brisk server.workers)"
    "MOCK_CPUS|2-3|CPUs of the mocks"
    "MOCK_SHARDS|2|shards per mock"
    "LOADGEN_CPUS|0-1|CPUs of brisk-loadgen"
    "LOADGEN_SHARDS|2|loadgen shard threads"
    "HARNESS_CPUS||CPUs of this script and its helpers (default: first LOADGEN_CPUS entry)"
    "MOCK_PLAIN_PORT|19080|plaintext mock (direct in P and N, SUT upstream in N)"
    "MOCK_TLS_PORT|19443|TLS mock (direct in T and the tool ramp of P, SUT upstream in P and T)"
    "SUT_P_PORT|19180|process under test, plaintext inbound (P, N)"
    "SUT_T_PORT|19543|process under test, TLS inbound (T)"
    "SC_PORT|19090|selfcheck mock"
    "RESERVED_PORTS|8317|ports of other services on the host, never used and sampled for CPU"
    "ALLOW_UNPREPARED_HOST|0|1 runs even if setup-host.sh --check finds settings to change"
    "SC_GATE|1|1 ends the session when the selfcheck fails"
    "SC_CONCURRENCY||selfcheck target, run at twice this (default: S1_CONCURRENCY)"
    "SC_WARMUP_S|10|selfcheck warmup, seconds"
    "SC_MEASURE_S|30|selfcheck measurement, seconds"
    "S1_CONCURRENCY|1000|S1 target concurrent streams"
    "S1_CHUNK_RATE|30|S1 chunks per second per stream"
    "S1_DUR_MEDIAN|5|S1 median stream duration, seconds"
    "S1_DUR_P99|60|S1 p99 stream duration, seconds"
    "S1_DUR_MAX|120|S1 stream duration cap, seconds"
    "S1_TTFT_US|0|S1 mock TTFT, microseconds (e4 uses E4_TTFTS_US)"
    "S1_CHUNK_BYTES|128|S1 content bytes per chunk"
    "S1_MAX_SLIP_US|200|request slip p99 limit of direct S1 arms, microseconds"
    "S1_WARMUP_S|150|S1 warmup, seconds"
    "S1_MEASURE_S|300|S1 measurement, seconds"
    "E4_TTFTS_US|0,300000|S1 mock TTFTs of e4, microseconds, one block each"
    "S2_RATE|2000|S2 fixed request rate per second"
    "S2_TTFT_US|200|S2 mock delay, microseconds"
    "S2_RESP_BYTES|1024|S2 and ramp response content bytes"
    "S2_PROMPT_BYTES|1024|S2 and ramp user message bytes"
    "S2_WARMUP_S|30|S2 warmup, seconds"
    "S2_MEASURE_S|300|S2 measurement, seconds"
    "RAMP_START|20000|ramp rate of the warmup and the first step, per second"
    "RAMP_STEP_PCT|5|ramp increase per step, percent"
    "RAMP_STEP_S|20|ramp step length, seconds"
    "RAMP_STOP_P99_MS|2|ramp p99 limit of a sustainable step, milliseconds"
    "RAMP_MAX_STEPS|40|ramp steps at most"
    "RAMP_WARMUP_S|30|ramp warmup at RAMP_START, seconds"
    "RAMP_TTFT_US|0|ramp mock delay, microseconds"
    "RAMP_MOCK_WRITE_LAG_LIMIT_US|5000|ramp mock write lag p99 limit, microseconds; a ramp ends by saturating, so here the limit only catches a stalled mock"
    "RAMP_TOOL_LIMIT_SHARE|0.9|share of the direct ramp's rate at which the baseline arm makes a throughput comparison tool-limited"
    "S3_SIZES|100k,1m,10m|S3 request body sizes, one block each"
    "S3_RATE|5|S3 request rate per second"
    "S3_TTFT_US|2000|S3 mock TTFT, microseconds"
    "S3_CHUNKS|1|S3 content chunks per response"
    "S3_SPIN_US|500|S3 loadgen busy-wait window, microseconds"
    "S3_MOCK_WRITE_LAG_LIMIT_US|5000|S3 mock write lag p99 limit, microseconds"
    "S3_WARMUP_S|30|S3 warmup per size, seconds"
    "S3_MEASURE_S|300|S3 measurement per size, seconds"
    "COMPARE_QUANTILES|50,99,99.9|compared quantiles of S1 and S2, percent; the verdicts need 50, 99 and 99.9"
    "S3_COMPARE_QUANTILES|50,99|compared quantiles of S3; the verdicts need 50 and 99"
    "COMPARE_RESAMPLES|2000|bootstrap resamples"
    "BASELINE_METRICS|chunk_latency|S1 metrics of compare's baseline criterion (arm A's p99 CI half width below 5%, run-m0.sh), reported only"
    "S2_BASELINE_METRICS|request_latency|S2 metrics of the baseline criterion"
    "S3_BASELINE_METRICS|ttft|S3 metrics of the baseline criterion"
    "MIN_REUSE|0.999|upstream connection reuse (1 - accepts / requests at the mock) below which a run is invalid"
    "IDLE_RSS_S|60|seconds the SUT idles between readiness and the first S1 request, for the idle RSS"
    "READY_TIMEOUT_S|30|seconds Brisk may take until GET /readyz returns 200"
    "PT_CONFIGS|P|gate configs whose S1 also runs brisk-pt (stream_usage passthrough, reported only); empty for none"
    "PERF|auto|perf stat run per SUT arm after S2: auto (when perf works), 1 (required), 0 (never)"
    "PERF_EVENTS||HITM-related perf events to try, comma-separated (default: Intel and AMD names, see perf_probe)"
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
            (($# >= 2)) || { echo "run-m1: $flag needs a value" >&2; exit 2; }
            value="$2"
            shift 2
            ;;
        *) echo "run-m1: unexpected argument: $1" >&2; exit 2 ;;
    esac
    name="${flag#--}"
    name="${name//-/_}"
    name="${name^^}"
    if [[ ! "$name" =~ ^[A-Z0-9_]+$ || -z "${knob_known[$name]:-}" ]]; then
        echo "run-m1: unknown option $flag (see --help)" >&2
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

valid_cpu_list() {
    [[ "$1" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]]
}

if [[ -z "$SESSION" ]]; then
    echo "run-m1: --session is required: gate, e11, e1, e2 or e4 (see --help)" >&2
    exit 2
fi
if [[ ! "$SESSION" =~ ^(gate|e11|e1|e2|e4)$ ]]; then
    echo "run-m1: unknown session '$SESSION'; use gate, e11, e1, e2 or e4" >&2
    exit 2
fi
for name in SUT_CPUS MOCK_CPUS LOADGEN_CPUS; do
    valid_cpu_list "${!name}" || die "$name must be a CPU list like 4-7 or 2,3, got '${!name}'"
done
: "${RUN_ID:=m1-$SESSION-$(date -u +%Y%m%dT%H%M%SZ)}"
: "${CONFIGS:=${session_configs[$SESSION]}}"
: "${SCENARIOS:=${session_scenarios[$SESSION]}}"
: "${SUT_WORKERS:=$(cpu_count "$SUT_CPUS")}"
: "${HARNESS_CPUS:=${LOADGEN_CPUS%%[,-]*}}"
: "${SC_CONCURRENCY:=$S1_CONCURRENCY}"
: "${S1_REPS:=${REPS:-5}}" "${S2_REPS:=${REPS:-3}}" "${S3_REPS:=${REPS:-3}}" "${RAMP_REPS:=${REPS:-3}}"

if ((print_config)); then
    for name in "${knob_names[@]}"; do
        printf '%s=%s\n' "$name" "${!name}"
    done
    exit 0
fi

# ---------------------------------------------------------------- checks

positive_number() {
    [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk -v v="$1" 'BEGIN { exit !(v > 0) }'
}

[[ -z "$REPS" || "$REPS" =~ ^[0-9]+$ ]] || die "REPS must be a non-negative integer, got '$REPS'"
for name in RETRY_INVALID EXTEND_MAX_REPS SEED SPIN_US S3_SPIN_US MOCK_COMMIT_US MOCK_SHARDS LOADGEN_SHARDS \
    MOCK_PLAIN_PORT MOCK_TLS_PORT SUT_P_PORT SUT_T_PORT SC_PORT SC_CONCURRENCY SC_WARMUP_S SC_MEASURE_S \
    S1_CONCURRENCY S1_TTFT_US S1_CHUNK_BYTES S1_MAX_SLIP_US S1_WARMUP_S S1_MEASURE_S \
    S2_TTFT_US S2_RESP_BYTES S2_PROMPT_BYTES S2_WARMUP_S S2_MEASURE_S \
    RAMP_STEP_S RAMP_MAX_STEPS RAMP_WARMUP_S RAMP_TTFT_US \
    S3_TTFT_US S3_CHUNKS S3_WARMUP_S S3_MEASURE_S COMPARE_RESAMPLES IDLE_RSS_S READY_TIMEOUT_S \
    S1_REPS S2_REPS S3_REPS RAMP_REPS SUT_WORKERS; do
    [[ "${!name}" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer, got '${!name}'"
done
for name in S1_REPS S2_REPS S3_REPS RAMP_REPS SUT_WORKERS MOCK_SHARDS LOADGEN_SHARDS READY_TIMEOUT_S \
    EXTEND_MAX_REPS RAMP_STEP_S RAMP_MAX_STEPS COMPARE_RESAMPLES S1_CONCURRENCY S1_MAX_SLIP_US; do
    ((${!name} >= 1)) || die "$name must be at least 1"
done
# The CPU window spans the measurement less a second at each end, and S1
# samples the memory in its middle.
for name in SC_MEASURE_S S1_MEASURE_S S2_MEASURE_S S3_MEASURE_S; do
    ((${!name} >= 4)) || die "$name must be at least 4 seconds"
done
for name in S1_CHUNK_RATE S1_DUR_MEDIAN S1_DUR_P99 S1_DUR_MAX S2_RATE S3_RATE RAMP_START RAMP_STEP_PCT \
    RAMP_STOP_P99_MS MOCK_WRITE_LAG_LIMIT_US S3_MOCK_WRITE_LAG_LIMIT_US RAMP_MOCK_WRITE_LAG_LIMIT_US; do
    positive_number "${!name}" || die "$name must be a positive number, got '${!name}'"
done
for name in MIN_REUSE RAMP_TOOL_LIMIT_SHARE; do
    if ! positive_number "${!name}" || ! awk -v v="${!name}" 'BEGIN { exit !(v <= 1) }'; then
        die "$name must be a number in (0, 1], got '${!name}'"
    fi
done
[[ "$MOCK_EMIT_POLICY" == full-spin || "$MOCK_EMIT_POLICY" == fixed ]] ||
    die "MOCK_EMIT_POLICY must be full-spin or fixed, got '$MOCK_EMIT_POLICY'"
for name in SC_GATE ALLOW_UNPREPARED_HOST EXTEND_UNCERTAIN; do
    [[ "${!name}" == 0 || "${!name}" == 1 ]] || die "$name must be 0 or 1, got '${!name}'"
done
[[ "$PERF" =~ ^(auto|0|1)$ ]] || die "PERF must be auto, 0 or 1, got '$PERF'"
[[ -z "$PERF_EVENTS" || "$PERF_EVENTS" =~ ^[A-Za-z0-9_.:/=-]+(,[A-Za-z0-9_.:/=-]+)*$ ]] ||
    die "PERF_EVENTS must be a comma-separated list of perf event names, got '$PERF_EVENTS'"
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "RUN_ID may contain letters, digits, '.', '_' and '-' only"
[[ "$RESERVED_PORTS" =~ ^[0-9]*([ ,]+[0-9]+)*$ ]] ||
    die "RESERVED_PORTS must be a list of port numbers, got '$RESERVED_PORTS'"
read -r -a reserved_ports <<<"${RESERVED_PORTS//,/ }"
[[ "$S3_SIZES" =~ ^[0-9]+[kKmM]?(,[0-9]+[kKmM]?)*$ ]] ||
    die "S3_SIZES must be a list of sizes like 100k,1m,10m, got '$S3_SIZES'"
[[ "$E4_TTFTS_US" =~ ^[0-9]+(,[0-9]+)*$ ]] ||
    die "E4_TTFTS_US must be a comma-separated list of microseconds, got '$E4_TTFTS_US'"
mapfile -t s3_sizes < <(tr ',' '\n' <<<"${S3_SIZES,,}")
mapfile -t e4_ttfts < <(tr ',' '\n' <<<"$E4_TTFTS_US")

seen=" "
for c in $CONFIGS; do
    [[ "$c" =~ ^[NPT]$ ]] || die "CONFIGS may contain N, P and T only, got '$c'"
    [[ "$seen" != *" $c "* ]] || die "CONFIGS names $c twice"
    seen+="$c "
done
[[ -n "${CONFIGS// /}" ]] || die "CONFIGS is empty"
for c in $PT_CONFIGS; do
    [[ "$c" =~ ^[NPT]$ ]] || die "PT_CONFIGS may contain N, P and T only, got '$c'"
done
seen=" "
scenario_list=()
for s in $SCENARIOS; do
    [[ "$s" =~ ^(sc|s1|s2|ramp|s3)$ ]] || die "SCENARIOS may contain sc, s1, s2, ramp and s3 only, got '$s'"
    [[ "$seen" != *" $s "* ]] || die "SCENARIOS names $s twice"
    seen+="$s "
    [[ "$s" == sc ]] || scenario_list+=("$s")
done

has_scenario() {
    [[ " $SCENARIOS " == *" $1 "* ]]
}

has_config() {
    [[ " $CONFIGS " == *" $1 "* ]]
}

in_list() {
    local item="$1" other
    shift
    for other in "$@"; do
        [[ "$item" != "$other" ]] || return 0
    done
    return 1
}

# Whether a comma-separated percent list names the quantile, e.g. 99.9.
has_quantile() {
    [[ ",$1," == *",$2,"* ]]
}

compared_metrics() {
    case "$1" in
        s1) echo ttft,chunk_latency,chunk_wire,mock_write_lag ;;
        s2) echo request_latency ;;
        s3) echo ttft,chunk_latency ;;
    esac
}

baseline_metrics() {
    case "$1" in
        s1) echo "$BASELINE_METRICS" ;;
        s2) echo "$S2_BASELINE_METRICS" ;;
        s3) echo "$S3_BASELINE_METRICS" ;;
    esac
}

compare_quantiles() {
    if [[ "$1" == s3 ]]; then echo "$S3_COMPARE_QUANTILES"; else echo "$COMPARE_QUANTILES"; fi
}

# A metric compare does not compare would make it refuse the comparison, and
# a missing quantile would leave a verdict without data, both only at the end
# of a block hours into the session.
for s in s1 s2 s3; do
    has_scenario "$s" || continue
    var_name=BASELINE_METRICS
    [[ "$s" == s1 ]] || var_name="${s^^}_BASELINE_METRICS"
    [[ "${!var_name}" =~ ^[a-z_]+(,[a-z_]+)*$ ]] ||
        die "$var_name must be a comma-separated list of metrics, got '${!var_name}'"
    IFS=, read -r -a metric_list <<<"${!var_name}"
    for metric in "${metric_list[@]}"; do
        [[ ",$(compared_metrics "$s")," == *",$metric,"* ]] ||
            die "$var_name names $metric, which $s does not compare ($(compared_metrics "$s"))"
    done
done
for q in 50 99 99.9; do
    if has_scenario s1 || has_scenario s2; then
        has_quantile "$COMPARE_QUANTILES" "$q" || die "COMPARE_QUANTILES must include $q for the S1 and S2 verdicts"
    fi
done
for q in 50 99; do
    if has_scenario s3; then
        has_quantile "$S3_COMPARE_QUANTILES" "$q" || die "S3_COMPARE_QUANTILES must include $q for the S3 verdicts"
    fi
done

session_has_brisk=0
session_has_floor=0
for arm in ${session_arms[$SESSION]}; do
    [[ "$arm" != brisk* ]] || session_has_brisk=1
    [[ "$arm" != floor-* ]] || session_has_floor=1
done

[[ -r /sys/devices/system/cpu/online ]] || die "/sys/devices/system/cpu/online is not readable; run-m1.sh needs the Linux benchmark host"
online=" $(expand_cpus "$(</sys/devices/system/cpu/online)") "
for name in SUT_CPUS MOCK_CPUS LOADGEN_CPUS HARNESS_CPUS; do
    valid_cpu_list "${!name}" || die "$name must be a CPU list like 0-1 or 2,3, got '${!name}'"
    # An assignment, so that a failed expansion stops the script.
    expanded="$(expand_cpus "${!name}")"
    for c in $expanded; do
        [[ "$online" == *" $c "* ]] || die "$name includes CPU $c, which is not online (online:$online)"
    done
done

# Overlapping sets would let the tools and the SUT compete for a core without
# any error, which only shows up as a quietly skewed delta.
disjoint() {
    local a="$1" b="$2" c bset
    bset=" $(expand_cpus "${!b}") "
    for c in $(expand_cpus "${!a}"); do
        [[ "$bset" != *" $c "* ]] || die "$a (${!a}) and $b (${!b}) share CPU $c"
    done
}
disjoint SUT_CPUS MOCK_CPUS
disjoint SUT_CPUS LOADGEN_CPUS
disjoint MOCK_CPUS LOADGEN_CPUS
disjoint SUT_CPUS HARNESS_CPUS
for pair in MOCK_SHARDS:MOCK_CPUS LOADGEN_SHARDS:LOADGEN_CPUS; do
    shards_knob="${pair%%:*}" cpus_knob="${pair#*:}"
    ((${!shards_knob} <= $(cpu_count "${!cpus_knob}"))) ||
        die "$shards_knob (${!shards_knob}) exceeds the $(cpu_count "${!cpus_knob}") CPU(s) of $cpus_knob (${!cpus_knob})"
done

for tool in jq curl taskset nstat sha256sum ss getconf flock lscpu grep; do
    command -v "$tool" >/dev/null || die "$tool is required"
done
required_bins=(brisk-mock brisk-loadgen)
((session_has_floor)) && required_bins+=(brisk-floor)
((session_has_brisk)) && required_bins+=(brisk)
for bin in "${required_bins[@]}"; do
    [[ -x "$BIN_DIR/$bin" ]] ||
        die "$BIN_DIR/$bin not found; run scripts/bench/sync.sh (brisk: CARGO_TARGET_DIR=~/brisk-target cargo build --locked --release -p brisk)"
done

ports=("$MOCK_PLAIN_PORT" "$MOCK_TLS_PORT" "$SUT_P_PORT" "$SUT_T_PORT" "$SC_PORT")
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
    # A listening port belongs to someone else; never bind next to it or send
    # load to it.
    if [[ -n "$(ss -Hltn "sport = :$port")" ]]; then
        die "port $port is already in use on this host"
    fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
brisk_template="$repo_root/scripts/bench/brisk-bench.toml.in"
if ((session_has_brisk)); then
    [[ -r "$brisk_template" ]] || die "$brisk_template is missing"
fi

# Everything forked from here on (tee, the sampler, jq, curl) inherits this;
# the measured processes are pinned explicitly.
taskset -pc "$HARNESS_CPUS" $$ >/dev/null || die "pinning the harness to CPUs $HARNESS_CPUS failed"

mkdir -p "$RESULTS_ROOT"
# The lock of run-m0.sh: sync.sh checks it, and M0 and M1 sessions share
# ports and CPUs. Children inherit fd 9, so a process left over from a
# crashed session keeps the lock, which is intended.
lock_file="$RESULTS_ROOT/.run-m0.lock"
exec 9>>"$lock_file"
flock -n 9 || die "another run-m0.sh or run-m1.sh session (or a process left over from one) holds $lock_file"

run_dir="$RESULTS_ROOT/$RUN_ID"
if [[ -e "$run_dir" ]] && [[ -n "$(ls -A "$run_dir")" ]]; then
    die "$run_dir already exists and is not empty; choose another RUN_ID"
fi
mkdir -p "$run_dir"/{results,compare,logs,host,bin,brisk}
touch "$run_dir"/{runs,compares,verdicts,processes}.jsonl
# Loadgen results carry the key in their recorded parameters until they are
# redacted, so they are written where only this user can read them.
stage_dir="$run_dir/.staging"
mkdir -m 700 "$stage_dir"
: >"$run_dir/host/sync-info"
: >"$run_dir/host/binaries.txt"
for name in "${knob_names[@]}"; do
    printf '%s=%s\n' "$name" "${!name}"
done >"$run_dir/host/knobs.env"
# tee ignores the hangup of a closed terminal and keeps writing the log.
exec > >(trap '' INT HUP; exec tee -a --output-error=warn "$run_dir/session.log") 2>&1

# ---------------------------------------------------------------- state

declare -A pids=()
declare -A foreign_pids=()
declare -A run_ctx=()
declare -A block_reps=()
lg_args=()
sampler_pid=""
certs_dir=""
ca=""
bench_key=""
key_sha256=""
key_name="run-m1"
# brisk-mock ignores the upstream key; Brisk refuses an empty one.
upstream_key_env="BRISK_BENCH_UPSTREAM_KEY"
upstream_key_value="brisk-mock-does-not-check-this"
loadgen_model="brisk-bench"
e2_mapped_model="brisk-bench-mapped"
sc_tls=0
run_ok=1
manifest_written=0
session_started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
invalid_runs=0
failed_runs=0
evaluation_errors=0
retries=0
extensions=0
host_ready=unknown
selfcheck_status=not_run
selfcheck_detail=""
perf_status=not_needed
perf_reason="the session runs no S2 block"
perf_events=""
perf_hitm_events=""
perf_hitm_note=""
clk_tck="$(getconf CLK_TCK)"

# ---------------------------------------------------------------- jq programs

# Shared definitions. judge_le is the 7.5 rule: FAIL when the interval's low
# end exceeds the limit, PASS when the estimate and the high end are within
# it, else UNCERTAIN; a single repetition's within-run interval is never
# PASS (03 section 9.2). tint is the Student t interval (95%) of a list.
# shellcheck disable=SC2016
jq_defs='
def absv: if . < 0 then 0 - . else . end;
def r2: . * 100 | round / 100;
def signed: if . == null then "n/a"
            else r2 as $v | (if $v >= 0 then "+" else "" end) + ($v | tostring)
            end;
def us: if . == null then "n/a" else . / 1000 | signed end;
def fixed1: if . == null then "n/a" else (. * 10 | round / 10 | tostring) end;
def fixed3: if . == null then "n/a" else (. * 1000 | round / 1000 | tostring) end;
def pct: if . == null then "n/a" else (. * 1000 | round / 10 | tostring) + "%" end;
def pct4: if . == null then "n/a" else (. * 1000000 | round / 10000 | tostring) + "%" end;
def rate: if . == null then "n/a" else (round | tostring) end;
def mib: if . == null then null else . / 1024 end;
def qlabel: "p" + (. * 100 * 1e6 | round / 1e6 | tostring);
def tcrit($df):
    if $df < 1 then null
    elif $df <= 30 then
        [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
         2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
         2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042][$df - 1]
    else 1.959964 as $z
         | $z + ($z * $z * $z + $z) / (4 * $df)
           + (5 * $z * $z * $z * $z * $z + 16 * $z * $z * $z + 3 * $z) / (96 * $df * $df)
    end;
def tint: length as $n
    | if $n == 0 then {n: 0, mean: null, lo: null, hi: null, half: null}
      else (add / $n) as $m
           | if $n < 2 then {n: $n, mean: $m, lo: null, hi: null, half: null}
             else ((map((. - $m) * (. - $m)) | add) / ($n - 1) | sqrt) as $sd
                  | (tcrit($n - 1) * $sd / ($n | sqrt)) as $h
                  | {n: $n, mean: $m, lo: ($m - $h), hi: ($m + $h), half: $h}
             end
      end;
def judge_le($x; $lo; $hi; $thr; $single):
    if $single then "UNCERTAIN"
    elif $lo > $thr then "FAIL"
    elif $x <= $thr and $hi <= $thr then "PASS"
    else "UNCERTAIN" end;
def judge_abs($x; $lo; $hi; $thr; $single):
    if $single then "UNCERTAIN"
    elif $lo > $thr or $hi < (0 - $thr) then "FAIL"
    elif $lo >= (0 - $thr) and $hi <= $thr then "PASS"
    else "UNCERTAIN" end;
def last_by(f): reduce .[] as $r ({order: [], by: {}};
        ($r | f) as $k
        | (if .by | has($k) then . else .order += [$k] end)
        | .by[$k] = $r)
    | [.order[] as $k | .by[$k]];
def ci_text($t; f): "[\($t.lo | f), \($t.hi | f)]";
'

# Repetitions of one block whose wanted arms are all valid in one attempt
# (the latest such attempt of each repetition). An attempt may also hold
# other arms (brisk-pt next to the gated ones) or only some of the wanted
# ones (the rerun of Brisk and brisk-pt). Input: runs.jsonl, slurped.
# shellcheck disable=SC2016
prog_select_reps='
($arms | split(" ")) as $want
| [.[] | select(.scen == $s and .config == $c and .var == $v and .kind == "run")]
| [group_by(.rep)[]
   | [group_by(.attempt)[]
      | map(select(.arm | IN($want[])))
      | select(length == ($want | length)
               and all(.[]; .valid == true and .file != null)
               and ((map(.arm) | sort) == ($want | sort)))]
   | last
   | select(. != null)] as $reps
| {reps: ($reps | length), pair_ids: [$reps[] | .[0].pair_id],
   runs: (reduce $want[] as $a ({}; .[$a] = [$reps[][] | select(.arm == $a)]))}
'

# One compares.jsonl record from a compare document.
# shellcheck disable=SC2016
prog_compare_record='
{name: $name, block: $block, status: $status, scen: $scen, a_arm: $a, b_arm: $b,
 json: "compare/\($name).json", text: "compare/\($name).txt",
 reps_used: $reps, reps_planned: $planned, pair_ids: $pair_ids,
 single_repetition_fallback, baseline_p99_ci_ok, baseline_pass: .gate_pass,
 metrics: [.comparisons[] as $c
           | $c
           | {metric, baseline_metric: .gate, baseline_pass: .pass, baseline_p99_ci_half_width_ratio,
              quantiles: [range(0; $c.quantiles | length) as $i
                          | $c.quantiles[$i]
                          | {q: .quantile, a_us: (.a_ns / 1000), b_us: (.b_ns / 1000),
                             delta_us: (.delta_ns / 1000),
                             delta_ci_us: [(.delta_ci_low_ns / 1000), (.delta_ci_high_ns / 1000)],
                             per_pair_delta_us: [$c.per_pair[] | .quantiles[$i].delta_ns / 1000]}]}]}
'

# Log lines of a compare document.
# shellcheck disable=SC2016
prog_compare_lines='
.comparisons[] as $c
| "  \($c.metric): baseline p99 CI half width \($c.baseline_p99_ci_half_width_ratio | pct) (M0 criterion 5%, reported)",
  ($c.quantiles[]
   | "    \(.quantile | qlabel): delta \(.delta_ns | us) us, 95% CI [\(.delta_ci_low_ns | us), \(.delta_ci_high_ns | us)] us")
'

# A verdict on one compared quantile's delta; see check_delta.
# shellcheck disable=SC2016
prog_check_delta='
($d[0]) as $doc
| (if $doc == null then null
   else first(($doc.comparisons[] | select(.metric == $metric) | .quantiles[]
               | select(((.quantile - $q) | absv) < 1e-9)), null)
   end) as $e
| (if $doc == null then true else ($doc.single_repetition_fallback == true) end) as $single
| (if $doc == null then 0 else ($doc.pairs | length) end) as $pairs
| ($limit * 1000) as $thr
| (if $e == null
   then {verdict: "MISSING", counts: ($scope == "gated"), note: "no comparison", x: null, lo: null, hi: null}
   else {x: $e.delta_ns, lo: $e.delta_ci_low_ns, hi: $e.delta_ci_high_ns, counts: ($scope == "gated"), note: null}
        # serde_json writes a non-finite bound as null, which jq would
        # order below every number.
        | if .x == null or .lo == null or .hi == null
          then .verdict = (if $kind == "report" then "REPORT" else "UNCERTAIN" end)
               | .note = "the comparison has no finite interval"
          else ((.hi - .lo) / 2) as $half
               | .verdict = (if $kind == "le" then judge_le(.x; .lo; .hi; $thr; $single)
                             elif $kind == "abs" then judge_abs(.x; .lo; .hi; $thr; $single)
                             # A decision rule changes a default, which the
                             # within-run interval of a single repetition is
                             # too narrow to do (03, 9.2).
                             elif ($kind == "improve" or $kind == "below0" or $kind == "notworse") and $single
                             then "UNDECIDED"
                             elif $kind == "improve" then (if .x <= (0 - $thr) and .hi < 0 then "MET" else "NOT_MET" end)
                             elif $kind == "below0" then (if .x < 0 and .hi < 0 then "MET" else "NOT_MET" end)
                             elif $kind == "notworse" then (if .lo <= 0 then "MET" else "NOT_MET" end)
                             else "REPORT" end)
               | if .verdict == "UNDECIDED" then .note = "a single repetition decides no rule" else . end
               | if $prec > 0 and $half > $prec * 1000
                 then .counts = false
                      | .note = "CI half width \($half / 1000 | fixed1) us above \($prec) us, a quarter of the limit"
                 else . end
          end
   end) as $v
| (if $kind == "le" then "limit <= \($limit) us"
   elif $kind == "abs" then "limit |delta| <= \($limit) us"
   elif $kind == "improve" then "rule: delta <= -\($limit) us and CI below 0"
   elif $kind == "below0" then "rule: delta and CI below 0"
   elif $kind == "notworse" then "rule: CI low end <= 0"
   else null end) as $rule_text
| ("\($label): \($metric) \($q | qlabel) delta \($v.x | us) us, 95% CI [\($v.lo | us), \($v.hi | us)] us, \($pairs) pair(s)"
   + (if $single and $e != null then ", one repetition (within-run interval)" else "" end)) as $head
| (if $v.note == null then "" else ": \($v.note)" end) as $note
| (if $rule_text == null then $head + " (reported)"
   else $head + "; \($rule_text): \($v.verdict)"
        + (if $v.counts then " (gated\($note))"
           elif $scope == "decision" then (if $v.note == null then "" else " (\($v.note))" end)
           else " (reported\($note))" end)
   end) as $text
| {id: $id, block: $block, kind: "delta", rule: $rule, verdict: $v.verdict, gated: $v.counts,
   extendable: ($v.counts and $v.verdict == "UNCERTAIN"), text: $text,
   data: {cmp: $cmp, metric: $metric, q: $q, limit_us: $limit, delta_ns: $v.x,
          ci_ns: [$v.lo, $v.hi], pairs: $pairs, single_repetition: $single}}
'

# The ramp pairs of one config and its direct tool limit. Input: runs.jsonl,
# slurped.
# shellcheck disable=SC2016
prog_ramp_pairs='
[.[] | select(.scen == "ramp" and .config == $c)] as $all
| {tool: ([$all[] | select(.kind == "tool" and .valid == true)] | last | .ramp.max_sustainable_rate),
   pairs: ([$all[] | select(.kind == "run")]
           | [group_by(.rep)[]
              | [group_by(.attempt)[]
                 | select(length == 2 and all(.[]; .valid == true)
                          and ((map(.arm) | sort) == ([$a, $b] | sort)))]
              | last
              | select(. != null)
              | {pair_id: .[0].pair_id,
                 a: (map(select(.arm == $a)) | .[0].ramp),
                 b: (map(select(.arm == $b)) | .[0].ramp)}])}
'

# The throughput verdict of a ramp block: 7.5 (gate) or the E1 rule. A ramp
# without a sustainable step failed its first step, so its rate lies below
# RAMP_START ($start), and its pair gets bounds of the ratio B / A instead
# of a value: [0, RAMP_START / A] when B has no rate, [B / RAMP_START, none]
# when A has none, no bound when neither has. The 7.5 rule over at least 3
# pairs: PASS when every ratio is at least 0.85 (every low bound), FAIL when
# their mean is below 0.85 (the mean of the high bounds), else UNCERTAIN,
# which more pairs can settle unless a pair has no bound. A tool-limited
# block is reported only, whatever its ratios.
# shellcheck disable=SC2016
prog_ramp_verdict='
def ramp_rate: if . == null then "<" + ($start | rate) else rate end;
def ratio_text: if .lo == .hi then .lo | fixed3
                elif .hi != null then "<" + (.hi | fixed3)
                elif .lo > 0 then ">" + (.lo | fixed3)
                else "n/a" end;
.tool as $tool
| .pairs as $pairs
| ($pairs | length) as $n
| ([$pairs[] | .a.max_sustainable_rate | numbers] | max) as $amax
# Without any rate A stays below RAMP_START, which the tool limit, a step
# rate itself, is at least.
| (if $tool == null then null
   elif $amax != null then ($amax >= $share * $tool)
   elif $share * $tool >= $start then false
   else null end) as $limited
| [$pairs[] | .a.max_sustainable_rate as $ra | .b.max_sustainable_rate as $rb
   | if $ra != null and $rb != null then {lo: ($rb / $ra), hi: ($rb / $ra)}
     elif $ra != null then {lo: 0, hi: ($start / $ra)}
     elif $rb != null then {lo: ($rb / $start), hi: null}
     else {lo: 0, hi: null} end] as $bounds
| [$bounds[] | if .lo == .hi then .lo else null end] as $ratios
| ([$bounds[] | select(.lo == 0 and .hi == null)] | length) as $unbounded
| (if $session == "e1" then
     {rule: "e1-throughput", counts: false, statistical: false,
      rule_text: "rule: at least 3 pairs, \($b) never a step below \($a) and a step (\($step_pct)%) above it in 2",
      verdict: (if $n == 0 then "MISSING"
                elif $limited == true then "TOOL_LIMITED"
                elif $limited == null then "TOOL_LIMIT_UNKNOWN"
                elif $n >= 3
                     and all($pairs[]; .a.last_step != null and .b.last_step != null
                                       and .b.last_step >= .a.last_step)
                     and ([$pairs[] | select(.a.last_step != null and .b.last_step != null
                                             and .b.last_step >= .a.last_step + 1)] | length) >= 2
                then "MET" else "NOT_MET" end)}
   else
     # Tool-limited first: such a block is reported only, whatever else holds.
     (if $n == 0 then {verdict: "MISSING", note: "no pair of valid ramps"}
      elif $limited == true then {verdict: "REPORT", note: "tool-limited: \($a) reached \($amax / $tool | pct) of the direct ramp"}
      elif $limited == null and $tool != null
      then {verdict: "UNCERTAIN",
            note: "\($a) has no sustainable step in any pair, and RAMP_START exceeds \($share) of the tool limit, so whether the block is tool-limited is unknown"}
      elif $limited == null then {verdict: "UNCERTAIN", note: "tool limit unknown: no valid direct ramp"}
      elif $n < 3 then {verdict: "UNCERTAIN", statistical: true, note: "\($n) pair(s), the rule needs 3"}
      elif all($bounds[]; .lo >= 0.85) then {verdict: "PASS"}
      elif all($bounds[]; .hi != null) and ([$bounds[].hi] | add / length) < 0.85 then {verdict: "FAIL"}
      elif $unbounded > 0
      then {verdict: "UNCERTAIN",
            note: "neither ramp of \($unbounded) pair(s) sustained RAMP_START (\($start | rate) req/s), so their ratios have no bound; lower RAMP_START"}
      else {verdict: "UNCERTAIN", statistical: true} end)
     | . + {rule: "7.5 S2 throughput",
            rule_text: "rule: at least 3 pairs, every ratio >= 0.85 PASS, their mean < 0.85 FAIL"}
     | .counts = ($scope == "gated" and .verdict != "REPORT")
   end) as $v
| ([$pairs[] | "\(.b.max_sustainable_rate | ramp_rate)/\(.a.max_sustainable_rate | ramp_rate)"] | join(", ")) as $rates
| (if $v.note == null then "" else ": \($v.note)" end) as $note
| {id: "\($block):ramp", block: $block, kind: "ramp", rule: $v.rule, verdict: $v.verdict, gated: $v.counts,
   extendable: ($v.counts and $v.verdict == "UNCERTAIN" and $v.statistical == true),
   text: ("\($block): max sustainable rate \($b)/\($a) per pair [\($rates)] req/s, ratios ["
          + ([$bounds[] | ratio_text] | join(", "))
          + "]; direct tool limit \($tool | rate) req/s; \($v.rule_text): \($v.verdict)"
          + (if $v.counts then " (gated\($note))"
             elif $session == "e1" then ""
             else " (reported\($note))" end)),
   data: {tool: $tool, limited: $limited, ratios: $ratios, ratio_bounds: $bounds, pairs: $pairs}}
'

# Sustainable rate per vCPU of the SUT, from the ramp pairs.
# shellcheck disable=SC2016
prog_ramp_vcpu='
def per_cpu: if . == null then null else . / $cpus end;
([.pairs[] | .a.max_sustainable_rate | numbers] | tint) as $ra
| ([.pairs[] | .b.max_sustainable_rate | numbers] | tint) as $rb
| {id: "\($block):per-vcpu", block: $block, kind: "report", rule: "7.5 S2 per vCPU", verdict: "REPORT",
 gated: false, extendable: false,
 text: ("\($block): mean sustainable rate per SUT vCPU (\($cpus)): \($b) \($rb.mean | per_cpu | rate), \($a) \($ra.mean | per_cpu | rate) req/s"
        + (if $session == "gate"
           then " (reported; 7.5 reference for Brisk: >= 20000 in P, >= 12000 in T; whether a vCPU of the host is a physical core is not confirmed)"
           else " (reported)" end)),
 data: {a: $ra, b: $rb, cpus: $cpus}}
'

# Upstream connection reuse of a block, 7.5: at least 99.9% in the steady
# state. A run below MIN_REUSE is voided and its repetition rerun (6.2),
# which keeps cold connections out of the comparisons but does not undo the
# observation, so the verdict reads every run of the block, voided ones
# included, and one run of a judged arm below 99.9% fails it. Judged are the
# Brisk arms of the selection, whose property 7.5 states, or in a block
# without one every arm but direct; the others are reported. A failed run
# (loadgen or the SUT broke) has no steady state and is left out. Input: a
# selection; $runs: runs.jsonl, slurped.
# shellcheck disable=SC2016
prog_reuse='
[.runs | keys_unsorted[]] as $arms
| ([$arms[] | select(startswith("brisk"))] | if length > 0 then . else [$arms[] | select(. != "direct")] end) as $judged
| [$runs[] | select(.scen == $s and .config == $c and .var == $v and .kind == "run" and .failed != true
                    and (.reuse.rate | type) == "number")] as $all
| [$arms[] as $arm
   | [$all[] | select(.arm == $arm)] as $r
   | {arm: $arm, judged: ($arm | IN($judged[])), runs: ($r | length), lowest: ([$r[].reuse.rate] | min),
      below: ([$r[] | select(.reuse.rate < 0.999)] | length),
      voided_below: ([$r[] | select(.reuse.rate < 0.999 and .valid != true)] | length)}] as $by
| [$by[] | select(.judged)] as $j
| (if ($j | length) == 0 or any($j[]; .runs == 0) then "MISSING"
   elif all($j[]; .below == 0) then "PASS"
   else "FAIL" end) as $verdict
| {id: "\($block):reuse", block: $block, kind: "reuse", rule: "7.5 reuse", verdict: $verdict,
   gated: ($scope == "gated"), extendable: false,
   text: ("\($block) upstream connection reuse over the window, lowest per arm over all its runs, voided ones included: "
          + ([$by[] | "\(.arm) \(.lowest | pct4) (\(.runs) run(s)"
                      + (if .below == 0 then ""
                         else ", \(.below) below 99.9%"
                              + (if .voided_below > 0 then ", \(.voided_below) of them voided and rerun" else "" end) end)
                      + (if .judged then "" else ", reported" end) + ")"] | join(", "))
          + "; limit >= 99.9% in every run of \($judged | join(", ")): \($verdict)"
          + (if $scope == "gated" then " (gated)" else " (reported)" end)),
   data: $by}
'

# CPU of the SUT per chunk or request, per arm. Input: a selection.
# shellcheck disable=SC2016
prog_cpu_arms='
[.runs | to_entries[] | select(.key != "direct")
 | {arm: .key, cpu: ([.value[].sut[$f] | numbers] | tint)}] as $by
| {id: "\($block):cpu", block: $block, kind: "report", rule: "cpu", verdict: "REPORT",
   gated: false, extendable: false,
   text: ("\($block) SUT CPU (utime + stime) per \($unit): "
          + ([$by[] | "\(.arm) \(.cpu.mean | fixed3) us"] | join(", ")) + " (reported)"),
   data: $by}
'

# CPU of two SUT arms per repetition: B - A and B / A with t intervals.
# shellcheck disable=SC2016
prog_cpu_pair='
[range(0; .reps) as $i
 | {a: .runs[$a][$i].sut[$f], b: .runs[$b][$i].sut[$f]}
 | select(.a != null and .b != null)] as $pp
| ([$pp[] | .b - .a] | tint) as $d
| ([$pp[] | select(.a > 0) | .b / .a] | tint) as $r
| {id: "\($block):cpu:\($b)-vs-\($a)", block: $block, kind: "report", rule: $rule, verdict: "REPORT",
   gated: false, extendable: false,
   text: ("\($block) CPU per \($unit) \($b) - \($a): \($d.mean | signed) us, 95% t CI \(ci_text($d; signed)) us, "
          + "ratio \($r.mean | fixed3) \(ci_text($r; fixed3)), \($d.n) pair(s)"
          + (if $rule == "7.5 CPU" then " (reported; M2 gates it against the M1 anchor)" else " (reported)" end)),
   data: {diff_us: $d, ratio: $r, per_pair: $pp}}
'

# Memory of the SUT arms of an S1 block. Input: a selection.
# shellcheck disable=SC2016
prog_memory='
[.runs | to_entries[] | select(.key != "direct")
 | {arm: .key, idle_kib: ([.value[].sut.idle_rss_kib | numbers] | tint),
    per_1k_mib: ([.value[].sut.mem_per_1k_streams_mib | numbers] | tint)}] as $by
| {id: "\($block):memory", block: $block, kind: "report", rule: "7.5 memory", verdict: "REPORT",
   gated: false, extendable: false,
   text: ("\($block) SUT memory: "
          + ([$by[] | "\(.arm) idle RSS \(.idle_kib.mean | mib | fixed1) MiB, per 1k streams \(.per_1k_mib.mean | fixed1) MiB"]
             | join("; "))
          + (if $session == "gate"
             then " (reported; 7.5 reference for Brisk: idle RSS <= 20 MiB, per 1k streams <= 48 MiB in P)"
             else " (reported)" end)),
   data: $by}
'

# perf stat counts of the S2 perf runs of one config. Input: runs.jsonl,
# slurped.
# shellcheck disable=SC2016
prog_perf_report='
[.[] | select(.kind == "perf" and .scen == "s2" and .config == $c)] | group_by(.arm) | map(last) as $recs
| {id: "\($block):perf", block: $block, kind: "report", rule: "7.5 HITM", verdict: "REPORT",
   gated: false, extendable: false,
   text: ("\($block) perf stat on the SUT over an extra S2 window (reported): "
          + (if ($recs | length) == 0 then "no perf run"
             else ([$recs[]
                    | .perf as $p
                    | "\(.arm): "
                      + (if $p.status == "ran"
                         then ([$p.counts | to_entries[]
                                | "\(.key) \(.value | rate) (\($p.per_request[.key] | fixed3)/request)"]
                               | join(", "))
                         else "not counted (\($p.reason))" end)]
                   | join("; "))
             end)
          + $hitm_note),
   data: [$recs[] | {arm, perf, requests: .sut.requests}]}
'

# E11 on S1 chunk_latency p99 (contract 05, 6.3).
# shellcheck disable=SC2016
prog_e11_latency='
def p99($x):
    if $x == null then null
    else first(($x.comparisons[] | select(.metric == "chunk_latency") | .quantiles[]
                | select(((.quantile - 0.99) | absv) < 1e-9)), null)
    end;
p99($fa[0]) as $A
| p99($fb[0]) as $B
| p99($ab[0]) as $D
| (if $A == null or $B == null or $D == null then {verdict: "MISSING", note: "a comparison is missing"}
   # The rule decides whether to build a pool, which the within-run interval
   # of a single repetition is too narrow to do (03, 9.2).
   elif any($fa[0], $fb[0], $ab[0]; .single_repetition_fallback == true)
   then {verdict: "UNCERTAIN", note: "a single repetition decides no rule"}
   elif $A.delta_ns <= 0 then {verdict: "NOT_TRIGGERED", note: "floor-A adds no p99 over direct"}
   elif $D.delta_ci_high_ns == null then {verdict: "UNCERTAIN", note: "floor-B - floor-A has no finite interval"}
   elif $B.delta_ns < 0.8 * $A.delta_ns and $D.delta_ci_high_ns < 0 then {verdict: "TRIGGERED"}
   elif $B.delta_ns < 0.8 * $A.delta_ns then {verdict: "NOT_TRIGGERED", note: "the CI of floor-B - floor-A reaches 0"}
   else {verdict: "NOT_TRIGGERED"} end) as $v
| {id: "\($block):e11-latency", block: $block, kind: "e11", rule: "e11-latency", verdict: $v.verdict,
   gated: false, extendable: false,
   text: ("\($block) E11 chunk_latency p99: floor-B - direct \($B.delta_ns | us) us, floor-A - direct \($A.delta_ns | us) us, "
          + "floor-B - floor-A \($D.delta_ns | us) us, 95% CI [\($D.delta_ci_low_ns | us), \($D.delta_ci_high_ns | us)] us; "
          + "rule: floor-B below 80% of floor-A and that CI below 0 triggers: \($v.verdict)"
          + (if $v.note == null then "" else " (\($v.note))" end)),
   data: {floor_a_delta_ns: $A.delta_ns, floor_b_delta_ns: $B.delta_ns,
          b_minus_a: {delta_ns: $D.delta_ns, ci_ns: [$D.delta_ci_low_ns, $D.delta_ci_high_ns]}}}
'

# E11 on CPU per chunk, floor-B / floor-A. Input: a selection.
# shellcheck disable=SC2016
prog_e11_cpu='
[range(0; .reps) as $i
 | [.runs["floor-a"][$i].sut.cpu_us_per_chunk, .runs["floor-b"][$i].sut.cpu_us_per_chunk]
 | select(.[0] != null and .[1] != null and .[0] > 0)
 | .[1] / .[0]] as $ratios
| ($ratios | tint) as $t
| (if $t.n == 0 then "MISSING"
   elif $t.lo == null then "UNCERTAIN"
   elif $t.hi < 0.85 then "TRIGGERED"
   elif $t.lo >= 0.85 then "NOT_TRIGGERED"
   else "UNCERTAIN" end) as $v
| {id: "\($block):e11-cpu", block: $block, kind: "e11", rule: "e11-cpu", verdict: $v,
   gated: false, extendable: false,
   text: ("\($block) E11 CPU per chunk floor-B / floor-A: \($t.mean | fixed3), 95% t CI \(ci_text($t; fixed3)), \($t.n) pair(s); "
          + "rule: below 0.85 (a drop of more than 15%) triggers: \($v)"),
   data: {ratios: $ratios, t: $t}}
'

# The decision of an e-session from its final verdicts. Input: verdicts.jsonl,
# slurped.
# shellcheck disable=SC2016
prog_decide='
last_by(.id) as $all
| (if $session == "e1" then
     ([$all[] | select(.rule == "e1-latency")] as $lat
     | [$all[] | select(.rule == "e1-throughput")] as $tp
     | (($lat | length) > 0 and all($lat[]; .verdict == "MET")) as $lat_met
     | (($tp | length) > 0 and all($tp[]; .verdict == "MET")) as $tp_met
     | if ($lat | length) == 0 and ($tp | length) == 0
       then {verdict: "UNDECIDED", text: "E1 decision: undecided, neither an S2 comparison nor a ramp"}
       elif $lat_met or $tp_met
       then {verdict: "MATCH",
             text: ("E1 decision: make match the default router ("
                    + ([if $lat_met then "latency rule met" else empty end,
                        if $tp_met then "throughput rule met" else empty end] | join(", ")) + ")")}
       elif any($lat[]; .verdict == "UNDECIDED")
       then {verdict: "UNDECIDED",
             text: "E1 decision: undecided, the latency rule rests on a single repetition and the throughput rule is not met (latency \([$lat[].verdict] | join(", ")), throughput \([$tp[].verdict] | join(", "))); the default stays axum"}
       else {verdict: "AXUM",
             text: "E1 decision: keep axum (latency \([$lat[].verdict] | join(", ")), throughput \([$tp[].verdict] | join(", ")))"}
       end)
   elif $session == "e2" then
     ([$all[] | select(.rule == "e2-ttft")] as $t
     | [$all[] | select(.rule == "e2-chunk")] as $ch
     | if ($t | length) == 0 or ($ch | length) == 0
       then {verdict: "UNDECIDED", text: "E2 decision: undecided, S3 or S1 comparisons are missing"}
       elif all($t[]; .verdict == "MET") and all($ch[]; .verdict == "MET")
       then {verdict: "CONCAT", text: "E2 decision: make concat the default splice (TTFT p50 better in all \($t | length) S3 size(s), S1 chunk p99 not worse)"}
       # No rule decided against concat, but one rests on a single repetition.
       elif any(($t + $ch)[]; .verdict == "UNDECIDED") and all(($t + $ch)[]; .verdict != "NOT_MET")
       then {verdict: "UNDECIDED",
             text: "E2 decision: undecided, a rule rests on a single repetition (S3 TTFT \([$t[].verdict] | join(", ")), S1 chunk p99 \([$ch[].verdict] | join(", "))); the default stays segments"}
       else {verdict: "SEGMENTS",
             text: "E2 decision: keep segments (S3 TTFT \([$t[].verdict] | join(", ")), S1 chunk p99 \([$ch[].verdict] | join(", ")))"}
       end)
   elif $session == "e4" then
     ([$all[] | select(.rule == "e4-p50" or .rule == "e4-p99")] as $e
     | if ($e | length) == 0
       then {verdict: "UNDECIDED", text: "E4 decision: undecided, no S1 comparison"}
       elif all($e[]; .verdict == "PASS")
       then {verdict: "KEEP_2S", text: "E4 decision: keep commit_hold 2s (|TTFT delta p50| <= 10 us and delta p99 <= 30 us everywhere)"}
       elif any($e[]; .verdict == "FAIL")
       then {verdict: "COST", text: "E4 decision: commit_hold 2s exceeds the limits; record the cost for the owner to decide"}
       else {verdict: "UNCERTAIN", text: "E4 decision: uncertain; some intervals straddle the limits or are missing"}
       end)
   elif $session == "e11" then
     ([$all[] | select(.rule == "e11-latency" or .rule == "e11-cpu")] as $e
     | if ($e | length) == 0
       then {verdict: "UNDECIDED", text: "E11 decision: undecided, no S1 block"}
       elif any($e[]; .verdict == "TRIGGERED")
       then {verdict: "BUILD_POOL",
             text: ("E11 decision: build the pool on hyper conn in M2 (triggered by "
                    + ([$e[] | select(.verdict == "TRIGGERED") | "\(.block) \(.rule)"] | join(", ")) + ")")}
       elif any($e[]; .verdict == "UNCERTAIN" or .verdict == "MISSING")
       then {verdict: "UNCERTAIN", text: "E11 decision: uncertain; a rule could not be decided"}
       else {verdict: "KEEP_REQWEST", text: "E11 decision: keep reqwest and keep collecting the legacy pool lock share in the S2 of M2 (C21)"}
       end)
   else empty end)
| {id: "decision", block: "session", kind: "decision", rule: "decision", verdict: .verdict,
   gated: false, extendable: false, text: .text}
'

# One runs.jsonl record from a result file. A ramp's loadgen validity is the
# one judged per step (prog_ramp_evidence).
# shellcheck disable=SC2016
prog_record_run='
.warmup_intervals as $w
| $ctx + {file: $file, label, scenario,
          valid: ((if $h.ramp == null then .validity.valid else $h.ramp.valid end) and ($h.reasons | length) == 0),
          loadgen_valid: .validity.valid,
          reasons: ((if $h.ramp == null then .validity.reasons else $h.ramp.run_reasons end) + $h.reasons),
          failed: $h.failed, seed: .params.common.seed,
          duration_s: .loadgen.duration_s, counters: .loadgen.counters,
          errors: ([.intervals[] | select(.index >= $w) | .errors | to_entries[]]
                   | group_by(.key) | map({key: .[0].key, value: (map(.value) | add)})
                   | from_entries),
          load_check: .loadgen.load_check, selfcheck: .loadgen.selfcheck,
          summary_us: (.summary | map_values({count, p50: (.p50_ns / 1000),
                       p99: (.p99_ns / 1000), p999: (.p999_ns / 1000), max: (.max_ns / 1000)})),
          cpu: $cpu, steal: $steal, net: $net, mock_stats: $mock, mock_balance: $balance,
          reuse: $h.reuse, sut: $h.sut, ramp: $h.ramp, perf: $h.perf}
'

# One runs.jsonl record of a run without a result file.
# shellcheck disable=SC2016
prog_record_missing='
$ctx + {file: null, valid: false, loadgen_valid: null,
        reasons: (["no result file (load generator exit status \($ctx.loadgen_rc))"] + $h.reasons),
        failed: $h.failed, cpu: $cpu, steal: $steal, net: $net, mock_stats: $mock, mock_balance: $balance,
        reuse: $h.reuse, sut: $h.sut, ramp: null, perf: $h.perf}
'

# CPU and memory of the SUT over one run's window. The window counts weight
# every one-second interval of the result by its overlap with the window.
# shellcheck disable=SC2016
prog_sut_evidence='
def num: if . == "" then null else tonumber end;
def window_counts($ws; $we):
    (.started_unix_ms / 1000) as $t0
    | (.intervals[0].start_ns - .intervals[0].index * 1e9) as $o
    | reduce (.intervals[] | select(.duration_ns > 0)) as $iv ({chunks: 0, requests: 0, counted_s: 0};
        ($t0 + ($iv.start_ns - $o) / 1e9) as $s
        | ($iv.duration_ns / 1e9) as $len
        | ((([$s + $len, $we] | min) - ([$s, $ws] | max)) / $len) as $f
        | if $f > 0
          then .chunks += $iv.chunks * $f | .requests += $iv.requests * $f | .counted_s += $f * $len
          else . end);
($ta | num) as $ta
| ($tb | num) as $tb
| ($ka | num) as $ka
| ($kb | num) as $kb
| ($idle | num) as $idle
| ($mid | num) as $mid
| (if $ta != null and $tb != null and $ka != null and $kb != null
   then {cpu_s: (($kb - $ka) / $hz), window_s: ($tb - $ta)}
   else {cpu_s: null, window_s: null} end) as $cpu
| (if $cpu.window_s != null and ($r | length) > 0 and (($r[0].intervals // []) | length) > 0
   then ($r[0] | window_counts($ta; $tb)) else null end) as $w
| {pid: ($pid | num), cpu_s: $cpu.cpu_s, window_s: $cpu.window_s,
   chunks: $w.chunks, requests: $w.requests, counted_s: $w.counted_s,
   cpu_us_per_chunk: (if $cpu.cpu_s != null and ($w.chunks // 0) > 0 then $cpu.cpu_s * 1e6 / $w.chunks else null end),
   cpu_us_per_request: (if $cpu.cpu_s != null and ($w.requests // 0) > 0 then $cpu.cpu_s * 1e6 / $w.requests else null end),
   idle_rss_kib: $idle, mid_rss_kib: $mid,
   mem_per_1k_streams_mib: (if $idle != null and $mid != null
                            then ($mid - $idle) / 1024 / ($streams / 1000) else null end)}
'

# Upstream connection reuse at the arm's mock since the window's reset. The
# statistics request's own connection is the one accept left out.
# shellcheck disable=SC2016
prog_reuse_evidence='
$m[$n] as $s
| if ($ok | not)
  then {mock: $n, accepts: null, requests: null, rate: null, ok: false,
        reason: "the mock statistics were not reset at the start of the window"}
  elif $s == null
  then {mock: $n, accepts: null, requests: null, rate: null, ok: false, reason: "no statistics of \($n)"}
  else ([$s.accepts - 1, 0] | max) as $acc
       | if $s.requests == 0
         then {mock: $n, accepts: $acc, requests: 0, rate: null, ok: false,
               reason: "no request reached \($n) in the window"}
         else (1 - $acc / $s.requests) as $rate
              | {mock: $n, accepts: $acc, requests: $s.requests, rate: $rate, ok: ($rate >= $min),
                 reason: (if $rate >= $min then null
                          else "upstream connection reuse \($rate | pct4) below MIN_REUSE \($min | pct4) (\($acc) accepts, \($s.requests) requests)"
                          end)}
         end
  end
'

# The sustainable rate of a ramp result, by the step condition of contract
# 05, 6.1: the step's p99 within the limit, no error, and loadgen's validity
# checks passing. loadgen checks validity once over the whole ramp, whose
# last step saturates something by design, and keeps no verdict per step;
# its result does keep the one-second intervals, so each step is checked
# here over its own intervals with loadgen's limits (validity.rs): emit lag
# p99 and p99.9 and mock write lag p99 from the interval histograms (base64
# of an uncompressed HDR V2 serialization, read as hdrhistogram 7.6 reads
# it), the failure and stale retry shares from the interval counts. The
# sustainable rate is the last step of the leading run of steps that pass.
# loadgen's whole-ramp reasons for those rules are thereby judged per step
# (judged_per_step); a reason no step can own (a clock step, receives
# without a kernel timestamp, negative spans, no samples at all) still voids
# the ramp (run_reasons, valid). The reasons are told apart by the wording
# of validity.rs, so a reworded one stays a reason of the run. An event
# counts in the interval it happened in: a request sent at the end of one
# step and answered in the next counts in the next, a negligible share of
# a 20 s step. Input: a result file.
# shellcheck disable=SC2016
prog_ramp_evidence='
def hdr_bytes:
    [explode[] | if . >= 65 and . <= 90 then . - 65 elif . >= 97 and . <= 122 then . - 71
                 elif . >= 48 and . <= 57 then . + 4 elif . == 43 then 62 elif . == 47 then 63
                 else empty end] as $s
    | ($s | length * 3 / 4 | floor) as $n
    # The sextets a padded end lacks are zero bits, and the bytes they fill
    # are cut off below.
    | [range(0; $s | length; 4) as $i
       | ($s[$i] * 262144 + ($s[$i + 1] // 0) * 4096 + ($s[$i + 2] // 0) * 64 + ($s[$i + 3] // 0)) as $w
       | ($w / 65536 | floor), (($w / 256 | floor) % 256), ($w % 256)]
    | .[:$n];
def hdr_uint($b; $at; $len): reduce $b[$at:$at + $len][] as $x (0; . * 256 + $x);
def hdr_decode:
    hdr_bytes as $b
    | if hdr_uint($b; 0; 4) != 478450451
      then error("an interval histogram is not an uncompressed HDR V2 serialization") else . end
    | {low: hdr_uint($b; 16; 8), sigfig: hdr_uint($b; 12; 4),
       # [index, count] of the non-empty buckets: LEB128 varints (7 bits a
       # byte, all 8 in a ninth) of ZigZag i64s, a negative one a run of
       # empty buckets.
       counts: (reduce $b[40:40 + hdr_uint($b; 4; 4)][] as $x ({i: 0, v: 0, m: 1, out: []};
                    (if .m == 72057594037927936 then .v += $x * .m | .done = true
                     else .v += ($x % 128) * .m | .done = ($x < 128) | .m *= 128 end)
                    | if .done | not then .
                      else (if .v % 2 == 0 then .v / 2 else -(.v + 1) / 2 end) as $z
                           | (if $z < 0 then .i -= $z
                              elif $z == 0 then .i += 1
                              else .out += [[.i, $z]] | .i += 1 end)
                           | .v = 0 | .m = 1
                      end)
                | .out)};
def hdr_merge:
    if length == 0 then null
    elif (map([.low, .sigfig]) | unique | length) > 1 then error("the interval histograms differ in their bounds")
    else {low: .[0].low, sigfig: .[0].sigfig,
          counts: ([.[].counts[]] | group_by(.[0]) | map([.[0][0], (map(.[1]) | add)]))}
    end;
# value_at_quantile of hdrhistogram: the highest value equivalent to the
# bucket in which the running count reaches ceil(q * total).
def hdr_quantile($q):
    if . == null then null
    else ((2 * pow(10; .sigfig) | log2 | ceil) - 1) as $half_mag
         | pow(2; $half_mag) as $half
         | (.low | log2 | floor) as $unit
         | ([.counts[][1]] | add) as $total
         | ([($q * $total | ceil), 1] | max) as $target
         | first(foreach .counts[] as $e (0; . + $e[1]; if . >= $target then $e[0] else empty end)) as $i
         | (($i / $half | floor) - 1) as $bucket
         | if $bucket < 0 then ($i + 1) * pow(2; $unit) - 1
           else (($i % $half) + $half + 1) * pow(2; $bucket + $unit) - 1 end
    end;
def step_rule:
    test("^emit lag p99(\\.9)? [0-9.]+ us over [0-9]+ sends ") or test("^mock write lag p99 [0-9.]+ us is not below ")
    or test("^[0-9]+ of [0-9]+ requests after the warmup failed ") or test("^[0-9]+ stale keep-alive retries for ");
def us1: . / 100 | round / 10;
def per_us: if . == null then null else . / 1000 end;
.loadgen.ramp as $r
| if $r == null then null
  else $r.stop_p99_ns as $lim
       | .warmup_intervals as $w
       | .params.ramp_step_s as $len
       # As loadgen converts --max-mock-write-lag-us (dist::seconds_to_ns).
       | (.params.common.max_mock_write_lag_us / 1e6 * 1e9 | round) as $mwl_lim
       | (.summary | has("mock_write_lag")) as $markers
       | .intervals as $iv
       | [$r.steps[] as $s
          | [$iv[] | select(.index >= $w + ($s.step - 1) * $len and .index < $w + $s.step * $len)] as $in
          | ([$in[] | .histograms.emit_lag | select(. != null) | hdr_decode] | hdr_merge) as $emit
          | ([$in[] | .histograms.mock_write_lag | select(. != null) | hdr_decode] | hdr_merge) as $mwl
          | ($emit | hdr_quantile(0.99)) as $e99
          | ($emit | hdr_quantile(0.999)) as $e999
          | ($mwl | hdr_quantile(0.99)) as $m99
          | (reduce $in[] as $x ({requests: 0, failures: 0, stale: 0};
                .requests += $x.requests
                | reduce ($x.errors | to_entries[]) as $e (.;
                      if $e.key == "stale_retry" then .stale += $e.value else .failures += $e.value end))) as $k
          | {step: $s.step, rate: $s.rate, requests: $s.requests, errors: $s.errors, censored: $s.censored,
             p99_us: ($s.p99_ns / 1000), emit_lag_p99_us: ($e99 | per_us), emit_lag_p999_us: ($e999 | per_us),
             mock_write_lag_p99_us: ($m99 | per_us), interval_failures: $k.failures,
             interval_stale_retries: $k.stale,
             reasons: [
                if $s.requests == 0 then "no request completed" else empty end,
                if $s.errors != 0 then "\($s.errors) failed request(s)" else empty end,
                if $s.p99_ns > $lim then "p99 \($s.p99_ns | us1) us above \($lim | us1) us" else empty end,
                if $e99 != null and $e99 >= 10000
                then "emit lag p99 \($e99 | us1) us, loadgen limit 10 us" else empty end,
                if $e999 != null and $e999 >= 1000000
                then "emit lag p99.9 \($e999 | us1) us, loadgen limit 1000 us" else empty end,
                if $m99 != null and $m99 >= $mwl_lim
                then "mock write lag p99 \($m99 | us1) us, limit \($mwl_lim | us1) us"
                elif $m99 == null and $markers then "no mock write lag samples"
                else empty end,
                if $k.requests + $k.failures > 0 and $k.failures / ($k.requests + $k.failures) > 0.001
                then "\($k.failures) of \($k.requests + $k.failures) requests in its intervals failed, loadgen limit 0.1%"
                else empty end,
                if $k.stale > 0.001 * $k.requests
                then "\($k.stale) stale keep-alive retries for \($k.requests) requests in its intervals, loadgen limit 0.1%"
                else empty end]}] as $steps
       | (reduce $steps[] as $s ({done: false, ok: []};
            if .done or ($s.reasons | length) > 0 then .done = true else .ok += [$s] end)
          | .ok) as $ok
       | [.validity.reasons[] | select(step_rule | not)] as $run_reasons
       | {stop_p99_ns: $lim, steps_judged: ($r.steps | length), stopped_by: $r.stopped_by,
          loadgen_max_sustainable_rate: $r.max_sustainable_rate,
          max_sustainable_rate: ($ok | last | .rate), last_step: ($ok | last | .step),
          ended_by: ([$steps[] | select((.reasons | length) > 0) | {step, reasons}] | first),
          valid: (($run_reasons | length) == 0), run_reasons: $run_reasons,
          judged_per_step: [.validity.reasons[] | select(step_rule)],
          steps: $steps}
  end
'

# perf stat counts from "event<TAB>value" lines. Input: raw text.
# shellcheck disable=SC2016
prog_perf_evidence='
split("\n") | map(select(length > 0) | split("\t")) as $rows
| ($rows | map({(.[0]): (.[1] | tonumber)}) | add // {}) as $counts
| {status: "ran", reason: null, counts: $counts,
   per_request: (if $req > 0 then ($counts | map_values(. / $req)) else null end)}
'

# Gated verdict counts: pass fail uncertain missing.
# shellcheck disable=SC2016
prog_counts='
last_by(.id) | [.[] | select(.gated)] as $g
| [([$g[] | select(.verdict == "PASS")] | length), ([$g[] | select(.verdict == "FAIL")] | length),
   ([$g[] | select(.verdict == "UNCERTAIN")] | length), ([$g[] | select(.verdict == "MISSING")] | length)]
| map(tostring) | join(" ")
'

# shellcheck disable=SC2016
prog_manifest='
def kv: split("\n") | map(select(test("=")) | capture("^(?<k>[^=]+)=(?<v>.*)$") | {(.k): .v}) | add;
($verdicts | last_by(.id)) as $final
| {run_id: $run_id, session: $session, status: $status, host: $host, started: $started, finished: $finished,
   host_ready: $host_ready, exit_status: ($exit_status | tonumber? // null),
   source: (($sync | kv) + {patch: $patch}), knobs: ($knobs | kv),
   binaries: ($binaries | split("\n") | map(select(length > 0) | split("  ")
              | {sha256: .[0], version: .[1], path: .[2]})),
   arms: ($arms | split(" ")),
   virtual_key: (if $key_sha256 == "" then null else {name: $key_name, sha256: $key_sha256} end),
   cpu_method: "per CPU: busy = 1 - (idle + iowait) / wall clock, steal = /proc/stat steal ticks / wall clock, softirq as /proc/softirqs events; per process and SUT: utime + stime over the window",
   selfcheck: $selfcheck, selfcheck_detail: $selfcheck_detail,
   perf: {status: $perf_status, reason: $perf_reason, events: $perf_events, hitm_events: $perf_hitm},
   invalid_runs: $invalid, retries: $retries, extensions: $extensions, failed_runs: $failed,
   evaluation_errors: $evaluation_errors,
   gated: {pass: ([$final[] | select(.gated and .verdict == "PASS")] | length),
           fail: ([$final[] | select(.gated and .verdict == "FAIL")] | length),
           uncertain: ([$final[] | select(.gated and .verdict == "UNCERTAIN")] | length),
           missing: ([$final[] | select(.gated and .verdict == "MISSING")] | length)},
   decision: ([$final[] | select(.kind == "decision")] | last),
   verdicts: $final, compares: ($compares | last_by(.name)), runs: $runs,
   processes: "processes.jsonl", session_log: "session.log", summary: "summary.txt"}
'

jq_programs=(prog_select_reps prog_compare_record prog_compare_lines prog_check_delta prog_ramp_pairs
    prog_ramp_verdict prog_ramp_vcpu prog_reuse prog_cpu_arms prog_cpu_pair prog_memory prog_perf_report
    prog_e11_latency prog_e11_cpu prog_decide prog_record_run prog_record_missing prog_sut_evidence
    prog_reuse_evidence prog_ramp_evidence prog_perf_evidence prog_counts prog_manifest)

# Compiles every program with each variable it names bound to a dummy, so
# that a mistake in one ends the session at its start instead of in the
# evaluation of a block hours later.
preflight_jq() {
    local prog_name name
    local -a args names
    for prog_name in "${jq_programs[@]}"; do
        mapfile -t names < <(grep -o '\$[A-Za-z_][A-Za-z0-9_]*' <<<"${!prog_name}" | sort -u | sed 's/^\$//')
        args=()
        for name in "${names[@]}"; do
            [[ "$name" == ENV || "$name" == ARGS || "$name" == __loc__ ]] || args+=(--arg "$name" 0)
        done
        jq -n "${args[@]}" "$jq_defs def _preflight: ${!prog_name}; empty" >/dev/null ||
            die "jq program $prog_name does not compile (jq $(jq --version))"
    done
}

# ---------------------------------------------------------------- processes

# Starts a process pinned with taskset in the background.
start_proc() {
    local name="$1" cpus="$2" logfile="$3"
    shift 3
    taskset -c "$cpus" "$@" >"$logfile" 2>&1 &
    pids[$name]=$!
}

# SIGTERM, then a second SIGTERM (floor and Brisk abandon their graceful
# drain on the second signal), then SIGKILL.
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

# Waits until an HTTP GET on the URL returns 2xx; the extra arguments go to
# curl.
wait_ready() {
    local name="$1" url="$2" timeout_s="$3" log_hint="$4" i
    shift 4
    for ((i = 0; i < timeout_s * 10; i++)); do
        alive "$name" || die "$name exited during startup; see $log_hint"
        if curl -sf -o /dev/null --max-time 1 "$@" "$url"; then
            return 0
        fi
        sleep 0.1
    done
    die "$name not ready at $url after $timeout_s s; see $log_hint"
}

# The text with the virtual key replaced.
redact() {
    local text="$*"
    if [[ -n "$bench_key" ]]; then
        text="${text//"$bench_key"/"bk-<redacted>"}"
    fi
    printf '%s' "$text"
}

# Appends the identity and per-thread CPU affinity of a running process.
fingerprint_proc() {
    local name="$1" tag="$2" pid="${pids[$1]:-}" threads="" task cmdline
    [[ -n "$pid" && -d "/proc/$pid" ]] || return 0
    for task in /proc/"$pid"/task/*; do
        threads+="${task##*/}"$'\t'"$(cat "$task/comm" 2>/dev/null)"$'\t'"$(awk '/^Cpus_allowed_list:/ {print $2}' "$task/status" 2>/dev/null)"$'\n'
    done
    cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null)" || return 0
    jq -nc --arg name "$name" --arg tag "$tag" --argjson pid "$pid" \
        --arg cmdline "$(redact "$cmdline")" \
        --arg exe "$(readlink "/proc/$pid/exe")" --arg threads "$threads" \
        '{name: $name, tag: $tag, pid: $pid, cmdline: ($cmdline | rtrimstr(" ")), exe: $exe,
          threads: ($threads | split("\n") | map(select(length > 0) | split("\t")
                    | {tid: (.[0] | tonumber), comm: .[1], cpus: .[2]}))}' \
        >>"$run_dir/processes.jsonl"
}

# Fails unless every thread of the SUT is confined to SUT_CPUS; a run on the
# wrong cores must not start. floor checks this itself, Brisk relies on
# taskset.
check_sut_affinity() {
    local pid="${pids[sut]}" want task got
    want="$(expand_cpus "$SUT_CPUS")"
    for task in /proc/"$pid"/task/*; do
        got="$(awk '/^Cpus_allowed_list:/ {print $2}' "$task/status" 2>/dev/null)" || continue
        [[ -n "$got" ]] || continue
        [[ "$(expand_cpus "$got")" == "$want" ]] ||
            die "thread ${task##*/} of the process under test runs on CPUs $got instead of $SUT_CPUS"
    done
}

# Invoked by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
    local status=$?
    # A second Ctrl-C, a TERM or a vanished log reader must not cut the
    # clean-up short and leave a SUT or a mock running.
    trap '' INT TERM HUP PIPE
    trap - EXIT
    set +e
    if [[ -n "$sampler_pid" ]]; then
        kill "$sampler_pid" 2>/dev/null
        wait "$sampler_pid" 2>/dev/null
    fi
    local name
    for name in loadgen sut mock-sc mock-plain mock-tls; do
        stop_proc "$name"
    done
    for name in "${!pids[@]}"; do
        stop_proc "$name"
    done
    [[ -n "$certs_dir" ]] && rm -rf "$certs_dir"
    rm -rf "$stage_dir"
    if ((!manifest_written)) && [[ -d "$run_dir" ]]; then
        write_manifest aborted "$status"
    fi
    if ((status != 0)); then
        log "session ended with status $status; results so far in $run_dir"
    fi
    exit "$status"
}

# ---------------------------------------------------------------- evidence

# Wall-clock time, per-CPU idle/steal/total ticks, per-CPU softirq event
# counts and per-process CPU ticks (run-m0.sh explains the method).
cpu_snapshot() {
    local out="$1" name pid stat rest
    local -a f
    shift
    {
        printf 'time %s\n' "$(date +%s.%N)"
        # user nice system idle iowait irq softirq steal ($2..$9); guest time
        # is already inside user and nice.
        awk '/^cpu[0-9]+ / { print "cpu", $1, $5 + $6, $9, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 }' /proc/stat
        awk 'NR == 1 { n = NF; for (i = 1; i <= NF; i++) cpu[i] = tolower($i); next }
             $1 == "NET_RX:" || $1 == "NET_TX:" || $1 == "TIMER:" {
                 k = tolower(substr($1, 1, length($1) - 1))
                 for (i = 1; i <= n; i++) print "sirq", cpu[i], k, $(i + 1)
             }' /proc/softirqs
        for name in "$@"; do
            pid="${pids[$name]:-}"
            [[ -n "$pid" && -r "/proc/$pid/stat" ]] || continue
            stat="$(<"/proc/$pid/stat")" || continue
            # Fields after "pid (comm) "; utime and stime are fields 14 and 15.
            rest="${stat##*) }"
            read -r -a f <<<"$rest"
            printf 'proc %s %s\n' "$name" "$((f[11] + f[12]))"
        done
        for name in "${!foreign_pids[@]}"; do
            pid="${foreign_pids[$name]}"
            [[ -r "/proc/$pid/stat" ]] || continue
            stat="$(<"/proc/$pid/stat")" || continue
            rest="${stat##*) }"
            read -r -a f <<<"$rest"
            printf 'proc %s %s\n' "$name" "$((f[11] + f[12]))"
        done
    } >"$out"
}

# Utilisation between two snapshots as JSON (run-m0.sh).
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

# VmRSS of a process in KiB; empty once it is gone.
vmrss_kib() {
    awk '/^VmRSS:/ { print $2 }' "/proc/$1/status" 2>/dev/null || true
}

# Resets the statistics of every running mock. Returns non-zero instead of
# ending the session, since the sampler runs in the background.
reset_live_mocks() {
    local name
    for name in mock-plain mock-tls mock-sc; do
        alive "$name" || continue
        mock_get "$name" /__bench/reset -X POST >/dev/null 2>&1 || return 1
    done
}

# Background sampler of one run's measurement window. After <offset>
# seconds it resets the mock statistics (marker <tag>.reset-ok), records the
# load generator's thread placement, snapshots CPU and interrupt counters and,
# with <perf>, starts perf stat on the SUT for the window; <mid> seconds later
# it reads the SUT's VmRSS (0: never); <window> seconds after the start it
# snapshots again (0: the caller does, when the load generator exits). Its
# sleeps are waited on so that a TERM ends them too, instead of leaving a
# sleep that holds the session log's pipe open.
sample_window() {
    local tag="$1" offset="$2" window="$3" mid="$4" perf="$5" nap_pid="" perf_pid="" rest
    local logs="$run_dir/logs"
    shift 5
    trap 'if [[ -n "$nap_pid" ]]; then kill "$nap_pid" 2>/dev/null; fi
          if [[ -n "$perf_pid" ]]; then kill "$perf_pid" 2>/dev/null; fi
          exit 0' TERM
    sleep "$offset" &
    nap_pid=$!
    wait "$nap_pid"
    if reset_live_mocks; then
        : >"$logs/$tag.reset-ok"
    fi
    fingerprint_proc loadgen "$tag"
    cat /proc/interrupts >"$logs/$tag.irq-a"
    cpu_snapshot "$logs/$tag.cpu-a" "$@"
    if ((window == 0)); then
        return 0
    fi
    if ((perf)) && [[ -n "${pids[sut]:-}" ]]; then
        perf stat -x, -e "$perf_events" -p "${pids[sut]}" -o "$logs/$tag.perf.csv" -- sleep "$window" \
            >/dev/null 2>"$logs/$tag.perf.err" &
        perf_pid=$!
    fi
    rest="$window"
    if ((mid > 0)) && [[ -n "${pids[sut]:-}" ]]; then
        sleep "$mid" &
        nap_pid=$!
        wait "$nap_pid"
        vmrss_kib "${pids[sut]}" >"$logs/$tag.rss-mid"
        rest=$((window - mid))
    fi
    sleep "$rest" &
    nap_pid=$!
    wait "$nap_pid"
    cpu_snapshot "$logs/$tag.cpu-b" "$@"
    cat /proc/interrupts >"$logs/$tag.irq-b"
    if [[ -n "$perf_pid" ]]; then
        wait "$perf_pid" || true
    fi
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
        mock-tls) curl -sf --max-time 5 --cacert "$ca" "$@" "https://127.0.0.1:$MOCK_TLS_PORT$path" ;;
        mock-sc)
            if ((sc_tls)); then
                curl -sf --max-time 5 --cacert "$ca" "$@" "https://127.0.0.1:$SC_PORT$path"
            else
                curl -sf --max-time 5 "$@" "http://127.0.0.1:$SC_PORT$path"
            fi
            ;;
        *) die "unknown mock $name" ;;
    esac
}

# The CPUs of a list as a JSON array.
cpu_array() {
    jq -nc --arg c "$(expand_cpus "$1")" '$c | split(" ") | map(tonumber)'
}

# Steal over a run's window (run-m0.sh).
steal_summary() {
    jq -nc --argjson cpu "$1" --argjson roles "$2" '
        if $cpu == null then null
        else [$cpu.cpus[].steal_pct | numbers] as $all
             | if ($all | length) == 0 then null
               else {max_pct: ($all | max), mean_pct: ($all | add / length * 10 | round / 10),
                     by_role_max_pct: ($roles | map_values([.[] as $c | $cpu.cpus["cpu\($c)"].steal_pct
                                                            | numbers] | max))}
               end
        end'
}

# Shard balance of one mock (run-m0.sh).
mock_balance() {
    jq -nc --argjson m "$1" --arg n "$2" '
        def ratio(f): [.[] | f | numbers] as $v
            | if ($v | length) == 0 then null
              else ($v | add / length) as $mean
                   | if $mean > 0 then ($v | max) / $mean * 1000 | round / 1000 else null end
              end;
        $m[$n].shards as $s
        | if ($s | type) != "array" then null
          else {mock: $n, shards: ($s | length),
                requests_max_over_mean: ($s | ratio(.requests)),
                chunks_max_over_mean: ($s | ratio(.chunks))}
          end'
}

# The mock that serves an arm: config P's direct arm reaches the plaintext
# mock and its SUT the TLS one; direct-tls always reaches the TLS one.
arm_mock() {
    local scen="$1" config="$2" arm="$3"
    if [[ "$scen" == sc ]]; then
        echo mock-sc
    elif [[ "$arm" == direct-tls ]]; then
        echo mock-tls
    elif [[ "$config" == N || ("$config" == P && "$arm" == direct) ]]; then
        echo mock-plain
    else
        echo mock-tls
    fi
}

# CPU and memory evidence of the SUT of the run in progress.
sut_evidence() {
    local tag="$1" result="$2" doc=/dev/null mid="" t_a="" k_a="" t_b="" k_b=""
    local a="$run_dir/logs/$tag.cpu-a" b="$run_dir/logs/$tag.cpu-b"
    # shellcheck disable=SC2016
    local extract='$1 == "time" { t = $2 } $1 == "proc" && $2 == "sut" { k = $3 } END { print t, k }'
    if [[ -f "$a" && -f "$b" ]]; then
        read -r t_a k_a <<<"$(awk "$extract" "$a")"
        read -r t_b k_b <<<"$(awk "$extract" "$b")"
    fi
    if [[ -f "$run_dir/logs/$tag.rss-mid" ]]; then
        mid="$(<"$run_dir/logs/$tag.rss-mid")"
    fi
    [[ -f "$result" ]] && doc="$result"
    jq -nc --slurpfile r "$doc" --arg pid "${run_ctx[sut_pid]}" --arg ta "$t_a" --arg ka "$k_a" \
        --arg tb "$t_b" --arg kb "$k_b" --arg idle "${run_ctx[idle_rss]}" --arg mid "$mid" \
        --argjson hz "$clk_tck" --argjson streams "$S1_CONCURRENCY" "$prog_sut_evidence"
}

# perf stat counts of one run as JSON.
perf_evidence() {
    local file="$1" requests="$2"
    if [[ ! -s "$file" ]]; then
        jq -nc --arg reason "perf stat wrote no counts; see logs/$(basename "${file%.csv}").err" \
            '{status: "failed", reason: $reason, counts: null, per_request: null}'
        return 0
    fi
    awk -F, '$1 ~ /^[0-9.]+$/ && $3 != "" { printf "%s\t%s\n", $3, $1 }' "$file" |
        jq -Rsc --argjson req "$requests" "$prog_perf_evidence"
}

# Moves a staged result into results/, with the key replaced in the
# recorded header.
finalize_result() {
    local staged="$1" final="$2"
    [[ -f "$staged" ]] || return 0
    if [[ -n "$bench_key" ]]; then
        jq '.params.common.headers |= map(if (.name | ascii_downcase) == "authorization"
                                          then .value = "Bearer <redacted>" else . end)' \
            "$staged" >"$staged.redacted" || die "redacting $(basename "$staged") failed"
        if grep -qF -- "$bench_key" "$staged.redacted"; then
            die "the virtual key is still in $(basename "$staged") after redaction; not moving it to results/"
        fi
        mv "$staged.redacted" "$final"
        rm -f "$staged"
    else
        mv "$staged" "$final"
    fi
}

# Replaces the key in a log file that should never have had it.
scrub_file() {
    [[ -n "$bench_key" && -f "$1" ]] || return 0
    if grep -qF -- "$bench_key" "$1"; then
        sed -i "s/$bench_key/bk-<redacted>/g" "$1"
        log "WARNING: $(basename "$1") contained the virtual key; replaced"
    fi
}

# Appends the runs.jsonl record of the run in progress.
record_run() {
    local result="$1" rc="$2" started="$3" cpu="$4" net="$5" mockstats="$6" steal="$7" balance="$8"
    local harness="$9" ctx
    ctx="$(jq -nc --arg scen "${run_ctx[scen]}" --arg config "${run_ctx[config]}" --arg var "${run_ctx[var]}" \
        --arg arm "${run_ctx[arm]}" --argjson rep "${run_ctx[rep]}" --argjson attempt "${run_ctx[attempt]}" \
        --argjson order "${run_ctx[order]}" --arg kind "${run_ctx[kind]}" --arg tag "${run_ctx[tag]}" \
        --arg pair "${run_ctx[pair_id]}" --argjson rc "$rc" --arg started "$started" \
        '{scen: $scen, config: $config, var: $var, arm: $arm, rep: $rep, attempt: $attempt, order: $order,
          kind: $kind, tag: $tag, pair_id: (if $pair == "" then null else $pair end),
          loadgen_rc: $rc, started: $started}')"
    if [[ -f "$result" ]]; then
        jq -c --argjson ctx "$ctx" --argjson h "$harness" --arg file "results/${result##*/}" \
            --argjson cpu "$cpu" --argjson net "$net" --argjson mock "$mockstats" \
            --argjson steal "$steal" --argjson balance "$balance" "$prog_record_run" \
            "$result" >>"$run_dir/runs.jsonl"
    else
        jq -nc --argjson ctx "$ctx" --argjson h "$harness" --argjson cpu "$cpu" --argjson net "$net" \
            --argjson mock "$mockstats" --argjson steal "$steal" --argjson balance "$balance" \
            "$prog_record_missing" >>"$run_dir/runs.jsonl"
    fi
}

# Runs brisk-loadgen (lg_args) for the run described by run_ctx, samples its
# window, stops the SUT and records the run. Sets run_ok to 1 for a valid run
# with no failure of loadgen or the SUT, else to 0.
execute_run() {
    local tag="${run_ctx[tag]}" scen="${run_ctx[scen]}" warmup="${run_ctx[warmup]}"
    local measure="${run_ctx[measure]}" open="${run_ctx[open_window]}" perf="${run_ctx[perf]}"
    local staged="${run_ctx[staged]}" result="${run_ctx[result]}" logs="$run_dir/logs"
    local started rc window=0 mid=0 i name stats mockstats="{}" sut_died=0 failed=0
    local cpu=null net reset_ok=false sut=null reuse=null ramp=null perf_ev=null
    local roles="$roles_scenario" steal balance harness valid
    local -a reasons=() sampled=(mock-plain mock-tls mock-sc sut loadgen)
    run_ok=1
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    net_snapshot "$logs/$tag.net-a"
    log "run $tag: $(redact "${lg_args[*]}")"
    taskset -c "$LOADGEN_CPUS" "$loadgen" "${lg_args[@]}" >"$logs/$tag.out" 2>"$logs/$tag.err" &
    pids[loadgen]=$!
    if ((!open)); then
        window=$((measure - 2))
    fi
    if [[ "$scen" == s1 && -n "${pids[sut]:-}" ]]; then
        mid=$((measure / 2 - 1))
    fi
    sample_window "$tag" "$((warmup + 1))" "$window" "$mid" "$perf" "${sampled[@]}" &
    sampler_pid=$!
    if wait "${pids[loadgen]}"; then rc=0; else rc=$?; fi
    unset "pids[loadgen]"
    # A fixed window ends about a second before loadgen does; the sampler
    # still running after a few more seconds means loadgen ended early.
    for ((i = 0; i < 50; i++)); do
        kill -0 "$sampler_pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$sampler_pid" 2>/dev/null; then
        kill "$sampler_pid" 2>/dev/null || true
    fi
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
    # An open window (a ramp, which stops at its first failing step) ends
    # with the load generator.
    if ((open)) && [[ -f "$logs/$tag.cpu-a" && ! -f "$logs/$tag.cpu-b" ]]; then
        cpu_snapshot "$logs/$tag.cpu-b" "${sampled[@]}"
        cat /proc/interrupts >"$logs/$tag.irq-b"
    fi
    if [[ -f "$logs/$tag.cpu-a" && -f "$logs/$tag.cpu-b" ]]; then
        cpu="$(cpu_usage "$logs/$tag.cpu-a" "$logs/$tag.cpu-b")"
    fi
    net_snapshot "$logs/$tag.net-b"
    net="$(net_delta "$logs/$tag.net-a" "$logs/$tag.net-b")"
    for name in mock-plain mock-tls mock-sc; do
        alive "$name" || continue
        stats="$(mock_get "$name" /__bench/stats)" || die "reading the $name statistics failed"
        mockstats="$(jq -c --arg n "$name" --argjson s "$stats" '. + {($n): $s}' <<<"$mockstats")"
    done
    if [[ -n "${pids[sut]:-}" ]]; then
        alive sut || sut_died=1
        stop_proc sut
    fi
    if [[ -f "$logs/$tag.reset-ok" ]]; then
        reset_ok=true
    fi
    finalize_result "$staged" "$result"
    scrub_file "$logs/$tag.out"
    scrub_file "$logs/$tag.err"
    sed -n '/^validity/,$p' "$logs/$tag.out" | sed 's/^/    /'

    # selfcheck exits non-zero for a FAIL verdict, which run_selfcheck
    # reports; only a missing result makes it a broken run.
    if ((rc != 0)) && [[ "$scen" != sc || ! -f "$result" ]]; then
        failed=1
        reasons+=("brisk-loadgen exited with status $rc (logs/$tag.err)")
    fi
    if ((sut_died)); then
        failed=1
        reasons+=("the process under test exited during the run (logs/$tag.sut.log)")
    fi
    if ((failed)); then
        failed_runs=$((failed_runs + 1))
    fi
    # compare reads the repetitions of S1, S2 and S3; a run it would refuse
    # is rerun here instead of costing the block its comparisons.
    if [[ "${run_ctx[kind]}" == run && "$scen" != ramp && -f "$result" ]]; then
        local tail_reason
        tail_reason="$(tail_screen_reason "$scen" "$result")"
        if [[ -n "$tail_reason" ]]; then
            reasons+=("$tail_reason")
        fi
    fi
    if [[ -n "${run_ctx[sut_pid]}" ]]; then
        sut="$(sut_evidence "$tag" "$result")"
        if [[ "$(jq -r '.cpu_s' <<<"$sut")" == null ]]; then
            reasons+=("the CPU window of the process under test was not sampled")
        fi
    fi
    if [[ "${run_ctx[check_reuse]}" == 1 ]]; then
        reuse="$(jq -nc --argjson m "$mockstats" --arg n "${run_ctx[arm_mock]}" --argjson ok "$reset_ok" \
            --argjson min "$MIN_REUSE" "$jq_defs$prog_reuse_evidence")"
        # A ramp raises the load step by step, so new upstream connections
        # belong to it; only the steady load of S1, S2 and S3 is held to the
        # reuse limit.
        if [[ "$scen" != ramp && "$(jq -r '.ok' <<<"$reuse")" != true ]]; then
            reasons+=("$(jq -r '.reason' <<<"$reuse")")
        fi
    fi
    if [[ "$scen" == ramp && -f "$result" ]]; then
        ramp="$(jq -c "$prog_ramp_evidence" "$result")"
    fi
    if ((perf)); then
        perf_ev="$(perf_evidence "$logs/$tag.perf.csv" "$(jq -r '.requests // 0' <<<"$sut")")"
    fi
    [[ "$scen" == sc ]] && roles="$roles_sc"
    steal="$(steal_summary "$cpu" "$roles")"
    balance="$(mock_balance "$mockstats" "${run_ctx[arm_mock]}")"
    log "  $tag: steal $(jq -r 'if . == null then "n/a"
                                else "max \(.max_pct)%, mean \(.mean_pct)% (\(.by_role_max_pct | to_entries
                                     | map("\(.key) \(.value // "n/a")%") | join(", ")))" end' <<<"$steal");" \
        "mock shard max/mean $(jq -r 'if . == null then "n/a"
                                      else "requests \(.requests_max_over_mean // "n/a"), chunks \(.chunks_max_over_mean // "n/a") (\(.mock), \(.shards) shard(s))" end' <<<"$balance")"
    if [[ "$sut" != null ]]; then
        log "  $tag: SUT $(jq -r "$jq_defs"'"CPU \(.cpu_s | fixed3) s over \(.window_s | fixed1) s: \(.cpu_us_per_chunk | fixed3) us/chunk, \(.cpu_us_per_request | fixed3) us/request; RSS idle \(.idle_rss_kib | mib | fixed1) MiB, middle \(.mid_rss_kib | mib | fixed1) MiB"' <<<"$sut")"
    fi
    if [[ "$reuse" != null ]]; then
        log "  $tag: reuse $(jq -r "$jq_defs"'"\(.rate | pct4) at \(.mock) (\(.accepts) accepts, \(.requests) requests)"' <<<"$reuse")"
    fi
    if [[ "$ramp" != null ]]; then
        log "  $tag: ramp $(jq -r "$jq_defs"'"sustainable \(.max_sustainable_rate | rate) req/s (step \(.last_step // "n/a") of \(.steps_judged) judged; \(.stopped_by))"
            + (if .ended_by == null then "" else "; step \(.ended_by.step) not sustainable: \(.ended_by.reasons | join(", "))" end)
            + (if (.judged_per_step | length) == 0 then ""
               else "; judged per step instead of over the whole ramp: \(.judged_per_step | join("; "))" end)' <<<"$ramp")"
    fi
    harness="$(jq -nc --argjson failed "$failed" --argjson reuse "$reuse" --argjson sut "$sut" \
        --argjson ramp "$ramp" --argjson perf "$perf_ev" \
        '{failed: ($failed == 1), reasons: $ARGS.positional, reuse: $reuse, sut: $sut, ramp: $ramp, perf: $perf}' \
        --args "${reasons[@]}")"
    record_run "$result" "$rc" "$started" "$cpu" "$net" "$mockstats" "$steal" "$balance" "$harness"
    valid="$(tail -n 1 "$run_dir/runs.jsonl" | jq -r '.valid')"
    if [[ "$valid" == true ]]; then
        log "  $tag: VALID"
    else
        run_ok=0
        if [[ "${run_ctx[kind]}" != perf ]]; then
            invalid_runs=$((invalid_runs + 1))
        fi
        log "  $tag: INVALID: $(tail -n 1 "$run_dir/runs.jsonl" | jq -r '.reasons | join("; ")')"
    fi
    if ((failed)); then
        run_ok=0
    fi
}

# ---------------------------------------------------------------- arms

sut_port() {
    if [[ "$1" == T ]]; then echo "$SUT_T_PORT"; else echo "$SUT_P_PORT"; fi
}

# Whether an arm runs without a process under test: direct, and direct-tls,
# the tool ramp of config P (tool_ramp_arm).
direct_arm() {
    [[ "$1" == direct || "$1" == direct-tls ]]
}

# Whether loadgen reaches an arm over TLS: every arm of config T, and
# direct-tls.
arm_over_tls() {
    [[ "$1" == T || "$2" == direct-tls ]]
}

target_url() {
    local config="$1" arm="$2" scheme=http port
    if arm_over_tls "$config" "$arm"; then
        scheme=https
    fi
    if ! direct_arm "$arm"; then
        port="$(sut_port "$config")"
    elif [[ "$scheme" == https ]]; then
        port="$MOCK_TLS_PORT"
    else
        port="$MOCK_PLAIN_PORT"
    fi
    echo "$scheme://127.0.0.1:$port"
}

# router splice stream_usage commit_hold of a Brisk arm; the defaults except
# where the arm is named after the setting it changes.
brisk_settings() {
    case "$1" in
        brisk | brisk-axum | brisk-segments | brisk-hold2s) echo "axum segments inject 2s" ;;
        brisk-pt) echo "axum segments passthrough 2s" ;;
        brisk-match) echo "match segments inject 2s" ;;
        brisk-concat) echo "axum concat inject 2s" ;;
        brisk-hold0s) echo "axum segments inject 0s" ;;
        *) die "no Brisk settings for arm $1" ;;
    esac
}

brisk_config_path() {
    echo "$run_dir/brisk/$1-$2.toml"
}

# A TOML basic string.
toml_string() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    printf '"%s"' "$s"
}

# Renders brisk-bench.toml.in for one Brisk arm and config.
render_brisk_config() {
    local arm="$1" config="$2" out="$3" template text key router splice usage hold
    local -A vals=()
    read -r router splice usage hold <<<"$(brisk_settings "$arm")"
    template="$(<"$brisk_template")"
    vals[LISTEN]="127.0.0.1:$(sut_port "$config")"
    vals[WORKERS]="$SUT_WORKERS"
    vals[SERVER_TLS_TABLE]=""
    if [[ "$config" == T ]]; then
        vals[SERVER_TLS_TABLE]="[server.tls]"$'\n'"cert = $(toml_string "$certs_dir/server.pem")"$'\n'"key = $(toml_string "$certs_dir/server.key")"
    fi
    vals[COMMIT_HOLD]="$hold"
    vals[ROUTER]="$router"
    vals[SPLICE]="$splice"
    vals[KEY_NAME]="$key_name"
    vals[KEY_SHA256]="$key_sha256"
    vals[UPSTREAM_KEY_ENV]="$upstream_key_env"
    vals[STREAM_USAGE]="$usage"
    vals[MODEL_MAP]="{}"
    if [[ "$SESSION" == e2 ]]; then
        vals[MODEL_MAP]="{ $(toml_string "$loadgen_model") = $(toml_string "$e2_mapped_model") }"
    fi
    if [[ "$config" == N ]]; then
        vals[UPSTREAM_BASE_URL]="http://127.0.0.1:$MOCK_PLAIN_PORT/v1"
        vals[CA_FILE_LINE]=""
    else
        vals[UPSTREAM_BASE_URL]="https://127.0.0.1:$MOCK_TLS_PORT/v1"
        vals[CA_FILE_LINE]="ca_file = $(toml_string "$ca")"
    fi
    text="$template"
    for key in "${!vals[@]}"; do
        [[ "$template" == *"@$key@"* ]] ||
            die "$brisk_template has no @$key@ placeholder; it and run-m1.sh disagree"
        text="${text//"@$key@"/"${vals[$key]}"}"
    done
    if [[ "$text" =~ @[A-Z0-9_]+@ ]]; then
        die "$brisk_template: run-m1.sh has no value for ${BASH_REMATCH[0]}"
    fi
    printf '%s\n' "$text" >"$out"
}

# Starts the process under test of an arm and waits until it is ready.
start_sut() {
    local arm="$1" config="$2" tag="$3" log_file="$run_dir/logs/$3.sut.log" scheme=http
    local -a args curl_ca=()
    if [[ "$config" == T ]]; then
        scheme=https
        curl_ca=(--cacert "$ca")
    fi
    if [[ "$arm" == floor-* ]]; then
        args=(--mode "${arm#floor-}" --listen "127.0.0.1:$(sut_port "$config")" --cpu-list "$SUT_CPUS"
            --workers "$SUT_WORKERS")
        if [[ "$config" == N ]]; then
            args+=(--upstream "http://127.0.0.1:$MOCK_PLAIN_PORT")
        else
            args+=(--upstream "https://127.0.0.1:$MOCK_TLS_PORT" --upstream-ca "$ca")
        fi
        if [[ "$config" == T ]]; then
            args+=(--tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key")
        fi
        start_proc sut "$SUT_CPUS" "$log_file" "$floor" "${args[@]}"
        wait_ready sut "$scheme://127.0.0.1:$(sut_port "$config")/v1/models" 10 "logs/$tag.sut.log" "${curl_ca[@]}"
    else
        start_proc sut "$SUT_CPUS" "$log_file" env "$upstream_key_env=$upstream_key_value" \
            "$brisk" serve --config "$(brisk_config_path "$arm" "$config")"
        wait_ready sut "$scheme://127.0.0.1:$(sut_port "$config")/readyz" "$READY_TIMEOUT_S" \
            "logs/$tag.sut.log" "${curl_ca[@]}"
    fi
    check_sut_affinity
    fingerprint_proc sut "$tag"
}

stream_shape() {
    lg_args+=(--chunk-rate "$S1_CHUNK_RATE" --dur-median "$S1_DUR_MEDIAN" --dur-p99 "$S1_DUR_P99"
        --dur-max "$S1_DUR_MAX" --ttft-us "$1" --chunk-bytes "$S1_CHUNK_BYTES")
}

block_name() {
    echo "$1-$2${3:+-$3}"
}

planned_reps() {
    case "$1" in
        s1) echo "$S1_REPS" ;;
        s2) echo "$S2_REPS" ;;
        s3) echo "$S3_REPS" ;;
        ramp) echo "$RAMP_REPS" ;;
    esac
}

# The two arms under test of a ramp block, baseline first.
ramp_arms() {
    local arm
    local -a arms=()
    for arm in ${session_arms[$SESSION]}; do
        [[ "$arm" == direct ]] || arms+=("$arm")
    done
    echo "${arms[*]}"
}

# The arms of a block.
block_arms() {
    local scen="$1" config="$2"
    if [[ "$scen" == ramp ]]; then
        ramp_arms
    elif [[ "$SESSION" == gate && "$scen" == s1 && " $PT_CONFIGS " == *" $config "* ]]; then
        echo "${session_arms[$SESSION]} brisk-pt"
    else
        echo "${session_arms[$SESSION]}"
    fi
}

# The variants of a scenario, one block each: the S3 sizes, the e4 mock TTFTs.
block_vars() {
    local t
    case "$1" in
        s3) printf '%s\n' "${s3_sizes[@]}" ;;
        s1)
            if [[ "$SESSION" == e4 ]]; then
                for t in "${e4_ttfts[@]}"; do
                    echo "ttft${t}us"
                done
            else
                echo ""
            fi
            ;;
        *) echo "" ;;
    esac
}

# The run order of the arms in repetition <rep>: the Latin square rotation
# of the block's arms, brisk-pt next to Brisk (after it in odd repetitions,
# before it in even ones).
rep_order() {
    local rep="$1" arm i k n pt=0
    shift
    local -a base=() order=() with=()
    for arm in "$@"; do
        if [[ "$arm" == brisk-pt ]]; then pt=1; else base+=("$arm"); fi
    done
    n=${#base[@]}
    k=$(((rep - 1) % n))
    for ((i = 0; i < n; i++)); do
        order+=("${base[(k + i) % n]}")
    done
    if ((pt)); then
        for arm in "${order[@]}"; do
            if [[ "$arm" != brisk ]]; then
                with+=("$arm")
            elif ((rep % 2 == 1)); then
                with+=(brisk brisk-pt)
            else
                with+=(brisk-pt brisk)
            fi
        done
        order=("${with[@]}")
    fi
    echo "${order[*]}"
}

# One run of one arm; sets run_ok. kind: run (a repetition), tool (the direct
# ramp of a ramp block), perf (the extra S2 run under perf stat).
run_arm() {
    local scen="$1" config="$2" var="$3" arm="$4" rep="$5" attempt="$6" pos="$7" kind="$8"
    local block base pair_id url seed tls=0 spin="$SPIN_US" lag="$MOCK_WRITE_LAG_LIMIT_US"
    local warmup measure open=0 ttft staged
    block="$(block_name "$scen" "$config" "$var")"
    case "$kind" in
        run) base="$block-$arm-r$rep" pair_id="$block-r$rep" ;;
        tool) base="$block-$arm-tool" pair_id="$block-tool" ;;
        perf) base="$block-$arm-perf" pair_id="$block-perf" ;;
    esac
    ((attempt == 0)) || base+="-a$attempt"
    seed=$((SEED + (rep > 0 ? rep - 1 : 0)))
    if arm_over_tls "$config" "$arm"; then
        tls=1
    fi
    url="$(target_url "$config" "$arm")"
    staged="$stage_dir/$base.json"
    case "$scen" in
        s1)
            ttft="$S1_TTFT_US"
            if [[ -n "$var" ]]; then
                ttft="${var#ttft}"
                ttft="${ttft%us}"
            fi
            lg_args=(stream "$url" --concurrency "$S1_CONCURRENCY")
            stream_shape "$ttft"
            lg_args+=(--warmup-s "$S1_WARMUP_S" --measure-s "$S1_MEASURE_S")
            if [[ "$arm" == direct ]]; then
                lg_args+=(--max-slip-us "$S1_MAX_SLIP_US")
            fi
            warmup="$S1_WARMUP_S" measure="$S1_MEASURE_S"
            ;;
        s2)
            lg_args=(nonstream "$url" --rate "$S2_RATE" --ttft-us "$S2_TTFT_US"
                --resp-bytes "$S2_RESP_BYTES" --prompt-bytes "$S2_PROMPT_BYTES"
                --warmup-s "$S2_WARMUP_S" --measure-s "$S2_MEASURE_S")
            warmup="$S2_WARMUP_S" measure="$S2_MEASURE_S"
            ;;
        ramp)
            lg_args=(nonstream "$url" --ramp-start "$RAMP_START" --ramp-step-pct "$RAMP_STEP_PCT"
                --ramp-step-s "$RAMP_STEP_S" --stop-p99-ms "$RAMP_STOP_P99_MS" --ramp-max-steps "$RAMP_MAX_STEPS"
                --ttft-us "$RAMP_TTFT_US" --resp-bytes "$S2_RESP_BYTES" --prompt-bytes "$S2_PROMPT_BYTES"
                --warmup-s "$RAMP_WARMUP_S")
            warmup="$RAMP_WARMUP_S" measure=0 open=1 lag="$RAMP_MOCK_WRITE_LAG_LIMIT_US"
            ;;
        s3)
            # One size per run, so the CPU window, network delta and mock
            # statistics each belong to that size alone.
            lg_args=(bigbody "$url" --sizes "$var" --rate "$S3_RATE" --ttft-us "$S3_TTFT_US"
                --chunks "$S3_CHUNKS" --warmup-s "$S3_WARMUP_S" --measure-s "$S3_MEASURE_S")
            warmup="$S3_WARMUP_S" measure="$S3_MEASURE_S" spin="$S3_SPIN_US" lag="$S3_MOCK_WRITE_LAG_LIMIT_US"
            ;;
    esac
    lg_args+=(--label "$arm-$config" --out "$staged" --seed "$seed" --shards "$LOADGEN_SHARDS"
        --cpu-list "$LOADGEN_CPUS" --spin-us "$spin" --max-mock-write-lag-us "$lag"
        --model "$loadgen_model" --pair-id "$pair_id")
    if ((tls)); then
        lg_args+=(--tls-ca "$ca")
    fi
    if [[ -n "$bench_key" ]]; then
        lg_args+=(--header "Authorization: Bearer $bench_key")
    fi
    # bigbody writes <out>-<size>.json.
    if [[ "$scen" == s3 ]]; then
        staged="$stage_dir/$base-$var.json"
    fi
    run_ctx=([scen]="$scen" [config]="$config" [var]="$var" [arm]="$arm" [rep]="$rep"
        [attempt]="$attempt" [order]="$pos" [kind]="$kind" [tag]="$base" [pair_id]="$pair_id"
        [warmup]="$warmup" [measure]="$measure" [open_window]="$open" [perf]=0 [sut_pid]=""
        [idle_rss]="" [check_reuse]=1 [arm_mock]="$(arm_mock "$scen" "$config" "$arm")"
        [staged]="$staged" [result]="$run_dir/results/$base.json")
    if [[ "$kind" == perf ]]; then
        run_ctx[perf]=1
    fi
    if ! direct_arm "$arm"; then
        start_sut "$arm" "$config" "$base"
        run_ctx[sut_pid]="${pids[sut]}"
        if [[ "$scen" == s1 ]] && ((IDLE_RSS_S > 0)); then
            log "  $base: $arm idles $IDLE_RSS_S s before the first request (idle RSS)"
            sleep "$IDLE_RSS_S"
            alive sut || die "the process under test exited while idle; see logs/$base.sut.log"
            run_ctx[idle_rss]="$(vmrss_kib "${pids[sut]}")"
        fi
    fi
    execute_run
}

# All arms of one repetition in the Latin square order. A repetition whose
# gated arms are not all valid (or one failed) is rerun, all arms with the
# same seed and order. brisk-pt is reported only: once the gated
# arms are valid, an invalid brisk-pt (or Brisk next to it) reruns just Brisk
# and brisk-pt at their positions, so that it never costs the gated
# comparisons a repetition. Both kinds of rerun count against RETRY_INVALID.
run_rep() {
    local scen="$1" config="$2" var="$3" rep="$4" attempt=0 arm pos gated_ok=0 pt_ok=1
    local attempt_gated attempt_pt
    shift 4
    local -a order run_set
    read -r -a order <<<"$(rep_order "$rep" "$@")"
    if in_list brisk-pt "${order[@]}"; then
        pt_ok=0
    fi
    run_set=("${order[@]}")
    while true; do
        log "== config $config, $scen${var:+ $var}, repetition $rep$( ((attempt == 0)) || echo ", rerun $attempt"): ${run_set[*]}"
        attempt_gated=1 attempt_pt=1
        pos=0
        for arm in "${order[@]}"; do
            pos=$((pos + 1))
            in_list "$arm" "${run_set[@]}" || continue
            run_arm "$scen" "$config" "$var" "$arm" "$rep" "$attempt" "$pos" run
            if ((!run_ok)); then
                [[ "$arm" == brisk-pt ]] || attempt_gated=0
                [[ "$arm" != brisk && "$arm" != brisk-pt ]] || attempt_pt=0
            fi
        done
        # The gated arms are compared from one attempt, so only an attempt
        # of all arms decides them.
        if ((${#run_set[@]} == ${#order[@]})); then
            gated_ok=$attempt_gated
        fi
        if ((attempt_pt)); then
            pt_ok=1
        fi
        if ((gated_ok && pt_ok)); then
            return 0
        fi
        if ((attempt >= RETRY_INVALID)); then
            if ((!gated_ok)); then
                log "  repetition $rep still has an invalid run after $attempt rerun(s); comparisons will leave it out"
            else
                log "  Brisk and brisk-pt of repetition $rep are still not valid together after $attempt rerun(s); the strip cost comparison will leave it out"
            fi
            return 0
        fi
        attempt=$((attempt + 1))
        retries=$((retries + 1))
        if ((!gated_ok)); then
            run_set=("${order[@]}")
            log "  rerunning repetition $rep, all arms, same seed and order"
        else
            run_set=()
            for arm in "${order[@]}"; do
                [[ "$arm" != brisk && "$arm" != brisk-pt ]] || run_set+=("$arm")
            done
            log "  rerunning Brisk and brisk-pt of repetition $rep, same seed and positions; the gated arms stand"
        fi
    done
}

# The direct arm whose ramp gives the ramp block of a config its tool limit:
# it reaches the mock the block's SUT arms reach, the TLS mock in P and T and
# the plaintext one in N. Every ramp block measures its own, right before
# its pairs, so P and T each run the same TLS path once.
tool_ramp_arm() {
    if [[ "$1" == P ]]; then echo direct-tls; else echo direct; fi
}

# The direct ramp that gives a ramp block its tool limit.
run_tool_ramp() {
    local config="$1" attempt=0 arm
    arm="$(tool_ramp_arm "$config")"
    while true; do
        log "== config $config, ramp, $arm tool limit at $(arm_mock ramp "$config" "$arm")$( ((attempt == 0)) || echo ", rerun $attempt")"
        run_arm ramp "$config" "" "$arm" 0 "$attempt" 1 tool
        if ((run_ok)); then
            return 0
        fi
        if ((attempt >= RETRY_INVALID)); then
            log "  the $arm ramp of config $config is still invalid; its tool limit is unknown"
            return 0
        fi
        attempt=$((attempt + 1))
        retries=$((retries + 1))
    done
}

# One more S2 run per SUT arm with perf stat on the SUT.
run_perf_runs() {
    local config="$1" arm
    shift
    [[ "$perf_status" == available ]] || return 0
    for arm in "$@"; do
        [[ "$arm" != direct ]] || continue
        log "== config $config, s2, perf stat run of $arm"
        run_arm s2 "$config" "" "$arm" 0 0 1 perf
    done
}

# ---------------------------------------------------------------- comparisons

# The block's repetitions whose gated arms are all valid, as JSON
# (prog_select_reps). brisk-pt, reported only, stays out of them; in a block
# with it, .pt holds the repetitions in which Brisk and brisk-pt are valid
# together (pt_pair_selection, pt_report_selection).
select_reps() {
    local s="$1" c="$2" v="$3" arm selection pt
    local -a gated=()
    for arm in $4; do
        [[ "$arm" == brisk-pt ]] || gated+=("$arm")
    done
    selection="$(jq -sc --arg s "$s" --arg c "$c" --arg v "$v" --arg arms "${gated[*]}" "$prog_select_reps" \
        "$run_dir/runs.jsonl")"
    if [[ " $4 " != *" brisk-pt "* ]]; then
        printf '%s\n' "$selection"
        return 0
    fi
    pt="$(jq -sc --arg s "$s" --arg c "$c" --arg v "$v" --arg arms "brisk brisk-pt" "$prog_select_reps" \
        "$run_dir/runs.jsonl")"
    jq -c --argjson pt "$pt" '. + {pt: $pt}' <<<"$selection"
}

# The selection a comparison of arms A and B reads: the gated repetitions,
# or for a pair with brisk-pt those of .pt.
pt_pair_selection() {
    local a="$1" b="$2" selection="$3"
    if [[ "$a" == brisk-pt || "$b" == brisk-pt ]]; then
        jq -c '.pt' <<<"$selection"
    else
        printf '%s\n' "$selection"
    fi
}

# The selection of the per-arm reports, which pair no arms: the gated
# repetitions with brisk-pt's runs of .pt added.
pt_report_selection() {
    jq -c 'if .pt == null then . else .runs["brisk-pt"] = .pt.runs["brisk-pt"] end | del(.pt)' <<<"$1"
}

# Why a result fails the tail screen of brisk-loadgen compare, or nothing.
# Failed requests have no latency, so compare refuses a run (short of
# --allow-invalid) whose requests after the warmup failed, stale retries
# aside, in a larger share than a tenth of the tail above the highest
# compared quantile: 1e-4 at p99.9, where loadgen's validity allows 1e-3.
# The percentages are read as compare reads them (cli.rs parse_percent moves
# the decimal point in the text), so the limit is the same double. Input: a
# result file.
# shellcheck disable=SC2016
prog_tail_screen='
def compare_fraction:
    gsub(" "; "") | split(".") as $p
    | ($p[0] | if length < 2 then ("00" + .)[-2:] else . end) as $int
    | (($int[:-2] | if . == "" then "0" else . end) + "." + $int[-2:] + ($p[1:] | join(""))) | tonumber;
([$quantiles | split(",")[] | compare_fraction] | max) as $qmax
| ((1 - $qmax) * 0.1) as $limit
| .warmup_intervals as $w
| reduce (.intervals[] | select(.index >= $w)) as $iv ({requests: 0, failures: 0};
      .requests += $iv.requests
      | reduce ($iv.errors | to_entries[] | select(.key != "stale_retry") | .value) as $n (.; .failures += $n))
| (.requests + .failures) as $total
| if $total > 0 and .failures / $total > $limit
  then "\(.failures) of \($total) requests after the warmup failed, more than \($limit | pct4) for the \($qmax | qlabel) tail; brisk-loadgen compare would refuse the run"
  else empty end
'
jq_programs+=(prog_tail_screen)

# The tail screen of one result of scenario <scen> (prog_tail_screen).
tail_screen_reason() {
    jq -r --arg quantiles "$(compare_quantiles "$1")" "$jq_defs$prog_tail_screen" "$2"
}

# Compares arm B against arm A over the block's selected repetitions; writes
# compare/<name>.{json,txt} and a compares.jsonl record.
compare_pair() {
    local scen="$1" block="$2" a="$3" b="$4" selection="$5"
    local name="$block-$b-vs-$a" json txt reps planned status="done" rc=0 reason="" pair_ids
    local -a a_files b_files args
    json="$run_dir/compare/$name.json"
    txt="$run_dir/compare/$name.txt"
    rm -f "$json" "$txt"
    reps="$(jq -r '.reps' <<<"$selection")"
    pair_ids="$(jq -c '.pair_ids' <<<"$selection")"
    planned="${block_reps[$block]}"
    if ((reps == 0)); then
        log "compare $name: skipped, no repetition has all arms valid"
        jq -nc --arg name "$name" --arg block "$block" --argjson planned "$planned" \
            '{name: $name, block: $block, status: "skipped", reason: "no repetition has all arms valid",
              reps_used: 0, reps_planned: $planned}' >>"$run_dir/compares.jsonl"
        return 0
    fi
    if ((reps < planned)); then
        status=partial
        log "compare $name: only $reps of $planned repetitions have all arms valid"
    fi
    mapfile -t a_files < <(jq -r --arg a "$a" '.runs[$a][].file' <<<"$selection")
    mapfile -t b_files < <(jq -r --arg b "$b" '.runs[$b][].file' <<<"$selection")
    args=(compare --a "${a_files[@]/#/$run_dir/}" --b "${b_files[@]/#/$run_dir/}"
        --metric "$(compared_metrics "$scen")" --gate-metrics "$(baseline_metrics "$scen")"
        --quantiles "$(compare_quantiles "$scen")" --resamples "$COMPARE_RESAMPLES" --out "$json")
    "$loadgen" "${args[@]}" >"$txt" 2>&1 || rc=$?
    if ((rc != 0)) || [[ ! -f "$json" ]]; then
        reason="brisk-loadgen compare exited with status $rc"
    elif ! jq -e '(.comparisons | length) > 0 and (.gate_pass | type) == "boolean"' "$json" >/dev/null; then
        reason="compare/$name.json lacks comparisons"
    fi
    if [[ -n "$reason" ]]; then
        log "compare $name: FAILED: $reason; see compare/$name.txt"
        # The verdicts must not read a document that failed its check.
        rm -f "$json"
        jq -nc --arg name "$name" --arg block "$block" --arg reason "$reason" \
            '{name: $name, block: $block, status: "failed", reason: $reason}' >>"$run_dir/compares.jsonl"
        return 0
    fi
    jq -c --arg name "$name" --arg block "$block" --arg status "$status" --arg scen "$scen" --arg a "$a" \
        --arg b "$b" --argjson reps "$reps" --argjson planned "$planned" --argjson pair_ids "$pair_ids" \
        "$prog_compare_record" "$json" >>"$run_dir/compares.jsonl"
    log "compare $name: $reps pair(s) ($(jq -r '.pair_ids | join(", ")' <<<"$selection"))"
    jq -r "$jq_defs$prog_compare_lines" "$json" | while IFS= read -r line; do log "$line"; done ||
        log "compare $name: printing the per-metric summary failed; see compare/$name.json"
}

# Every comparison of the session between two arms of the block, each over
# its pt_pair_selection.
compare_block() {
    local scen="$1" block="$2" selection="$3" pair a b pair_selection
    shift 3
    for pair in ${session_compares[$SESSION]}; do
        a="${pair%%:*}" b="${pair#*:}"
        if in_list "$a" "$@" && in_list "$b" "$@"; then
            pair_selection="$(pt_pair_selection "$a" "$b" "$selection")"
            compare_pair "$scen" "$block" "$a" "$b" "$pair_selection"
        fi
    done
}

# ---------------------------------------------------------------- verdicts

# Appends a verdict record and logs its text. A record that could not be
# computed is counted and ends the session with status 1, after the
# remaining measurements.
add_verdict() {
    local record="$1"
    if [[ -z "$record" ]] || ! jq -e 'type == "object" and (.id | type) == "string"' <<<"$record" >/dev/null 2>&1; then
        evaluation_errors=$((evaluation_errors + 1))
        log "ERROR: a verdict could not be computed (see the jq error above); the session will end with status 1"
        return 0
    fi
    printf '%s\n' "$record" >>"$run_dir/verdicts.jsonl"
    log "  $(jq -r '.text' <<<"$record")"
}

# Judges one compared quantile's delta.
#   kind      le        delta <= limit by the 7.5 rule
#             abs       |delta| <= limit (PASS: CI inside [-limit, limit];
#                       FAIL: CI outside it; else UNCERTAIN)
#             improve   delta <= -limit and CI high below 0 (E1): MET or NOT_MET
#             below0    delta and CI high below 0 (E2): MET or NOT_MET
#             notworse  CI low end at most 0 (E2): MET or NOT_MET
#             (the last three UNDECIDED on a single repetition, as the le
#             and abs ones are UNCERTAIN)
#             report    no rule
#   precision a positive value keeps the verdict gated only while the CI
#             half width is within it (TTFT p99, 7.5), microseconds
#   scope     gated, reported or decision
check_delta() {
    local block="$1" cmp="$2" metric="$3" q="$4" kind="$5" limit="$6" precision="$7" scope="$8"
    local rule="$9" label="${10}" doc
    doc="$run_dir/compare/$block-$cmp.json"
    [[ -f "$doc" ]] || doc=/dev/null
    add_verdict "$(jq -nc --slurpfile d "$doc" --arg id "$block:$cmp:$metric:$q:$kind" --arg block "$block" \
        --arg cmp "$cmp" --arg metric "$metric" --argjson q "$q" --arg kind "$kind" --argjson limit "$limit" \
        --argjson prec "$precision" --arg scope "$scope" --arg rule "$rule" --arg label "$label" \
        "$jq_defs$prog_check_delta")"
}

# Upstream connection reuse of the selection's arms over every run of the
# block (prog_reuse).
report_reuse() {
    local block="$1" selection="$2" scope="$3" scen="$4" config="$5" var="$6"
    add_verdict "$(jq -c --slurpfile runs "$run_dir/runs.jsonl" --arg block "$block" --arg scope "$scope" \
        --arg s "$scen" --arg c "$config" --arg v "$var" "$jq_defs$prog_reuse" <<<"$selection")"
}

# CPU per chunk (S1) or per request (S2, S3) of every SUT arm, and between
# every compared pair of SUT arms (over its pt_pair_selection).
report_cpu() {
    local scen="$1" block="$2" selection="$3" field=cpu_us_per_request unit=request pair a b rule
    local report_selection pair_selection
    shift 3
    if [[ "$scen" == s1 ]]; then
        field=cpu_us_per_chunk unit=chunk
    fi
    report_selection="$(pt_report_selection "$selection")"
    add_verdict "$(jq -c --arg block "$block" --arg f "$field" --arg unit "$unit" "$jq_defs$prog_cpu_arms" \
        <<<"$report_selection")"
    for pair in ${session_compares[$SESSION]}; do
        a="${pair%%:*}" b="${pair#*:}"
        [[ "$a" != direct && "$b" != direct ]] || continue
        if in_list "$a" "$@" && in_list "$b" "$@"; then
            rule=cpu
            if [[ "$SESSION" == gate && "$a" == floor-a && "$b" == brisk ]]; then
                rule="7.5 CPU"
            fi
            pair_selection="$(pt_pair_selection "$a" "$b" "$selection")"
            add_verdict "$(jq -c --arg block "$block" --arg f "$field" --arg unit "$unit" --arg a "$a" \
                --arg b "$b" --arg rule "$rule" "$jq_defs$prog_cpu_pair" <<<"$pair_selection")"
        fi
    done
}

report_memory() {
    local block="$1" selection="$2" report_selection
    report_selection="$(pt_report_selection "$selection")"
    add_verdict "$(jq -c --arg block "$block" --arg session "$SESSION" "$jq_defs$prog_memory" <<<"$report_selection")"
}

# perf stat counts of the S2 perf runs of a config, or why there are none;
# the legacy pool lock share (C21) is not collected by this script.
report_perf() {
    local block="$1" config="$2" note=""
    if [[ "$perf_status" != available ]]; then
        add_verdict "$(jq -nc --arg block "$block" --arg reason "$perf_reason" \
            '{id: "\($block):perf", block: $block, kind: "report", rule: "7.5 HITM", verdict: "REPORT",
              gated: false, extendable: false,
              text: "\($block) HITM counts: not collected: \($reason)", data: null}')"
    else
        if [[ -n "$perf_hitm_note" ]]; then
            note="; $perf_hitm_note"
        fi
        add_verdict "$(jq -sc --arg block "$block" --arg c "$config" --arg hitm_note "$note" \
            "$jq_defs$prog_perf_report" "$run_dir/runs.jsonl")"
    fi
    add_verdict "$(jq -nc --arg block "$block" \
        '{id: "\($block):c21", block: $block, kind: "report", rule: "7.5 C21", verdict: "REPORT",
          gated: false, extendable: false,
          text: "\($block) legacy pool lock share (C21): not collected by run-m1.sh; it needs a lock contention profile of the SUT",
          data: null}')"
}

# The 7.5 limit of the S3 TTFT delta p50 of a size, microseconds; empty for
# a size the contract names no limit for.
s3_limit_us() {
    case "$1" in
        100k) echo 300 ;;
        1m) echo 1500 ;;
        10m) echo 10000 ;;
        *) echo "" ;;
    esac
}

gate_s1_checks() {
    local block="$1" config="$2" scope="$3" t50=100 t99=300 p99=75 cmp
    shift 3
    local fa="S1 Brisk vs floor-A" di="S1 Brisk vs direct"
    if [[ "$config" == T ]]; then
        t50=150 t99=400 p99=100
    fi
    check_delta "$block" brisk-vs-floor-a chunk_latency 0.99 le 20 0 "$scope" "7.5 floor-A" "$fa"
    check_delta "$block" brisk-vs-floor-a chunk_latency 0.999 le 100 0 "$scope" "7.5 floor-A" "$fa"
    check_delta "$block" brisk-vs-floor-a ttft 0.5 le 20 0 "$scope" "7.5 floor-A" "$fa"
    check_delta "$block" brisk-vs-floor-a ttft 0.99 le 60 15 "$scope" "7.5 floor-A" "$fa"
    check_delta "$block" brisk-vs-direct ttft 0.5 le "$t50" 0 "$scope" "7.5 direct" "$di"
    check_delta "$block" brisk-vs-direct ttft 0.99 le "$t99" "$p99" "$scope" "7.5 direct" "$di"
    check_delta "$block" brisk-vs-direct chunk_latency 0.5 le 30 0 "$scope" "7.5 direct" "$di"
    check_delta "$block" brisk-vs-direct chunk_latency 0.99 le 100 0 "$scope" "7.5 direct" "$di"
    check_delta "$block" brisk-vs-direct chunk_latency 0.999 le 500 0 "$scope" "7.5 direct" "$di"
    for cmp in brisk-vs-floor-a brisk-vs-direct; do
        check_delta "$block" "$cmp" chunk_wire 0.5 report 0 0 reported "7.5 chunk_wire" "S1 ${cmp//-vs-/ vs }"
        check_delta "$block" "$cmp" chunk_wire 0.99 report 0 0 reported "7.5 chunk_wire" "S1 ${cmp//-vs-/ vs }"
    done
    if in_list brisk-pt "$@"; then
        local strip="S1 strip cost (inject vs passthrough)"
        check_delta "$block" brisk-vs-brisk-pt chunk_latency 0.5 report 0 0 reported "7.5 strip" "$strip"
        check_delta "$block" brisk-vs-brisk-pt chunk_latency 0.99 report 0 0 reported "7.5 strip" "$strip"
        check_delta "$block" brisk-vs-brisk-pt chunk_latency 0.999 report 0 0 reported "7.5 strip" "$strip"
        check_delta "$block" brisk-vs-brisk-pt ttft 0.5 report 0 0 reported "7.5 strip" "$strip"
        check_delta "$block" brisk-vs-brisk-pt chunk_wire 0.5 report 0 0 reported "7.5 strip" "$strip"
    fi
}

gate_s3_checks() {
    local block="$1" size="$2" scope="$3" limit label="S3 $2 Brisk vs floor-A"
    limit="$(s3_limit_us "$size")"
    if [[ -z "$limit" ]]; then
        check_delta "$block" brisk-vs-floor-a ttft 0.5 report 0 0 reported "7.5 S3" "$label (no 7.5 limit for this size)"
        check_delta "$block" brisk-vs-floor-a ttft 0.99 report 0 0 reported "7.5 S3" "$label (no 7.5 limit for this size)"
        return 0
    fi
    check_delta "$block" brisk-vs-floor-a ttft 0.5 le "$limit" 0 "$scope" "7.5 S3" "$label"
    check_delta "$block" brisk-vs-floor-a ttft 0.99 le "$((2 * limit))" 0 "$scope" "7.5 S3" "$label"
}

e11_checks() {
    local block="$1" selection="$2" fa fb ab
    fa="$run_dir/compare/$block-floor-a-vs-direct.json"
    fb="$run_dir/compare/$block-floor-b-vs-direct.json"
    ab="$run_dir/compare/$block-floor-b-vs-floor-a.json"
    [[ -f "$fa" ]] || fa=/dev/null
    [[ -f "$fb" ]] || fb=/dev/null
    [[ -f "$ab" ]] || ab=/dev/null
    add_verdict "$(jq -nc --slurpfile fa "$fa" --slurpfile fb "$fb" --slurpfile ab "$ab" --arg block "$block" \
        "$jq_defs$prog_e11_latency")"
    add_verdict "$(jq -c --arg block "$block" "$jq_defs$prog_e11_cpu" <<<"$selection")"
}

evaluate_ramp() {
    local config="$1" a="$2" b="$3" block="ramp-$1" scope=reported pairs
    if [[ "$SESSION" == gate && "$config" == P ]]; then
        scope=gated
    fi
    pairs="$(jq -sc --arg c "$config" --arg a "$a" --arg b "$b" "$prog_ramp_pairs" "$run_dir/runs.jsonl")"
    add_verdict "$(jq -c --arg block "$block" --arg a "$a" --arg b "$b" --arg session "$SESSION" --arg scope "$scope" \
        --argjson share "$RAMP_TOOL_LIMIT_SHARE" --argjson start "$RAMP_START" --arg step_pct "$RAMP_STEP_PCT" \
        "$jq_defs$prog_ramp_verdict" <<<"$pairs")"
    add_verdict "$(jq -c --arg block "$block" --arg a "$a" --arg b "$b" --arg session "$SESSION" \
        --argjson cpus "$(cpu_count "$SUT_CPUS")" "$jq_defs$prog_ramp_vcpu" <<<"$pairs")"
}

# Compares a block and records its verdicts and reports.
evaluate_block() {
    local scen="$1" config="$2" var="$3" block selection scope=reported
    shift 3
    block="$(block_name "$scen" "$config" "$var")"
    log "== evaluating $block (${block_reps[$block]} repetition(s))"
    if [[ "$scen" == ramp ]]; then
        evaluate_ramp "$config" "$@"
        return 0
    fi
    selection="$(select_reps "$scen" "$config" "$var" "$*")"
    compare_block "$scen" "$block" "$selection" "$@"
    if [[ "$SESSION" == gate && "$config" == P ]]; then
        scope=gated
    fi
    if [[ "$scen" == s3 ]]; then
        report_reuse "$block" "$selection" reported "$scen" "$config" "$var"
    else
        report_reuse "$block" "$selection" "$scope" "$scen" "$config" "$var"
    fi
    report_cpu "$scen" "$block" "$selection" "$@"
    if [[ "$scen" == s1 ]]; then
        report_memory "$block" "$selection"
    fi
    if [[ "$scen" == s2 ]]; then
        report_perf "$block" "$config"
    fi
    case "$SESSION:$scen" in
        gate:s1) gate_s1_checks "$block" "$config" "$scope" "$@" ;;
        gate:s3) gate_s3_checks "$block" "$var" "$scope" ;;
        e11:s1) e11_checks "$block" "$selection" ;;
        e1:s2)
            check_delta "$block" brisk-match-vs-brisk-axum request_latency 0.5 improve 2 0 decision \
                e1-latency "E1 S2 match vs axum"
            ;;
        e2:s3)
            check_delta "$block" brisk-concat-vs-brisk-segments ttft 0.5 below0 0 0 decision \
                e2-ttft "E2 S3 $var concat vs segments"
            ;;
        e2:s1)
            check_delta "$block" brisk-concat-vs-brisk-segments chunk_latency 0.99 notworse 0 0 decision \
                e2-chunk "E2 S1 concat vs segments"
            ;;
        e4:s1)
            check_delta "$block" brisk-hold2s-vs-brisk-hold0s ttft 0.5 abs 10 0 decision \
                e4-p50 "E4 S1 ${var:+$var }commit_hold 2s vs 0s"
            check_delta "$block" brisk-hold2s-vs-brisk-hold0s ttft 0.99 le 30 0 decision \
                e4-p99 "E4 S1 ${var:+$var }commit_hold 2s vs 0s"
            ;;
    esac
}

# Whether a gated verdict of the block is UNCERTAIN in a way more
# repetitions can resolve.
block_extendable() {
    jq -se --arg b "$1" "$jq_defs"'[.[] | select(.block == $b)] | last_by(.id) | any(.[]; .extendable == true)' \
        "$run_dir/verdicts.jsonl" >/dev/null
}

# Runs one block (scenario, config, variant): its repetitions, the perf runs
# after S2, then the comparisons and verdicts, extending the block while a
# gated verdict is UNCERTAIN.
run_block() {
    local scen="$1" config="$2" var="$3" block reps rep
    local -a arms
    block="$(block_name "$scen" "$config" "$var")"
    read -r -a arms <<<"$(block_arms "$scen" "$config")"
    reps="$(planned_reps "$scen")"
    log "== block $block: arms ${arms[*]}, $reps repetition(s)"
    if [[ "$scen" == ramp ]]; then
        run_tool_ramp "$config"
    fi
    for ((rep = 1; rep <= reps; rep++)); do
        run_rep "$scen" "$config" "$var" "$rep" "${arms[@]}"
    done
    block_reps[$block]="$reps"
    if [[ "$scen" == s2 ]]; then
        run_perf_runs "$config" "${arms[@]}"
    fi
    evaluate_block "$scen" "$config" "$var" "${arms[@]}"
    while ((EXTEND_UNCERTAIN && reps < EXTEND_MAX_REPS)) && block_extendable "$block"; do
        reps=$((reps + 1))
        extensions=$((extensions + 1))
        log "== block $block: a gated verdict is UNCERTAIN; adding repetition $reps (at most $EXTEND_MAX_REPS)"
        run_rep "$scen" "$config" "$var" "$reps" "${arms[@]}"
        block_reps[$block]="$reps"
        evaluate_block "$scen" "$config" "$var" "${arms[@]}"
    done
}

# ---------------------------------------------------------------- perf

# HITM-related events tried by default. Their names differ by vendor and
# generation, so each is probed and only those that count on this host are
# used: Intel's snoop-hit-modified loads and AMD's fills from another core's
# cache in the same CCX.
perf_hitm_candidates="mem_load_l3_hit_retired.xsnp_hitm,mem_load_l3_hit_retired.xsnp_fwd,mem_load_uops_l3_hit_retired.xsnp_hitm,ocr.demand_data_rd.l3_hit.snoop_hitm,ls_any_fills_from_sys.local_ccx,ls_dmnd_fills_from_sys.local_ccx,ls_any_fills_from_sys.int_cache,ls_dmnd_fills_from_sys.int_cache"
perf_generic_events="task-clock,context-switches,cpu-migrations"

# Whether perf stat counts the event here (a VM may not expose the PMU, in
# which case perf reports it as not supported).
perf_counts() {
    local out
    out="$(perf stat -x, -e "$1" -- true 2>&1 >/dev/null)" || return 1
    awk -F, -v ev="$1" 'index($3, ev) == 1 && $1 ~ /^[0-9.]+$/ { ok = 1 } END { exit !ok }' <<<"$out"
}

# Decides whether the S2 perf runs happen and with which events.
perf_probe() {
    local paranoid ev out
    local -a hitm=() candidates
    if ((session_has_brisk + session_has_floor == 0)) || ! has_scenario s2; then
        return 0
    fi
    if [[ "$PERF" == 0 ]]; then
        perf_status=disabled perf_reason="PERF=0"
        return 0
    fi
    paranoid="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)" || paranoid=unreadable
    if ! command -v perf >/dev/null; then
        perf_status=unavailable perf_reason="perf is not installed"
    elif ! out="$(perf stat -x, -e task-clock -- true 2>&1 >/dev/null)"; then
        perf_status=unavailable
        perf_reason="perf stat fails (perf_event_paranoid $paranoid): $(head -n 1 <<<"$out")"
    else
        IFS=, read -r -a candidates <<<"${PERF_EVENTS:-$perf_hitm_candidates}"
        for ev in "${candidates[@]}"; do
            if perf_counts "$ev"; then
                hitm+=("$ev")
            fi
        done
        perf_status=available
        perf_reason=""
        perf_hitm_events="$(IFS=,; echo "${hitm[*]}")"
        perf_events="$perf_generic_events${perf_hitm_events:+,$perf_hitm_events}"
        if ((${#hitm[@]} == 0)); then
            perf_hitm_note="no HITM-related event counts on this host (tried ${PERF_EVENTS:-$perf_hitm_candidates}; perf_event_paranoid $paranoid; a VM may not expose the PMU), so only $perf_generic_events are counted"
        fi
    fi
    if [[ "$PERF" == 1 && ("$perf_status" != available || ${#hitm[@]} -eq 0) ]]; then
        die "PERF=1 but ${perf_reason:-$perf_hitm_note}"
    fi
    log "perf: $perf_status${perf_reason:+ ($perf_reason)}${perf_events:+; events $perf_events}"
}

# ---------------------------------------------------------------- selfcheck

# One selfcheck against a mock with the given TLS setting; sets sc_result to
# PASS or FAIL.
selfcheck_one() {
    local tls="$1" config=P scheme=http tag=selfcheck
    local -a mock_tls=() curl_ca=()
    if ((tls)); then
        config=T scheme=https tag=selfcheck-T
        mock_tls=(--tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key")
        curl_ca=(--cacert "$ca")
    fi
    sc_tls="$tls"
    log "selfcheck $config: $SC_CONCURRENCY x 2 streams, mock $MOCK_SHARDS shard(s) on CPUs $MOCK_CPUS, loadgen $LOADGEN_SHARDS shard(s) on $LOADGEN_CPUS"
    start_proc mock-sc "$MOCK_CPUS" "$run_dir/logs/$tag.mock.log" "$mock" serve \
        --listen "127.0.0.1:$SC_PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS" --spin-us "$SPIN_US" \
        "${mock_emit_args[@]}" "${mock_tls[@]}"
    wait_ready mock-sc "$scheme://127.0.0.1:$SC_PORT/v1/models" 10 "logs/$tag.mock.log" "${curl_ca[@]}"
    fingerprint_proc mock-sc "$tag"
    lg_args=(selfcheck "$scheme://127.0.0.1:$SC_PORT" --concurrency "$SC_CONCURRENCY")
    stream_shape "$S1_TTFT_US"
    lg_args+=(--warmup-s "$SC_WARMUP_S" --measure-s "$SC_MEASURE_S" --label "selfcheck-$config"
        --out "$stage_dir/$tag.json" --seed "$SEED" --shards "$LOADGEN_SHARDS" --cpu-list "$LOADGEN_CPUS"
        --spin-us "$SPIN_US" --max-mock-write-lag-us "$MOCK_WRITE_LAG_LIMIT_US")
    if ((tls)); then
        lg_args+=(--tls-ca "$ca")
    fi
    run_ctx=([scen]=sc [config]="$config" [var]="" [arm]=direct [rep]=1 [attempt]=0 [order]=1 [kind]=sc
        [tag]="$tag" [pair_id]="" [warmup]="$SC_WARMUP_S" [measure]="$SC_MEASURE_S" [open_window]=0
        [perf]=0 [sut_pid]="" [idle_rss]="" [check_reuse]=0 [arm_mock]=mock-sc
        [staged]="$stage_dir/$tag.json" [result]="$run_dir/results/$tag.json")
    execute_run
    stop_proc mock-sc
    sc_tls=0
    if [[ -f "$run_dir/results/$tag.json" ]] && jq -e '.loadgen.selfcheck.pass' "$run_dir/results/$tag.json" >/dev/null; then
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

# ---------------------------------------------------------------- summary

write_manifest() {
    local status="$1" exit_status="${2:-}" patch=null
    if [[ -f "$run_dir/host/sync-diff.patch" ]]; then
        patch='"host/sync-diff.patch"'
    fi
    jq -n --arg status "$status" --arg exit_status "$exit_status" --arg session "$SESSION" --arg run_id "$RUN_ID" \
        --arg started "$session_started" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg host "$(hostname)" \
        --arg selfcheck "$selfcheck_status" --arg selfcheck_detail "$selfcheck_detail" \
        --arg host_ready "$host_ready" --argjson invalid "$invalid_runs" --argjson failed "$failed_runs" \
        --argjson retries "$retries" --argjson extensions "$extensions" \
        --argjson evaluation_errors "$evaluation_errors" --argjson patch "$patch" \
        --arg arms "${session_arms[$SESSION]}" --arg key_name "$key_name" --arg key_sha256 "$key_sha256" \
        --arg perf_status "$perf_status" --arg perf_reason "$perf_reason" --arg perf_events "$perf_events" \
        --arg perf_hitm "$perf_hitm_events" \
        --rawfile knobs "$run_dir/host/knobs.env" --rawfile sync "$run_dir/host/sync-info" \
        --rawfile binaries "$run_dir/host/binaries.txt" \
        --slurpfile runs "$run_dir/runs.jsonl" --slurpfile compares "$run_dir/compares.jsonl" \
        --slurpfile verdicts "$run_dir/verdicts.jsonl" \
        "$jq_defs$prog_manifest" >"$run_dir/manifest.json" 2>/dev/null || log "writing manifest.json failed"
    manifest_written=1
}

write_summary() {
    local counts="$1" failed_compares="$2" status="$3"
    {
        printf 'session %s (%s): %s, exit status %s\n' "$SESSION" "$RUN_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$status"
        printf 'configs %s; scenarios %s; selfcheck %s%s\n' "$CONFIGS" "$SCENARIOS" "$selfcheck_status" \
            "${selfcheck_detail:+ ($selfcheck_detail)}"
        printf 'runs: %d invalid (%d repetition rerun(s), %d block extension(s)), %d failed; %d missing or partial comparison(s); %d evaluation error(s)\n' \
            "$invalid_runs" "$retries" "$extensions" "$failed_runs" "$failed_compares" "$evaluation_errors"
        read -r n_pass n_fail n_uncertain n_missing <<<"$counts"
        printf 'gated verdicts: %d PASS, %d FAIL, %d UNCERTAIN, %d MISSING\n' "$n_pass" "$n_fail" "$n_uncertain" "$n_missing"
        echo
        jq -rs "$jq_defs"'last_by(.id)[] | .text' "$run_dir/verdicts.jsonl"
    } >"$run_dir/summary.txt"
}

# Fails when the virtual key shows up under the session directory, replacing
# it in every file but the session log, which is still being written.
check_key_leak() {
    local file
    local -a leaked
    [[ -n "$bench_key" ]] || return 0
    mapfile -t leaked < <(grep -rlF --exclude-dir=bin -- "$bench_key" "$run_dir" || true)
    ((${#leaked[@]} > 0)) || return 0
    for file in "${leaked[@]}"; do
        log "ERROR: the virtual key appears in ${file#"$run_dir/"}"
        if [[ "$file" != "$run_dir/session.log" ]]; then
            sed -i "s/$bench_key/bk-<redacted>/g" "$file"
        fi
    done
    return 1
}

# ---------------------------------------------------------------- session

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

preflight_jq

log "session $SESSION ($RUN_ID) in $run_dir"
log "arms: ${session_arms[$SESSION]}; configs: $CONFIGS; scenarios: $SCENARIOS"
log "core plan: SUT $SUT_CPUS ($SUT_WORKERS worker(s)), mocks $MOCK_CPUS ($MOCK_SHARDS shard(s)), loadgen $LOADGEN_CPUS ($LOADGEN_SHARDS shard(s)), harness $HARNESS_CPUS"
log "mock emission policy $MOCK_EMIT_POLICY (commit window $MOCK_COMMIT_US us); mock write lag p99 limit $MOCK_WRITE_LAG_LIMIT_US us (S3 $S3_MOCK_WRITE_LAG_LIMIT_US us, ramp $RAMP_MOCK_WRITE_LAG_LIMIT_US us); minimum reuse $MIN_REUSE"
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
for bin in "${required_bins[@]}"; do
    cp -p "$BIN_DIR/$bin" "$run_dir/bin/$bin"
done
mock="$run_dir/bin/brisk-mock"
loadgen="$run_dir/bin/brisk-loadgen"
floor="$run_dir/bin/brisk-floor"
brisk="$run_dir/bin/brisk"
for bin in "${required_bins[@]}"; do
    version="$("$run_dir/bin/$bin" --version 2>/dev/null)" || version="($bin --version failed)"
    printf '%s  %s  %s\n' "$(sha256sum "$run_dir/bin/$bin" | cut -d' ' -f1)" "$version" "$run_dir/bin/$bin"
done >"$run_dir/host/binaries.txt"
if ((session_has_brisk)); then
    cp "$brisk_template" "$run_dir/host/"
fi
# An older build would reject a flag only at its first use, hours in.
mock_help="$("$mock" serve --help)"
for flag in --emit-policy --commit-us; do
    [[ "$mock_help" == *"$flag"* ]] || die "$BIN_DIR/brisk-mock serve has no $flag; sync a build that has it"
done
for sub in selfcheck stream nonstream bigbody; do
    [[ "$("$loadgen" "$sub" --help)" == *--max-mock-write-lag-us* ]] ||
        die "$BIN_DIR/brisk-loadgen $sub has no --max-mock-write-lag-us; sync a build that has it"
done
[[ "$("$loadgen" stream --help)" == *--max-slip-us* ]] ||
    die "$BIN_DIR/brisk-loadgen stream has no --max-slip-us; sync a build that has it"
if ((session_has_floor)); then
    [[ "$("$floor" --help)" == *--mode* ]] || die "$BIN_DIR/brisk-floor has no --mode (floor-B); build the M1 floor"
fi
if ((session_has_brisk)); then
    for sub in keygen serve check-config; do
        "$brisk" "$sub" --help >/dev/null 2>&1 || die "$BIN_DIR/brisk has no $sub subcommand; build the M1 brisk"
    done
    [[ "$("$brisk" serve --help)" == *--config* ]] || die "$BIN_DIR/brisk serve has no --config"
fi
# Every mock of the session, selfcheck and scenarios alike.
mock_emit_args=(--emit-policy "$MOCK_EMIT_POLICY" --commit-us "$MOCK_COMMIT_US")

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

roles_scenario="$(jq -nc --argjson s "$(cpu_array "$SUT_CPUS")" --argjson m "$(cpu_array "$MOCK_CPUS")" \
    --argjson l "$(cpu_array "$LOADGEN_CPUS")" '{sut: $s, mock: $m, loadgen: $l}')"
roles_sc="$(jq -nc --argjson m "$(cpu_array "$MOCK_CPUS")" --argjson l "$(cpu_array "$LOADGEN_CPUS")" \
    '{mock: $m, loadgen: $l}')"

# Planned duration, for the log only; reruns, extensions and comparisons come
# on top, and a ramp is counted with all its steps.
run_overhead_s=10
planned_s=0
for config in $CONFIGS; do
    for scen in "${scenario_list[@]}"; do
        read -r -a plan_arms <<<"$(block_arms "$scen" "$config")"
        mapfile -t plan_vars < <(block_vars "$scen")
        case "$scen" in
            s1) run_s=$((S1_WARMUP_S + S1_MEASURE_S)) ;;
            s2) run_s=$((S2_WARMUP_S + S2_MEASURE_S)) ;;
            s3) run_s=$((S3_WARMUP_S + S3_MEASURE_S)) ;;
            ramp) run_s=$((RAMP_WARMUP_S + RAMP_MAX_STEPS * RAMP_STEP_S)) ;;
        esac
        for arm in "${plan_arms[@]}"; do
            arm_s=$((run_s + run_overhead_s))
            if [[ "$scen" == s1 && "$arm" != direct ]]; then
                arm_s=$((arm_s + IDLE_RSS_S))
            fi
            planned_s=$((planned_s + ${#plan_vars[@]} * $(planned_reps "$scen") * arm_s))
        done
        if [[ "$scen" == ramp ]]; then
            planned_s=$((planned_s + run_s + run_overhead_s))
        fi
    done
done
log "planned session time at most about $((planned_s / 3600)) h $((planned_s % 3600 / 60)) min before reruns, extensions, perf runs and comparisons"

certs_dir="$(mktemp -d)"
"$mock" gen-cert --out-dir "$certs_dir" --san 127.0.0.1 localhost >"$run_dir/logs/gen-cert.log" 2>&1 ||
    die "gen-cert failed; see logs/gen-cert.log"
ca="$certs_dir/ca.pem"

perf_probe

if ((session_has_brisk)); then
    # The frozen stdout of brisk keygen (contract 05, section 1.5): the key,
    # an empty line, then the [[keys]] snippet. Nothing of it is printed on a
    # mismatch, since it may hold the key.
    keygen_out="$("$brisk" keygen --name "$key_name" 2>"$run_dir/logs/keygen.err")" ||
        die "brisk keygen failed; see logs/keygen.err"
    mapfile -t keygen_lines <<<"$keygen_out"
    unset keygen_out
    ((${#keygen_lines[@]} == 5)) ||
        die "brisk keygen printed ${#keygen_lines[@]} line(s) instead of 5; its output does not follow the frozen format"
    [[ "${keygen_lines[0]}" =~ ^bk-[A-Za-z0-9_-]{36}$ ]] ||
        die "line 1 of brisk keygen is not a bk- key of 36 base64url characters"
    [[ -z "${keygen_lines[1]}" && "${keygen_lines[2]}" == "[[keys]]" && "${keygen_lines[3]}" == "name = \"$key_name\"" ]] ||
        die "lines 2 to 4 of brisk keygen are not an empty line, [[keys]] and the name"
    [[ "${keygen_lines[4]}" =~ ^sha256\ =\ \"([0-9a-f]{64})\"$ ]] ||
        die "line 5 of brisk keygen is not sha256 = \"<64 lowercase hex digits>\""
    key_sha256="${BASH_REMATCH[1]}"
    bench_key="${keygen_lines[0]}"
    unset keygen_lines
    log "virtual key $key_name: sha256 $key_sha256"
    # Every Brisk configuration of the session, validated before the first run.
    declare -A rendered=()
    for config in $CONFIGS; do
        for scen in "${scenario_list[@]}"; do
            for arm in $(block_arms "$scen" "$config"); do
                [[ "$arm" == brisk* && -z "${rendered[$arm-$config]:-}" ]] || continue
                cfg="$(brisk_config_path "$arm" "$config")"
                render_brisk_config "$arm" "$config" "$cfg"
                env "$upstream_key_env=$upstream_key_value" "$brisk" check-config --config "$cfg" \
                    >"$run_dir/logs/check-config-$arm-$config.log" 2>&1 ||
                    die "brisk check-config rejects brisk/${cfg##*/}; see logs/check-config-$arm-$config.log"
                rendered[$arm-$config]=1
                log "configuration brisk/${cfg##*/} checked"
            done
        done
    done
fi

if has_scenario sc; then
    run_selfcheck
    if [[ "$selfcheck_status" == FAIL ]] && ((SC_GATE)); then
        log "selfcheck failed; the tools lack headroom on this core plan, so no scenario is run (SC_GATE=0 overrides)"
        write_manifest selfcheck_failed 1
        exit 1
    fi
fi

if ((${#scenario_list[@]} > 0)); then
    start_proc mock-plain "$MOCK_CPUS" "$run_dir/logs/mock-plain.log" "$mock" serve \
        --listen "127.0.0.1:$MOCK_PLAIN_PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS" --spin-us "$SPIN_US" \
        "${mock_emit_args[@]}"
    start_proc mock-tls "$MOCK_CPUS" "$run_dir/logs/mock-tls.log" "$mock" serve \
        --listen "127.0.0.1:$MOCK_TLS_PORT" --shards "$MOCK_SHARDS" --cpu-list "$MOCK_CPUS" --spin-us "$SPIN_US" \
        "${mock_emit_args[@]}" --tls-cert "$certs_dir/server.pem" --tls-key "$certs_dir/server.key"
    wait_ready mock-plain "http://127.0.0.1:$MOCK_PLAIN_PORT/v1/models" 10 logs/mock-plain.log
    wait_ready mock-tls "https://127.0.0.1:$MOCK_TLS_PORT/v1/models" 10 logs/mock-tls.log --cacert "$ca"
    fingerprint_proc mock-plain session
    fingerprint_proc mock-tls session

    for config in $CONFIGS; do
        for scen in "${scenario_list[@]}"; do
            mapfile -t vars < <(block_vars "$scen")
            for var in "${vars[@]}"; do
                run_block "$scen" "$config" "$var"
            done
        done
    done
    stop_proc mock-plain
    stop_proc mock-tls
fi

if [[ "$SESSION" != gate ]]; then
    add_verdict "$(jq -sc --arg session "$SESSION" "$jq_defs$prog_decide" "$run_dir/verdicts.jsonl")"
fi

counts="$(jq -rs "$jq_defs$prog_counts" "$run_dir/verdicts.jsonl")"
read -r n_pass n_fail n_uncertain n_missing <<<"$counts"
failed_compares="$(jq -rs "$jq_defs"'[last_by(.name)[] | select(.status != "done")] | length' "$run_dir/compares.jsonl")"
rm -rf "$stage_dir"
leak=0
check_key_leak || leak=1
exit_status=0
if [[ "$selfcheck_status" == FAIL ]] || ((failed_runs + failed_compares + evaluation_errors + leak + n_fail + n_missing > 0)); then
    exit_status=1
elif ((n_uncertain > 0)); then
    exit_status=3
fi
write_summary "$counts" "$failed_compares" "$exit_status"
write_manifest complete "$exit_status"
log "session $RUN_ID done: selfcheck $selfcheck_status, $invalid_runs invalid run(s) ($retries repetition rerun(s), $extensions block extension(s)), $failed_runs failed run(s), $failed_compares missing or partial comparison(s), gated verdicts $n_pass PASS, $n_fail FAIL, $n_uncertain UNCERTAIN, $n_missing MISSING"
while IFS= read -r line; do
    log "$line"
done <"$run_dir/summary.txt"
log "results in $run_dir"
exit "$exit_status"
