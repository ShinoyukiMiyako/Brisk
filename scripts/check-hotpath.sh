#!/usr/bin/env bash
# Hot-path rules that clippy cannot express, checked by grep over the source of
# brisk-proto and brisk-gateway (crates/*/src only). Fails (exit 1) and prints
# every offending line if any rule is broken:
#
# - R3: no serde attribute that makes a derived deserializer buffer its input:
#   `flatten`, `untagged`, and internally or adjacently tagged enums
#   (`tag = "..."`, `content = "..."`).
# - R17: no tracing spans (`#[instrument]`, `span!(` and every `*_span!(`), and
#   no tracing event (`trace!(` ... `error!(`, `event!(`) whose arguments
#   include `?headers`, `?request`, `?req`, `?uri`, `%uri`, `?parts` or
#   `?body`, or whose format string captures one of those names inline
#   (`"{req:?}"`), so the inbound `Authorization` header and the raw URI never
#   reach a log. A multi-line event is checked up to its closing parenthesis.
# - R9: the per-chunk files (brisk-gateway body/chunk.rs and body/idle.rs,
#   brisk-proto sse.rs and usage.rs) read no clock, log nothing, touch no
#   atomics and take no locks. A line ending in `// hotpath-allow: <reason>`
#   is exempt. Files that do not exist yet are skipped.
# - Cow borrow: a struct field whose type contains `Cow<'` has `borrow` on the
#   same or the previous line; serde deserializes a `Cow` without
#   `#[serde(borrow)]` into an owned, allocated value. A line carrying
#   `// hotpath-allow: <reason>` is exempt (a function parameter on a line of
#   its own looks like a field to a line-based check).
#
# Lines that are only a `//` comment are prose, not code, and are skipped by
# every rule except as the "previous line" of the Cow check.
#
# Before checking the sources the script runs the rules over built-in violating
# and clean samples and fails unless every rule fires exactly where expected.
# awk implementations (gawk, mawk, BWK awk) differ in regex details, and a rule
# that silently stops matching would otherwise make the check look clean. Set
# AWK to choose the implementation; the default is `awk`.
#
# Works on Linux, macOS and Git Bash on Windows; needs bash, grep, mktemp and
# awk.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

awk_bin="${AWK:-awk}"

roots=(crates/brisk-proto/src crates/brisk-gateway/src)
r9_files=(
    crates/brisk-gateway/src/body/chunk.rs
    crates/brisk-gateway/src/body/idle.rs
    crates/brisk-proto/src/sse.rs
    crates/brisk-proto/src/usage.rs
)

program="$(
    cat <<'AWK'
BEGIN {
    n = split(r9_list, parts, " ")
    for (i = 1; i <= n; i++) r9_files[parts[i]] = 1

    before = "(^|[^A-Za-z0-9_])"
    after = "([^A-Za-z0-9_]|$)"
    event_macro = before "(trace|debug|info|warn|error|event)!\\("
    logged_value = "(\\?(headers|request|req|uri|parts|body)|%uri)" after
    inline_capture = "[{](headers|request|req|uri|parts|body)(:[^}]*)?[}]"
    allow_marker = "//[[:space:]]*hotpath-allow:[[:space:]]*[^[:space:]]"
    cow_field = "^[[:space:]]*(pub(\\([^)]*\\))?[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*[[:space:]]*:[^:].*Cow<'"

    r3_count = 0
    r3_pattern[++r3_count] = "(^|[^A-Za-z0-9_.])flatten[[:space:]]*($|[,)])"
    r3_name[r3_count] = "flatten"
    r3_pattern[++r3_count] = before "untagged" after
    r3_name[r3_count] = "untagged"
    r3_pattern[++r3_count] = before "tag[[:space:]]*=[[:space:]]*\""
    r3_name[r3_count] = "tag ="
    r3_pattern[++r3_count] = before "content[[:space:]]*=[[:space:]]*\""
    r3_name[r3_count] = "content ="

    r9_count = 0
    r9_pattern[++r9_count] = "Instant::now"
    r9_pattern[++r9_count] = "SystemTime"
    r9_pattern[++r9_count] = "tracing::"
    r9_pattern[++r9_count] = event_macro
    r9_pattern[++r9_count] = "Ordering::"
    r9_pattern[++r9_count] = "fetch_"
    r9_pattern[++r9_count] = "\\.lock\\("
    r9_pattern[++r9_count] = "Mutex"
    r9_pattern[++r9_count] = "RwLock"

    failures = 0
}

function report(rule, message) {
    printf "%s:%d: %s: %s\n    %s\n", FILENAME, FNR, rule, message, trimmed(line)
    failures++
}

function trimmed(s) {
    sub(/^[[:space:]]+/, "", s)
    return s
}

# String literals may contain parentheses and `?name` text that is not code.
# Raw strings go first because a backslash in them escapes nothing. Char and
# byte literals go before plain strings because the `"` in `b'"'` would
# otherwise open a string that swallows the code after it; a char literal holds
# exactly one character or escape, so a lifetime (`'a`) never matches.
function without_strings(s) {
    gsub(/b?r#"([^"]|"[^#])*"#/, "\"\"", s)
    gsub(/b?r"[^"]*"/, "\"\"", s)
    gsub(/b?'([^'\\]|\\u[{][0-9A-Fa-f_]*[}]|\\[^u])'/, "'_'", s)
    gsub(/"([^"\\]|\\.)*"/, "\"\"", s)
    return s
}

function paren_delta(s,    opens, closes) {
    opens = gsub(/\(/, "(", s)
    closes = gsub(/\)/, ")", s)
    return opens - closes
}

function check_r3(code,    i) {
    for (i = 1; i <= r3_count; i++) {
        if (code ~ r3_pattern[i]) report("R3", "serde `" r3_name[i] "` buffers the input")
    }
}

# `raw` still holds the string literals, where inline format captures live.
function check_r17(code, raw,    rest) {
    if (code ~ /#\[([A-Za-z_][A-Za-z0-9_]*::)*instrument/) report("R17", "`#[instrument]` records request fields in a span")
    if (code ~ /span!\(/) report("R17", "tracing spans are not allowed")

    if (in_event) {
        if (code ~ logged_value || raw ~ inline_capture) report("R17", "tracing event logs headers, the request, its URI, parts or body")
        depth += paren_delta(code)
        if (depth <= 0) in_event = 0
        return
    }
    if (match(code, event_macro)) {
        rest = substr(code, RSTART)
        if (rest ~ logged_value || raw ~ inline_capture) report("R17", "tracing event logs headers, the request, its URI, parts or body")
        depth = paren_delta(rest)
        in_event = (depth > 0)
    }
}

function check_r9(code,    i) {
    if (line ~ allow_marker) return
    for (i = 1; i <= r9_count; i++) {
        if (code ~ r9_pattern[i]) {
            report("R9", "per-chunk code must not read clocks, log, use atomics or lock (add `// hotpath-allow: <reason>` if justified)")
            return
        }
    }
}

FNR == 1 {
    prev = ""
    in_event = 0
    depth = 0
    is_r9 = (FILENAME in r9_files)
}

{
    sub(/\r$/, "")
    line = $0
    if (line !~ /^[[:space:]]*\/\//) {
        code = without_strings(line)
        check_r3(code)
        check_r17(code, line)
        if (is_r9) check_r9(code)
        if (line ~ cow_field && line !~ /borrow/ && prev !~ /borrow/ && line !~ allow_marker) {
            report("Cow", "`Cow` field without `#[serde(borrow)]` on this or the previous line")
        }
    }
    prev = line
}

END {
    exit (failures > 0 ? 1 : 0)
}
AWK
)"

# Runs the rules over samples: every line of the bad_* files listed in
# `expected` must be reported, and nothing else (in particular nothing in the
# good_* files, which hold look-alikes that are not violations).
self_test() {
    local dir
    dir="$(mktemp -d)"
    # shellcheck disable=SC2064 # expand now: `dir` is local to this function.
    trap "rm -rf '$dir'" RETURN

    cat >"$dir/bad_general.rs" <<'RS'
#[serde(flatten)]
#[serde(untagged)]
#[serde(tag = "type")]
#[serde(content = "c")]
#[tracing::instrument]
let _span = tracing::info_span!("request");
tracing::warn!(?headers, "rejected");
tracing::debug!(uri = %uri, "x");
tracing::info!(
    status = ?status,
    ?req,
);
tracing::warn!("rejected {req:?}");
tracing::error!(
    "failed {parts}",
);
    name: Cow<'a, str>,
RS
    cat >"$dir/bad_r9.rs" <<'RS'
let t = Instant::now();
let t = SystemTime::now();
tracing::trace!("x");
COUNT.fetch_add(1, Relaxed);
let g = m.lock();
static M: Mutex<u8> = Mutex::new(0);
let r: RwLock<u8>;
let found = memchr(b'"', bytes).is_some() && COUNT.load(Ordering::Relaxed) > 0 && b.starts_with(b"usage");
let p = r"\"; let t = Instant::now(); let s = "a";
let q = r#"a " b"#; let t = Instant::now(); let s = "a";
RS
    cat >"$dir/good_general.rs" <<'RS'
tracing::warn!(status = ?status, "upstream {code} {req_id} {body_len}");
let v: Vec<u8> = it.flatten().collect();
let content = 1;
const S: &str = "#[serde(flatten)] ?headers span!(";
// #[serde(untagged)] tracing::warn!(?headers)
#[serde(borrow)]
    name: Cow<'a, str>,
    body: Cow<'a, str>, // hotpath-allow: function parameter, not a serde field
let c = '"'; let d = b'\''; let e = '\u{1F600}'; let f = b'\\';
fn f<'a>(x: &'a str) -> &'a str { x }
tracing::info!(
    status = ?status,
);
compile_error!("no");
RS
    cat >"$dir/good_r9.rs" <<'RS'
let t = Instant::now(); // hotpath-allow: the first chunk reads the clock once
let found = memchr(b'"', bytes).is_some();
let s = "Instant::now Mutex fetch_add";
// Ordering::Relaxed would be wrong here.
compile_error!("no");
RS

    local expected actual
    expected="$(printf '%s\n' \
        bad_general.rs:{1,2,3,4,5,6,7,8,11,13,15,17} \
        bad_r9.rs:{1,2,3,4,5,6,7,8,9,10})"
    actual="$(
        cd "$dir" &&
            "$awk_bin" -v r9_list="bad_r9.rs good_r9.rs" "$program" \
                bad_general.rs bad_r9.rs good_general.rs good_r9.rs |
            grep -o '^[a-z_0-9]*\.rs:[0-9]*' || true
    )"
    actual="$(printf '%s\n' "$actual" | tr -d '\r' | sort -u -t: -k1,1 -k2,2n)"
    expected="$(printf '%s\n' "$expected" | sort -u -t: -k1,1 -k2,2n)"
    if [[ "$actual" != "$expected" ]]; then
        echo "check-hotpath: self-test failed with $awk_bin; the rules do not behave as written" >&2
        diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
        echo "(< expected but not reported, > reported but not expected)" >&2
        return 1
    fi
}

self_test

# Capture first so a grep failure aborts the script instead of looking clean.
# The empty pattern matches every line, so this lists every non-empty file.
file_list="$(grep -rl --include='*.rs' -e '' "${roots[@]}" | sort)"
file_list="${file_list//$'\r'/}"
files=()
while IFS= read -r file; do
    files+=("$file")
done <<<"$file_list"

if "$awk_bin" -v r9_list="${r9_files[*]}" "$program" "${files[@]}"; then
    echo "check-hotpath: OK (${#files[@]} files under ${roots[*]}; self-test passed with $awk_bin)"
else
    echo "check-hotpath: violations found (rules: R3, R17, R9, Cow borrow; see the header of $0)" >&2
    exit 1
fi
