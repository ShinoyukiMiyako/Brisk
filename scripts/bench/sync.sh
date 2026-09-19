#!/usr/bin/env bash
# Copies the working tree to the benchmark host and builds the three bench
# binaries there in release mode.
#
# Usage: scripts/bench/sync.sh [--no-build] [--profile <cargo profile>]
#
# Environment:
#   BENCH_HOST          host to sync to (default 192.168.10.180)
#   BENCH_SSH_OPTS      extra ssh options (default "-o BatchMode=yes")
#   BENCH_REMOTE_DIR    source directory on the host, replaced on every sync
#                       (default ~/brisk)
#   BENCH_TARGET_DIR    CARGO_TARGET_DIR on the host, kept between syncs so
#                       builds stay incremental (default ~/brisk-target)
#   BENCH_RESULTS_ROOT  session parent on the host; a sync is refused while
#                       run-m0.sh holds its lock there (default ~/bench-results)
#
# Directories are relative to the remote home (optionally written with a
# leading ~/) and may contain letters, digits, '.', '_' and '-' only.
# Runs from Git Bash on Windows as well as from Linux or macOS.

# The remote commands are composed here on purpose; only ~ is left for the
# remote shell to expand.
# shellcheck disable=SC2029

set -euo pipefail

usage() {
    sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

build=1
profile=release
while (($# > 0)); do
    case "$1" in
        --no-build) build=0 ;;
        --profile)
            (($# >= 2)) || { echo "sync: --profile needs a value" >&2; exit 2; }
            profile="$2"
            shift
            ;;
        -h | --help) usage; exit 0 ;;
        *) echo "sync: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done
[[ "$profile" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "sync: invalid profile '$profile'" >&2; exit 2; }

host="${BENCH_HOST:-192.168.10.180}"
read -r -a ssh_opts <<<"${BENCH_SSH_OPTS:--o BatchMode=yes}"
# Kept unexpanded here: the remote shell resolves ~ to the remote home.
remote_dir="${BENCH_REMOTE_DIR:-~/brisk}"
target_dir="${BENCH_TARGET_DIR:-~/brisk-target}"
results_root="${BENCH_RESULTS_ROOT:-~/bench-results}"

# The paths are spliced into remote command lines and remote_dir is replaced
# with rm -rf, so they must be plain paths below the remote home: no spaces or
# shell syntax, no absolute path, no . or .. segment that could reach the home
# itself or leave it.
check_remote_path() {
    local name="$1" path="$2" seg
    if [[ ! "$path" =~ ^(~/)?[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]]; then
        echo "sync: $name '$path' must be a relative path below the remote home" >&2
        exit 2
    fi
    IFS=/ read -r -a segs <<<"${path#\~/}"
    for seg in "${segs[@]}"; do
        if [[ "$seg" == . || "$seg" == .. ]]; then
            echo "sync: $name '$path' must not contain . or .. segments" >&2
            exit 2
        fi
    done
}
check_remote_path BENCH_REMOTE_DIR "$remote_dir"
check_remote_path BENCH_TARGET_DIR "$target_dir"
check_remote_path BENCH_RESULTS_ROOT "$results_root"

# cargo names the dev profile's output directory `debug`.
profile_dir="$profile"
[[ "$profile" == dev ]] && profile_dir=debug

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

# The host builds without .git, so the binaries' embedded commit reads
# `unknown`; run-m0.sh copies .sync-info and .sync-diff.patch into every
# session instead. The patch (tracked changes plus untracked files against
# HEAD) and its hash identify what a dirty tree actually synced.
meta="$(mktemp -d)"
trap 'rm -rf "$meta"' EXIT
commit="$(git rev-parse HEAD 2>/dev/null)" || commit=unknown
porcelain="$(git status --porcelain -- . 2>/dev/null)" || porcelain=""
dirty=false
[[ -n "$porcelain" ]] && dirty=true
{
    if [[ "$commit" != unknown ]]; then
        git diff HEAD --binary -- .
        while IFS= read -r -d '' file; do
            # --no-index exits 1 when the files differ, which they always do.
            git diff --no-index --binary -- /dev/null "$file" || (($? == 1))
        done < <(git ls-files --others --exclude-standard -z -- .)
    fi
} >"$meta/.sync-diff.patch"
diff_sha="$(sha256sum "$meta/.sync-diff.patch" | cut -d' ' -f1)"
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" || branch=unknown
cat >"$meta/.sync-info" <<EOF
commit=${commit}
dirty=${dirty}
diff_sha256=${diff_sha}
branch=${branch}
synced_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
synced_from=$(hostname)
EOF

# A running session executes its own copies of the binaries, but a build
# would still compete with it for CPU.
lock="${results_root}/.run-m0.lock"
lock_rc=0
ssh "${ssh_opts[@]}" "$host" "[ ! -f ${lock} ] || flock -n ${lock} true" || lock_rc=$?
case "$lock_rc" in
    0) ;;
    1)
        echo "sync: a run-m0.sh session (or a process left over from one) holds ${host}:${lock}; not syncing" >&2
        exit 1
        ;;
    *) echo "sync: checking ${host}:${lock} failed (status $lock_rc)" >&2; exit 1 ;;
esac

echo "sync: ${repo_root} -> ${host}:${remote_dir} (commit ${commit}$([[ $dirty == true ]] && echo ", dirty, diff ${diff_sha:0:12}"))"
# --exclude patterns are anchored to the archive root (./), except the
# *.local.toml pattern, which must match at any depth. The two metadata files
# are appended to the archive root from the temporary directory.
#
# Remote side: unpack into <dir>.new, then swap it in, so a failed transfer
# leaves the previous tree intact. The existing directory is removed only if
# it is not the home directory and an earlier sync created it.
tar -czf - \
    --exclude=./target \
    --exclude=./.git \
    --exclude=./docs \
    --exclude=./bench-results \
    --exclude='*.local.toml' \
    . -C "$meta" .sync-info .sync-diff.patch |
    ssh "${ssh_opts[@]}" "$host" \
        "set -e; dir=${remote_dir}; new=${remote_dir}.new; \
         rm -rf \"\$new\"; mkdir -p \"\$new\"; tar --warning=no-timestamp -xzf - -C \"\$new\"; \
         if [ -e \"\$dir\" ]; then \
             if [ \"\$(cd \"\$dir\" && pwd -P)\" = \"\$(cd && pwd -P)\" ]; then echo \"refusing to replace the home directory\" >&2; exit 1; fi; \
             if [ ! -f \"\$dir/.sync-info\" ]; then echo \"\$dir exists but was not created by sync.sh; not replacing it\" >&2; exit 1; fi; \
             rm -rf \"\$dir\"; \
         fi; \
         mv \"\$new\" \"\$dir\"; \
         chmod +x \"\$dir\"/scripts/*.sh \"\$dir\"/scripts/bench/*.sh"

if ((build == 0)); then
    echo "sync: build skipped"
    exit 0
fi

echo "sync: building brisk-mock, brisk-loadgen, brisk-floor (--profile ${profile}) on ${host}"
ssh "${ssh_opts[@]}" "$host" \
    "set -e; . ~/.cargo/env; cd ${remote_dir}; \
     CARGO_TARGET_DIR=${target_dir} cargo build --locked --profile ${profile} \
         -p brisk-mock -p brisk-loadgen -p brisk-floor; \
     ls -l ${target_dir}/${profile_dir}/brisk-mock ${target_dir}/${profile_dir}/brisk-loadgen \
         ${target_dir}/${profile_dir}/brisk-floor"
