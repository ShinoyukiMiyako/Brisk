#!/usr/bin/env bash
# Copies a session directory from the benchmark host into bench-results/
# of this checkout (gitignored).
#
# Usage: scripts/bench/pull.sh <run-id>
#        scripts/bench/pull.sh --list
#
# Environment:
#   BENCH_HOST          host to pull from (default 192.168.10.180)
#   BENCH_SSH_OPTS      extra ssh options (default "-o BatchMode=yes")
#   BENCH_RESULTS_ROOT  session parent on the host (default ~/bench-results)
#
# An existing local copy of the session is replaced. Runs from Git Bash on
# Windows as well as from Linux or macOS.

# The remote commands are composed here on purpose; only ~ is left for the
# remote shell to expand.
# shellcheck disable=SC2029

set -euo pipefail

usage() {
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

(($# == 1)) || { usage >&2; exit 2; }
host="${BENCH_HOST:-192.168.10.180}"
read -r -a ssh_opts <<<"${BENCH_SSH_OPTS:--o BatchMode=yes}"
remote_root="${BENCH_RESULTS_ROOT:-~/bench-results}"

case "$1" in
    -h | --help) usage; exit 0 ;;
    --list)
        ssh "${ssh_opts[@]}" "$host" "ls -1t ${remote_root}"
        exit 0
        ;;
esac

run_id="$1"
# The id becomes part of a remote command line and a local path.
[[ "$run_id" =~ ^[A-Za-z0-9._-]+$ && "$run_id" != . && "$run_id" != .. ]] ||
    { echo "pull: invalid run id '$run_id'" >&2; exit 2; }

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest_root="$repo_root/bench-results"
mkdir -p "$dest_root"
staging="$(mktemp -d "$dest_root/.pull-XXXXXX")"
trap 'rm -rf "$staging"' EXIT

if ! ssh "${ssh_opts[@]}" "$host" "test -d ${remote_root}/${run_id}"; then
    echo "pull: ${host}:${remote_root}/${run_id} not found (see --list)" >&2
    exit 1
fi
ssh "${ssh_opts[@]}" "$host" "tar -czf - -C ${remote_root} ${run_id}" | tar -xzf - -C "$staging"

rm -rf "${dest_root:?}/$run_id"
mv "$staging/$run_id" "$dest_root/$run_id"
echo "pull: ${host}:${remote_root}/${run_id} -> $dest_root/$run_id ($(du -sh "$dest_root/$run_id" | cut -f1))"
