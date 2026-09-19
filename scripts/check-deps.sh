#!/usr/bin/env bash
# R1: the data-plane crate must not pull in database, password-hashing or
# admin-plane crates. Fails (exit 1) if any forbidden crate appears among the
# normal (non-dev, non-build) dependencies of brisk-gateway.
#
# Works on Linux, macOS and Git Bash on Windows; needs only cargo and grep.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

forbidden=(sqlx rusqlite argon2 brisk-admin)

# Capture first so a cargo failure aborts the script instead of looking clean.
# --target all: cfg-gated (platform-specific) dependencies must be checked too,
# not only those of the host running the script.
tree="$(cargo tree -p brisk-gateway -e normal --prefix none --target all)"
# cargo on Windows emits CRLF line endings.
tree="${tree//$'\r'/}"

status=0
for name in "${forbidden[@]}"; do
    # Lines look like "<crate> v<version> ...". Match the crate name exactly or
    # as a family prefix (sqlx-core, sqlx-postgres, ...), but not unrelated
    # crates that merely contain the name as a substring.
    if matches="$(grep -E "^${name}(-[a-z0-9_-]+)? v" <<<"$tree" | sort -u)"; then
        echo "check-deps: forbidden dependency '${name}' found in brisk-gateway:" >&2
        echo "$matches" | sed 's/^/  /' >&2
        status=1
    fi
done

if [[ $status -eq 0 ]]; then
    echo "check-deps: OK (none of ${forbidden[*]} in brisk-gateway normal deps)"
fi
exit "$status"
