#!/usr/bin/env bash
# Dependency rules. Fails (exit 1) if any rule is broken:
#
# - R1: the data-plane crate must not pull in database, password-hashing or
#   admin-plane crates, so none of them may appear among the normal
#   (non-dev, non-build) dependencies of brisk-gateway.
# - brisk-proto stays synchronous: tokio, hyper, reqwest and axum (or any
#   crate of their families) must not appear among its normal dependencies.
#
# Works on Linux, macOS and Git Bash on Windows; needs only cargo and grep.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

status=0

# check_forbidden <package> <reason> <crate>...
check_forbidden() {
    local package="$1" reason="$2"
    shift 2

    # Capture first so a cargo failure aborts the script instead of looking
    # clean. --target all: cfg-gated (platform-specific) dependencies must be
    # checked too, not only those of the host running the script.
    local tree
    tree="$(cargo tree -p "$package" -e normal --prefix none --target all)"
    # cargo on Windows emits CRLF line endings.
    tree="${tree//$'\r'/}"

    local name matches line found=0
    for name in "$@"; do
        # Lines look like "<crate> v<version> ...". Match the crate name
        # exactly or as a family prefix (sqlx-core, tokio-util, ...), but not
        # unrelated crates that merely contain the name as a substring.
        if matches="$(grep -E "^${name}(-[a-z0-9_-]+)? v" <<<"$tree" | sort -u)"; then
            echo "check-deps: forbidden dependency '${name}' found in ${package} (${reason}):" >&2
            while IFS= read -r line; do
                echo "  $line" >&2
            done <<<"$matches"
            found=1
        fi
    done

    if [[ $found -eq 0 ]]; then
        echo "check-deps: OK (none of $* in ${package} normal deps)"
    else
        status=1
    fi
}

check_forbidden brisk-gateway "R1" sqlx rusqlite argon2 brisk-admin
check_forbidden brisk-proto "brisk-proto must stay synchronous" tokio hyper reqwest axum

exit "$status"
