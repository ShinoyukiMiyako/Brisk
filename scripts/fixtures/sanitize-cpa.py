#!/usr/bin/env python3
"""Sanitise captured CPA responses into the fixtures under fixtures/cpa, or check them.

    sanitize-cpa.py SRC_DIR DST_DIR
    sanitize-cpa.py --check DIR [--secret-env NAME]...

SRC_DIR holds the raw captures (docs/samples/cpa, local only). The first form
writes the sanitised copies listed in FIXTURES to DST_DIR and prints the
README table rows (name, source, size, SHA-256) for them.

Every replacement keeps the length of the value it replaces, so line endings,
SSE framing and byte offsets match the capture exactly and a fixture can stand
in for the capture byte for byte. The output depends on the input only, so
running the script again reproduces the same bytes.

--check verifies the rules on every file in DIR, that DIR holds exactly the
expected files, and that README.md lists the SHA-256 of each one.
--secret-env NAME additionally fails if any file contains the value of the
environment variable NAME. CPA api-keys are arbitrary operator-chosen strings
that no prefix check can recognise, so the operator passes the real key this
way; only file names are reported, never the value.

Standard library only (contract 05, section 4.4).
"""

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

# (capture stem, fixture stem, body suffix, keep the .headers file)
FIXTURES = (
    ("grok46-xhigh-chat-stream", "chat-stream-grok46-xhigh", ".sse", True),
    ("grok46-suffix-chat-stream", "chat-stream-grok46-suffix", ".sse", True),
    ("grok-chat-stream", "chat-stream-grok43", ".sse", False),
    ("openai-chat-stream", "chat-stream-gpt55", ".sse", True),
    ("grok46-xhigh-chat-nonstream", "chat-nonstream-grok46-xhigh", ".json", True),
    ("openai-chat-nonstream", "chat-nonstream-gpt55", ".json", False),
    ("error-bad-key", "error-bad-key", ".json", True),
    ("grok46-bogus-effort", "error-bogus-effort", ".json", True),
    ("grok46-xhigh-responses-stream", "responses-stream-grok46-xhigh", ".sse", False),
    ("grok46-suffix-anthropic-stream", "anthropic-stream-grok46-suffix", ".sse", False),
    ("grok46-suffix-gemini-sse", "gemini-sse-grok46-suffix", ".sse", False),
)

README = "README.md"

WS = rb"[ \t\r\n]*"
# JSON string members whose value identifies a request or response. Values
# with escapes are not matched on purpose: --check then reports them as
# unsanitised instead of this script rewriting them incorrectly.
ID_MEMBER = re.compile(rb'"(id|item_id|responseId|response_id)"' + WS + b":" + WS + rb'"([^"\\]*)"')
ID_KEY = re.compile(rb'"(?:id|item_id|responseId|response_id)"' + WS + b":" + WS)
# Member name -> prefix that is kept; everything after it becomes "0".
ZEROED_MEMBERS = {
    b"prompt_cache_key": b"",
    b"safety_identifier": b"user-",
    b"system_fingerprint": b"fp_",
}
ZEROED_MEMBER = re.compile(
    rb'"(' + b"|".join(re.escape(name) for name in ZEROED_MEMBERS) + rb')"' + WS + b":" + WS + rb'"([^"\\]*)"'
)
ZEROED_KEY = re.compile(
    rb'"(?:' + b"|".join(re.escape(name) for name in ZEROED_MEMBERS) + rb')"' + WS + b":" + WS
)
# The trace id is <timestamp>-<hash>-<hash>; the hashes include an index of
# the upstream credential, the timestamp does not.
TRACE_ID = re.compile(rb"(?im)^(x-cpa-trace-id:[ \t]*)([0-9]+)-([0-9A-Za-z]+)-([0-9A-Za-z]+)([ \t]*\r?)$")
TRACE_ID_LINE = re.compile(rb"(?im)^x-cpa-trace-id:.*$")
CREDENTIAL_TRACES = (
    ("a bk- key", re.compile(rb"(?<![A-Za-z0-9_])bk-[A-Za-z0-9]")),
    ("an sk- key", re.compile(rb"(?<![A-Za-z0-9_])sk-[A-Za-z0-9]")),
    ("a Bearer credential", re.compile(rb"(?i)bearer[ \t]")),
    ("an x-api-key header", re.compile(rb"(?i)x-api-key")),
    ("an x-goog-api-key header", re.compile(rb"(?i)x-goog-api-key")),
    ("an authorization header", re.compile(rb"(?im)^authorization[ \t]*:")),
    ("a cookie header", re.compile(rb"(?im)^(?:set-)?cookie[ \t]*:")),
    ("a credential query parameter", re.compile(rb"(?i)[?&](?:key|auth_token)=")),
)
README_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|.*\|\s*`([0-9a-f]{64})`\s*\|\s*$")


class SanitiseError(Exception):
    pass


def expected_names() -> list[str]:
    names = []
    for _, stem, suffix, headers in FIXTURES:
        names.append(stem + suffix)
        if headers:
            names.append(stem + ".headers")
    return names


def placeholder(rest: bytes, serial: int) -> bytes:
    """`serial` in decimal, left-padded with zeros, laid over the non-hyphen
    positions of `rest`."""
    slots = sum(1 for byte in rest if byte != ord("-"))
    digits = str(serial).encode()
    if len(digits) > slots:
        raise SanitiseError(f"id {rest!r} is too short for serial {serial}")
    digits = digits.rjust(slots, b"0")
    out = bytearray()
    taken = 0
    for byte in rest:
        if byte == ord("-"):
            out.append(byte)
        else:
            out.append(digits[taken])
            taken += 1
    return bytes(out)


def sanitise_ids(data: bytes) -> bytes:
    # Keyed by the part after the prefix: `rs_<uuid>`, `msg_<uuid>` and the
    # bare `<uuid>` of one response keep sharing a placeholder, as they
    # shared the original.
    serials: dict[bytes, int] = {}

    def replace(match: re.Match) -> bytes:
        value = match.group(2)
        underscore = value.find(b"_")
        prefix, rest = (value[: underscore + 1], value[underscore + 1 :]) if underscore >= 0 else (b"", value)
        if not rest:
            raise SanitiseError(f"id value {value!r} has nothing to replace")
        serial = serials.setdefault(rest, len(serials) + 1)
        head = match.group(0)[: match.start(2) - match.start()]
        return head + prefix + placeholder(rest, serial) + b'"'

    return ID_MEMBER.sub(replace, data)


def sanitise_zeroed(data: bytes) -> bytes:
    def replace(match: re.Match) -> bytes:
        name, value = match.group(1), match.group(2)
        prefix = ZEROED_MEMBERS[name]
        kept = prefix if value.startswith(prefix) else b""
        rest = value[len(kept) :]
        # The cache key is a UUID and becomes the all-zero UUID; the other
        # values are opaque strings and become zeros throughout.
        keep_hyphens = name == b"prompt_cache_key"
        zeroed = bytes(byte if keep_hyphens and byte == ord("-") else ord("0") for byte in rest)
        head = match.group(0)[: match.start(2) - match.start()]
        return head + kept + zeroed + b'"'

    return ZEROED_MEMBER.sub(replace, data)


def sanitise_headers(data: bytes) -> bytes:
    def replace(match: re.Match) -> bytes:
        return (
            match.group(1)
            + match.group(2)
            + b"-"
            + b"0" * len(match.group(3))
            + b"-"
            + b"0" * len(match.group(4))
            + match.group(5)
        )

    out = TRACE_ID.sub(replace, data)
    if len(TRACE_ID_LINE.findall(data)) != len(TRACE_ID.findall(data)):
        raise SanitiseError("an X-Cpa-Trace-Id header does not look like <timestamp>-<hash>-<hash>")
    return out


def sanitise(data: bytes, is_headers: bool) -> bytes:
    out = sanitise_headers(data) if is_headers else sanitise_zeroed(sanitise_ids(data))
    if len(out) != len(data):
        raise SanitiseError("a replacement changed the length of the file")
    return out


def json_string_after(data: bytes, pos: int) -> bytes | None:
    """The raw content of the JSON string starting at `pos`, or None if the
    value there is not a string without escapes."""
    if data[pos : pos + 1] != b'"':
        return None
    end = data.find(b'"', pos + 1)
    if end < 0:
        return None
    value = data[pos + 1 : end]
    return None if b"\\" in value else value


def id_is_placeholder(value: bytes) -> bool:
    underscore = value.find(b"_")
    rest = value[underscore + 1 :] if underscore >= 0 else value
    digits = rest.replace(b"-", b"")
    # Placeholders are small serials padded with zeros; a real hex id or UUID
    # is never all digits with this many leading zeros.
    return bool(digits) and digits.isdigit() and len(digits.lstrip(b"0")) <= 6


def check_secrets(name: str, data: bytes, secrets: dict[str, bytes]) -> list[str]:
    return [f"{name}: contains the value of ${env}" for env, secret in secrets.items() if secret in data]


def check_fixture(name: str, data: bytes) -> list[str]:
    problems = []
    for match in ID_KEY.finditer(data):
        if data[match.end() : match.end() + 1] != b'"':
            continue
        value = json_string_after(data, match.end())
        if value is None or not id_is_placeholder(value):
            line = data.count(b"\n", 0, match.start()) + 1
            problems.append(f"{name}:{line}: id value is not a sanitised placeholder")
    for match in ZEROED_KEY.finditer(data):
        if data[match.end() : match.end() + 1] != b'"':
            continue
        value = json_string_after(data, match.end())
        member = match.group(0).split(b'"')[1]
        prefix = ZEROED_MEMBERS[member]
        rest = value[len(prefix) :] if value is not None and value.startswith(prefix) else value
        if rest is None or rest.strip(b"0-") != b"":
            line = data.count(b"\n", 0, match.start()) + 1
            problems.append(f"{name}:{line}: {member.decode()} is not zeroed")
    for match in TRACE_ID_LINE.finditer(data):
        parsed = TRACE_ID.match(data, match.start())
        if parsed is None or parsed.group(3).strip(b"0") or parsed.group(4).strip(b"0"):
            line = data.count(b"\n", 0, match.start()) + 1
            problems.append(f"{name}:{line}: X-Cpa-Trace-Id keeps its hash segments")
    for what, pattern in CREDENTIAL_TRACES:
        for match in pattern.finditer(data):
            line = data.count(b"\n", 0, match.start()) + 1
            problems.append(f"{name}:{line}: looks like {what}")
    return problems


def check_readme(directory: Path, names: list[str]) -> list[str]:
    readme = directory / README
    if not readme.is_file():
        return [f"{README} is missing"]
    listed: dict[str, str] = {}
    for line in readme.read_text(encoding="utf-8").splitlines():
        row = README_ROW.match(line)
        if row:
            listed[row.group(1)] = row.group(2)
    problems = []
    for name in names:
        digest = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        if name not in listed:
            problems.append(f"{README}: no SHA-256 row for {name}")
        elif listed[name] != digest:
            problems.append(f"{README}: SHA-256 of {name} does not match the file")
    for name in sorted(set(listed) - set(names)):
        problems.append(f"{README}: lists {name}, which is not a fixture")
    return problems


def run_check(directory: Path, secret_envs: list[str]) -> int:
    secrets: dict[str, bytes] = {}
    for env in secret_envs:
        value = os.environ.get(env)
        if not value:
            print(f"sanitize-cpa: --secret-env {env}: the variable is not set or empty", file=sys.stderr)
            return 2
        secrets[env] = value.encode()
    if not directory.is_dir():
        print(f"sanitize-cpa: {directory} is not a directory", file=sys.stderr)
        return 2

    names = expected_names()
    present = sorted(entry.name for entry in directory.iterdir())
    problems = []
    for name in sorted(set(names) - set(present)):
        problems.append(f"{name}: missing")
    for name in sorted(set(present) - set(names) - {README}):
        problems.append(f"{name}: not a known fixture")
    for name in present:
        path = directory / name
        if not path.is_file():
            continue
        data = path.read_bytes()
        # README.md documents the credential patterns, so only the secret
        # values are looked for in it.
        if name != README:
            problems.extend(check_fixture(name, data))
        problems.extend(check_secrets(name, data, secrets))
    problems.extend(check_readme(directory, [name for name in names if name in present]))

    for problem in problems:
        print(f"sanitize-cpa: {problem}", file=sys.stderr)
    if problems:
        return 1
    extra = f", no value of {', '.join('$' + env for env in secret_envs)}" if secret_envs else ""
    print(f"sanitize-cpa: {len(names)} fixtures in {directory} pass{extra}")
    return 0


def run_sanitise(source: Path, target: Path) -> int:
    target.mkdir(parents=True, exist_ok=True)
    rows = []
    for capture, stem, suffix, headers in FIXTURES:
        pairs = [(capture + ".body", stem + suffix, False)]
        if headers:
            pairs.append((capture + ".headers", stem + ".headers", True))
        for source_name, target_name, is_headers in pairs:
            data = (source / source_name).read_bytes()
            try:
                out = sanitise(data, is_headers)
            except SanitiseError as error:
                print(f"sanitize-cpa: {source_name}: {error}", file=sys.stderr)
                return 1
            (target / target_name).write_bytes(out)
            digest = hashlib.sha256(out).hexdigest()
            rows.append(f"| `{target_name}` | `{source_name}` | {len(out)} | `{digest}` |")
    print("\n".join(rows))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", metavar="DIR", type=Path, help="check the fixtures in DIR instead of writing them")
    parser.add_argument(
        "--secret-env",
        metavar="NAME",
        action="append",
        default=[],
        help="with --check: fail if a fixture contains the value of this environment variable (repeatable)",
    )
    parser.add_argument("dirs", nargs="*", type=Path, metavar="SRC_DIR DST_DIR")
    args = parser.parse_args()

    if args.check is not None:
        if args.dirs:
            parser.error("--check takes no other directories")
        return run_check(args.check, args.secret_env)
    if args.secret_env:
        parser.error("--secret-env only applies to --check")
    if len(args.dirs) != 2:
        parser.error("expected SRC_DIR and DST_DIR")
    return run_sanitise(args.dirs[0], args.dirs[1])


if __name__ == "__main__":
    sys.exit(main())
