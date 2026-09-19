#!/usr/bin/env python3
"""Emit a GitHub Actions error annotation for every failed test in a JUnit report.

Job logs of a public repository need a signed-in viewer, but annotations are
served by the public check-runs API, so failures stay diagnosable either way.
"""

import sys
import xml.etree.ElementTree as ET

# GitHub keeps at most 10 error annotations per step.
MAX_ANNOTATIONS = 10
MAX_CHARS = 4000


def escape_data(text: str) -> str:
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(text: str) -> str:
    return escape_data(text).replace(":", "%3A").replace(",", "%2C")


def failure_text(case: ET.Element) -> str:
    parts = []
    for tag in ("failure", "error"):
        for node in case.findall(tag):
            if node.get("message"):
                parts.append(node.get("message", ""))
            if node.text:
                parts.append(node.text)
    for tag in ("system-out", "system-err"):
        for node in case.findall(tag):
            if node.text:
                parts.append(node.text)
    text = "\n".join(part.strip() for part in parts if part.strip())
    # The end of the output carries the panic message and assertion values.
    return text[-MAX_CHARS:]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: junit-annotations.py <junit.xml>", file=sys.stderr)
        return 2
    try:
        root = ET.parse(sys.argv[1]).getroot()
    except FileNotFoundError:
        print(f"::warning::no JUnit report at {sys.argv[1]}")
        return 0
    failed = [
        case
        for case in root.iter("testcase")
        if case.find("failure") is not None or case.find("error") is not None
    ]
    for case in failed[:MAX_ANNOTATIONS]:
        title = f"{case.get('classname', '')} {case.get('name', '')}".strip()
        print(f"::error title={escape_property(title)}::{escape_data(failure_text(case))}")
    if len(failed) > MAX_ANNOTATIONS:
        names = ", ".join(case.get("name", "") for case in failed[MAX_ANNOTATIONS:])
        print(f"::error title=more failures::{escape_data(names)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
