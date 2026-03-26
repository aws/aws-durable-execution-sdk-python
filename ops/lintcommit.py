#!/usr/bin/env python3
# Checks that commit messages conform to conventional commits
# (https://www.conventionalcommits.org/).
#
# To run tests:
#
#     python -m pytest ops/__tests__/test_lintcommit.py

from __future__ import annotations

import os
import re
import sys

TYPES: set[str] = {
    "build",
    "chore",
    "ci",
    "deps",
    "docs",
    "feat",
    "fix",
    "perf",
    "refactor",
    "style",
    "test",
}

MAX_SUBJECT_LENGTH: int = 50
MAX_SCOPE_LENGTH: int = 30
MAX_BODY_LINE_LENGTH: int = 72


def validate_subject(subject_line: str) -> str | None:
    """Validate a commit message subject line.

    Returns None if valid, else an error message string.
    """
    parts: list[str] = subject_line.split(":", maxsplit=1)

    if len(parts) < 2:
        return "missing colon (:) char"

    type_scope: str = parts[0]
    subject: str = parts[1].strip()

    # Parse type and optional scope: type or type(scope)
    scope: str | None = None
    commit_type: str = type_scope

    if "(" in type_scope:
        paren_start: int = type_scope.index("(")
        commit_type = type_scope[:paren_start]

        if not type_scope.endswith(")"):
            return "must be formatted like type(scope):"

        scope = type_scope[paren_start + 1 : -1]

    if " " in commit_type:
        return f'type contains whitespace: "{commit_type}"'

    if commit_type not in TYPES:
        return f'invalid type "{commit_type}"'

    if scope is not None:
        if len(scope) > MAX_SCOPE_LENGTH:
            return f"invalid scope (must be <={MAX_SCOPE_LENGTH} chars)"

        if re.search(r"[^- a-z0-9]", scope):
            return f'invalid scope (must be lowercase, ascii only): "{scope}"'

    if len(subject) == 0:
        return "empty subject"

    if len(subject) > MAX_SUBJECT_LENGTH:
        return f"invalid subject (must be <={MAX_SUBJECT_LENGTH} chars)"

    if subject.endswith("."):
        return "subject must not end with a period"

    return None


def validate_body(body: str) -> list[str]:
    """Validate the body of a commit message.

    Returns a list of warnings (not hard errors) for body issues.
    """
    warnings: list[str] = []
    for i, line in enumerate(body.splitlines(), start=1):
        if len(line) > MAX_BODY_LINE_LENGTH:
            warnings.append(
                f"body line {i} exceeds {MAX_BODY_LINE_LENGTH} chars ({len(line)} chars)"
            )
    return warnings


def validate_message(message: str) -> tuple[str | None, list[str]]:
    """Validate a full commit message (subject + optional body).

    Returns (error, warnings) where error is None if the subject is valid.
    """
    lines: list[str] = message.strip().splitlines()
    if not lines:
        return ("empty commit message", [])

    subject_line: str = lines[0]
    error: str | None = validate_subject(subject_line)

    warnings: list[str] = []
    # Check for blank line between subject and body
    if len(lines) > 1 and lines[1].strip() != "":
        warnings.append("missing blank line between subject and body")

    if len(lines) > 2:
        body: str = "\n".join(lines[2:])
        warnings.extend(validate_body(body))

    return (error, warnings)


def _format_error(title: str, reason: str) -> str:
    valid_types: str = ", ".join(sorted(TYPES))
    return f"""Invalid commit message: `{title}`

* Problem: {reason}
* Expected format: `type(scope): subject...`
    * type: one of ({valid_types})
    * scope: optional, lowercase, <{MAX_SCOPE_LENGTH} chars
    * subject: must be <{MAX_SUBJECT_LENGTH} chars
"""


def run_local() -> None:
    """Validate local commit messages ahead of origin/main.

    If there are uncommitted changes, prints a warning and skips validation.
    """
    import subprocess

    # Check for uncommitted changes
    status: subprocess.CompletedProcess[str] = subprocess.run(
        ["git", "status", "--porcelain"],
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        print(
            "WARNING: uncommitted changes detected, skipping commit message validation.\n"
            "Commit your changes and re-run to validate."
        )
        return

    # Get all commit messages ahead of origin/main
    result: subprocess.CompletedProcess[str] = subprocess.run(
        ["git", "log", "origin/main..HEAD", "--format=%H%n%B%n---END---"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"git log failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    raw: str = result.stdout.strip()
    if not raw:
        print("No local commits ahead of origin/main")
        return

    blocks: list[str] = raw.split("---END---")
    has_errors: bool = False

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines: list[str] = block.splitlines()
        sha: str = lines[0][:7]
        message: str = "\n".join(lines[1:]).strip()

        if not message:
            continue

        error, warnings = validate_message(message)
        subject: str = message.splitlines()[0]

        if error:
            print(f"FAIL {sha}: {subject}", file=sys.stderr)
            print(f"  Error: {error}", file=sys.stderr)
            has_errors = True
        else:
            print(f"PASS {sha}: {subject}")

        for warning in warnings:
            print(f"  Warning: {warning}")

    if has_errors:
        sys.exit(1)


def main() -> None:
    run_local()


if __name__ == "__main__":
    main()
