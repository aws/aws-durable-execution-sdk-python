#!/usr/bin/env python3
# Checks that commit messages conform to conventional commits
# (https://www.conventionalcommits.org/).
#
# To run self-tests:
#
#     python ops/lintcommit.py test

from __future__ import annotations

import os
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
    if subject_line.startswith("Merge"):
        return None

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

        import re

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


def _test() -> None:
    """Run self-tests."""
    subject_tests: dict[str, str | None] = {
        " foo(scope): bar": 'type contains whitespace: " foo"',
        "build: update build process": None,
        "chore: update dependencies": None,
        "ci: configure CI/CD": None,
        "config: update configuration files": 'invalid type "config"',
        "deps: bump aws-sdk group with 5 updates": None,
        "docs: update documentation": None,
        "feat(sdk): add new feature": None,
        "feat(sdk):": "empty subject",
        "feat foo):": 'type contains whitespace: "feat foo)"',
        "feat(foo)): sujet": 'invalid scope (must be lowercase, ascii only): "foo)"',
        "feat(foo: sujet": "must be formatted like type(scope):",
        "feat(Q Foo Bar): bar": 'invalid scope (must be lowercase, ascii only): "Q Foo Bar"',
        "feat(sdk): bar": None,
        "feat: foo": None,
        "fix: foo": None,
        "fix(sdk): resolve issue": None,
        "foo (scope): bar": 'type contains whitespace: "foo "',
        "invalid title": "missing colon (:) char",
        "perf: optimize performance": None,
        "refactor: improve code structure": None,
        "revert: feat: add new feature": 'invalid type "revert"',
        "style: format code": None,
        "test: add new tests": None,
        "types: add type definitions": 'invalid type "types"',
        "Merge staging into feature/lambda-get-started": None,
        "feat(foo): fix the types": None,
        "chore(deps): bump the aws-sdk group with 5 updates": None,
        "chore(deps-dev): bump flatted from 3.4.1 to 3.4.2": None,
        "feat: add thing.": "subject must not end with a period",
    }

    passed: int = 0
    failed: int = 0

    for title, expected in subject_tests.items():
        result: str | None = validate_subject(title)
        if result == expected:
            print(f'PASS: "{title}"')
            passed += 1
        else:
            print(f'FAIL: "{title}" (expected "{expected}", got "{result}")')
            failed += 1
    # Body validation tests
    body_tests: list[tuple[str, bool, int]] = [
        # (message, expect_error, expected_warning_count)
        ("feat: add thing\n\nShort body line.", False, 0),
        ("feat: add thing\nNo blank line.", False, 1),
        ("feat: add thing\n\n" + "x" * 80, False, 1),
    ]

    for message, expect_error, expected_warnings in body_tests:
        error, warnings = validate_message(message)
        has_error: bool = error is not None
        if has_error == expect_error and len(warnings) == expected_warnings:
            print(f'PASS: body "{message[:40]}..."')
            passed += 1
        else:
            print(
                f'FAIL: body "{message[:40]}..." '
                f"expected error={expect_error}, expected warnings={expected_warnings})"
            )
            failed += 1

    print(f"\n{passed} tests passed, {failed} tests failed")
    if failed > 0:
        sys.exit(1)


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        _test()
    else:
        run_local()


if __name__ == "__main__":
    main()
