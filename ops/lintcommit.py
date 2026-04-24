#!/usr/bin/env python3
# Checks that commit messages conform to conventional commits
# (https://www.conventionalcommits.org/).
#
# To run tests:
#
#     python -m pytest ops/tests/test_lintcommit.py

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field

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

    if subject != subject.lower():
        return "subject must be lowercase"

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
    body_start: int = 2
    if len(lines) > 1 and lines[1].strip() != "":
        warnings.append("missing blank line between subject and body")
        body_start = 1

    if len(lines) > body_start:
        body: str = "\n".join(lines[body_start:])
        warnings.extend(validate_body(body))

    return (error, warnings)


@dataclass
class CommitResult:
    """Result of validating a single commit."""

    sha: str
    subject: str
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass
class LintResult:
    """Result of linting a range of commits."""

    commits: list[CommitResult] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""
    empty: bool = False
    git_error: str = ""

    @property
    def has_errors(self) -> bool:
        return bool(self.git_error) or any(c.error for c in self.commits)


def lint_range(git_range: str, *, skip_dirty_check: bool = False) -> LintResult:
    """Validate commit messages in a git range (e.g. 'origin/main..HEAD').

    Args:
        git_range: A git revision range like 'origin/main..HEAD'.
        skip_dirty_check: When True, skip the uncommitted changes check
            (useful in CI where the worktree may be clean by definition).

    Returns:
        A LintResult with per-commit validation results.
    """
    if not skip_dirty_check:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
        )
        if status.stdout.strip():
            return LintResult(
                skipped=True,
                skip_reason=(
                    "uncommitted changes detected, skipping commit message validation.\n"
                    "Commit your changes and re-run to validate."
                ),
            )

    result = subprocess.run(
        ["git", "log", "--no-merges", git_range, "-z", "--format=%H%n%B"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return LintResult(git_error=result.stderr.strip())

    if not result.stdout.strip():
        return LintResult(empty=True)

    commits: list[CommitResult] = []
    for record in result.stdout.split("\0"):
        if not record.strip():
            continue
        sha, _, message = record.partition("\n")
        message = message.strip()
        if not message:
            continue

        error, warnings = validate_message(message)
        subject = message.splitlines()[0]
        commits.append(
            CommitResult(
                sha=sha[:7],
                subject=subject,
                error=error,
                warnings=warnings,
            )
        )

    return LintResult(commits=commits)


def write_output(lint_result: LintResult, git_range: str) -> None:
    """Write lint results to stdout/stderr."""
    if lint_result.skipped:
        print(f"WARNING: {lint_result.skip_reason}")
        return

    if lint_result.git_error:
        print(f"git log failed: {lint_result.git_error}", file=sys.stderr)
        return

    if lint_result.empty:
        print(f"No commits in range {git_range}")
        return

    for commit in lint_result.commits:
        if commit.error:
            print(f"FAIL {commit.sha}: {commit.subject}", file=sys.stderr)
            print(f"  Error: {commit.error}", file=sys.stderr)
        else:
            print(f"PASS {commit.sha}: {commit.subject}")

        for warning in commit.warnings:
            print(f"  Warning: {warning}")


def run_range(git_range: str, *, skip_dirty_check: bool = False) -> None:
    """Validate commit messages in a git range and exit on errors."""
    lint_result = lint_range(git_range, skip_dirty_check=skip_dirty_check)
    write_output(lint_result, git_range)
    if lint_result.has_errors:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lint commit messages for conventional commits compliance."
    )
    parser.add_argument(
        "--range",
        default=None,
        dest="git_range",
        help="Validate all commits in a git revision range (e.g. 'origin/main..HEAD'). "
        "Skips the uncommitted-changes check (useful in CI).",
    )
    args = parser.parse_args()

    if args.git_range is not None:
        run_range(args.git_range, skip_dirty_check=True)
    else:
        run_range("origin/main..HEAD")


if __name__ == "__main__":
    main()
