#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from subprocess import CompletedProcess
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest

from lintcommit import lint_range, validate_message, validate_subject


# region validate_subject: valid subjects


def test_valid_feat() -> None:
    assert validate_subject("feat: add new feature") is None


def test_valid_fix() -> None:
    assert validate_subject("fix: resolve issue") is None


def test_valid_fix_with_scope() -> None:
    assert validate_subject("fix(sdk): resolve issue") is None


def test_valid_build() -> None:
    assert validate_subject("build: update build process") is None


def test_valid_chore() -> None:
    assert validate_subject("chore: update dependencies") is None


def test_valid_ci() -> None:
    assert validate_subject("ci: configure ci/cd") is None


def test_valid_deps() -> None:
    assert validate_subject("deps: bump aws-sdk group with 5 updates") is None


def test_valid_docs() -> None:
    assert validate_subject("docs: update documentation") is None


def test_valid_feat_with_scope() -> None:
    assert validate_subject("feat(sdk): add new feature") is None


def test_valid_feat_scope_bar() -> None:
    assert validate_subject("feat(sdk): bar") is None


def test_valid_feat_foo() -> None:
    assert validate_subject("feat: foo") is None


def test_valid_fix_foo() -> None:
    assert validate_subject("fix: foo") is None


# region validate_subject: invalid subjects


def test_invalid_type() -> None:
    assert validate_subject("config: foo") == 'invalid type "config"'


def test_missing_colon() -> None:
    assert validate_subject("invalid title") == "missing colon (:) char"


def test_period_at_end() -> None:
    assert validate_subject("feat: add thing.") == "subject must not end with a period"


def test_empty_subject() -> None:
    assert validate_subject("feat: ") == "empty subject"


def test_subject_too_long() -> None:
    long_subject: str = "feat: " + "a" * 51
    result = validate_subject(long_subject)
    assert result is not None
    assert "invalid subject" in result


def test_type_with_whitespace() -> None:
    assert validate_subject("fe at: foo") == 'type contains whitespace: "fe at"'


def test_scope_not_closed() -> None:
    assert validate_subject("feat(sdk: foo") == "must be formatted like type(scope):"


def test_scope_too_long() -> None:
    long_scope: str = "a" * 31
    result = validate_subject(f"feat({long_scope}): foo")
    assert result is not None
    assert "invalid scope" in result


def test_scope_uppercase() -> None:
    result = validate_subject("feat(SDK): foo")
    assert result is not None
    assert "invalid scope" in result


def test_subject_uppercase() -> None:
    assert validate_subject("feat: Add new feature") == "subject must be lowercase"


# region validate_message


def test_valid_subject_only() -> None:
    error, warnings = validate_message("feat: add thing")
    assert error is None
    assert warnings == []


def test_valid_with_body() -> None:
    error, warnings = validate_message("feat: add thing\n\nThis is the body.")
    assert error is None
    assert warnings == []


def test_missing_blank_line() -> None:
    _, warnings = validate_message("feat: add thing\nNo blank line.")
    assert "missing blank line between subject and body" in warnings


def test_missing_blank_line_body_still_checked() -> None:
    _, warnings = validate_message("feat: add thing\n" + "x" * 80)
    assert "missing blank line between subject and body" in warnings
    assert any("exceeds 72 chars" in w for w in warnings), (
        "body line length should be checked even without blank line"
    )


def test_long_body_line() -> None:
    _, warnings = validate_message("feat: add thing\n\n" + "x" * 80)
    assert len(warnings) == 1
    assert "exceeds 72 chars" in warnings[0]


def test_empty_message() -> None:
    error, _ = validate_message("")
    assert error == "empty commit message"


def test_invalid_subject_in_message() -> None:
    error, _ = validate_message("invalid title")
    assert error == "missing colon (:) char"


# region lint_range


def _make_git_log_output(*messages: str) -> str:
    """Build fake ``git log --no-merges -z --format=%H%n%B`` output.

    Records are separated by null characters.
    """
    records: list[str] = []
    for i, msg in enumerate(messages):
        sha = f"abc{i:04d}" + "0" * 33  # 40-char fake SHA
        records.append(f"{sha}\n{msg}\n")
    return "\0".join(records)


def _completed(
    stdout: str = "", stderr: str = "", returncode: int = 0
) -> CompletedProcess[str]:
    """Shorthand for a ``subprocess.CompletedProcess``."""
    return CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


@patch("subprocess.run")
def test_lint_range_all_valid(mock_run) -> None:
    log_output = _make_git_log_output(
        "feat: add new feature",
        "fix(sdk): resolve issue",
    )
    mock_run.return_value = _completed(stdout=log_output)

    result = lint_range("origin/main..HEAD", skip_dirty_check=True)

    assert not result.has_errors
    assert len(result.commits) == 2
    assert all(c.error is None for c in result.commits)


@patch("subprocess.run")
def test_lint_range_with_invalid_commit(mock_run) -> None:
    log_output = _make_git_log_output(
        "feat: add new feature",
        "bad commit no colon",
    )
    mock_run.return_value = _completed(stdout=log_output)

    result = lint_range("origin/main..HEAD", skip_dirty_check=True)

    assert result.has_errors
    assert result.commits[0].error is None
    assert result.commits[1].error == "missing colon (:) char"


@patch("subprocess.run")
def test_lint_range_empty(mock_run) -> None:
    mock_run.return_value = _completed(stdout="")

    result = lint_range("origin/main..HEAD", skip_dirty_check=True)

    assert result.empty
    assert not result.has_errors


@patch("subprocess.run")
def test_lint_range_git_failure(mock_run) -> None:
    mock_run.return_value = _completed(returncode=1, stderr="fatal: bad range")

    result = lint_range("bad..range", skip_dirty_check=True)

    assert result.has_errors
    assert result.git_error == "fatal: bad range"


@patch("subprocess.run")
def test_lint_range_dirty_worktree_skips(mock_run) -> None:
    """When skip_dirty_check=False and worktree is dirty, validation is skipped."""
    mock_run.return_value = _completed(stdout=" M .github/scripts/lintcommit.py\n")

    result = lint_range("origin/main..HEAD", skip_dirty_check=False)

    assert result.skipped
    assert "uncommitted changes" in result.skip_reason
    # git log should never have been called (only git status)
    mock_run.assert_called_once()


@patch("subprocess.run")
def test_lint_range_warnings_collected(mock_run) -> None:
    log_output = _make_git_log_output(
        "feat: add thing\n\n" + "x" * 80,
    )
    mock_run.return_value = _completed(stdout=log_output)

    result = lint_range("origin/main..HEAD", skip_dirty_check=True)

    assert not result.has_errors
    assert len(result.commits) == 1
    assert any("exceeds 72 chars" in w for w in result.commits[0].warnings)
