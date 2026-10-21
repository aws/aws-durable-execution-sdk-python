#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from current_latest_version import current_latest_version
from parse_testing_version import parse_testing_version

SCRIPTS = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize(
    "release_tag,expected",
    [
        ("testing-v2.0.0", "2.0.0"),
        ("testing-v1.2.1", "1.2.1"),
        ("sdk-v2.0.0,testing-v2.0.0", "2.0.0"),
        ("testing-v2.0.0,sdk-v2.1.0", "2.0.0"),
        ("otel-v1.0.0,testing-v1.2.1,sdk-v2.0.0", "1.2.1"),
        ("sdk-v2.0.0, testing-v1.2.1 ", "1.2.1"),
        ("sdk-v2.1.0", ""),
        ("otel-v1.0.0", ""),
        ("sdk-v2.0.0,otel-v1.0.0", ""),
        ("not-testing-v1.2.1", ""),
        ("sdk-v2.0.0,mytesting-v1.2.1", ""),
        ("", ""),
        ("v2.0.0", ""),
        ("random-text", ""),
    ],
)
def test_parse_testing_version(release_tag, expected):
    assert parse_testing_version(release_tag) == expected


@pytest.mark.parametrize(
    "version",
    [
        "2.0.0",
        "2.0.0rc1",
        "2.0.0-beta",
        "2.0.0.post1",
        "2.0.0.dev1",
        "2.0.0rc1.post2.dev3",
        "2.0.0_rc1",
        "2.0.0-1",
        "02.00.0-RC1",
        "1.2." + "3" * 116,  # Longest version usable by the per-architecture tags.
    ],
)
def test_release_and_latest_preserve_supported_versions(version):
    assert parse_testing_version(f"sdk-v1.0.0,testing-v{version}") == version
    assert current_latest_version(["latest", f"v{version}"]) == f"v{version}"


@pytest.mark.parametrize(
    "version",
    [
        "",
        "1",
        "1.2",
        "1.2.3.4",
        "1.2.3.4rc1",
        "1.2.3garbage",
        "1.2.3-x86_64",
        "1.2.3-arm64",
        "1.2.3+local",
        "1!1.2.3",
        "v1.2.3",
        "1.2.3/rc1",
        "1.2.3\nrc1",
        "ersion-1.2.1",
        "1.2." + "3" * 117,
    ],
)
def test_release_and_latest_reject_unsupported_versions(version):
    with pytest.raises(ValueError):
        parse_testing_version(f"testing-v{version}")
    assert current_latest_version(["latest", f"v{version}"]) == ""


@pytest.mark.parametrize(
    "release_tag",
    [
        "testing-v1.2.3,testing-v1.2.3",
        "testing-v1.2.3,sdk-v2.0.0,testing-v1.2.4",
        "testing-vbad, testing-v1.2.3",
    ],
)
def test_duplicate_testing_components_fail(release_tag):
    with pytest.raises(ValueError, match="more than once"):
        parse_testing_version(release_tag)


@pytest.mark.parametrize(
    "release_tag,expected",
    [("testing-v1.2.3.post1", "1.2.3.post1"), ("sdk-v2.0.0", "")],
)
def test_cli_appends_github_output(tmp_path, release_tag, expected):
    output = tmp_path / "github-output"
    output.write_text("existing=value\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "parse_testing_version.py")],
        env={**os.environ, "RELEASE_TAG": release_tag, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == f"{expected}\n"
    assert result.stderr == ""
    assert output.read_text(encoding="utf-8") == (
        f"existing=value\ntag_version={expected}\n"
    )


@pytest.mark.parametrize(
    "release_tag",
    ["testing-v1.2.3.4", "testing-v1.2.3,testing-v1.2.4"],
)
def test_cli_invalid_tag_fails_without_github_output(tmp_path, release_tag):
    output = tmp_path / "github-output"
    output.write_text("existing=value\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "parse_testing_version.py")],
        env={**os.environ, "RELEASE_TAG": release_tag, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Invalid testing release tag:" in result.stderr
    assert output.read_text(encoding="utf-8") == "existing=value\n"


def test_cli_without_github_output():
    env = {**os.environ, "RELEASE_TAG": "testing-v1.2.3-beta"}
    env.pop("GITHUB_OUTPUT", None)
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "parse_testing_version.py")],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == "1.2.3-beta\n"
    assert result.stderr == ""
