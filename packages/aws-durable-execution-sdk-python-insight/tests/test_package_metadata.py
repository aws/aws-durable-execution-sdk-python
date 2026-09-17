# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaging checks for the Workflow Insight plugin.

The plugin and the core SDK are separate distributions, so pip resolves their
versions independently. A declared bound that admits a core release without the
contract this plugin uses is an install that succeeds and then fails at handler
initialization, which is why the bound is asserted here rather than left to
review.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from packaging.version import Version


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]
CORE_DISTRIBUTION = "aws-durable-execution-sdk-python"


def _core_version() -> str:
    """The core SDK version this repository builds, read from its source.

    Read from the file rather than imported, so the check keeps describing this
    repository even when a published core is installed alongside these sources.
    """
    about = (
        REPOSITORY_ROOT
        / "packages"
        / "aws-durable-execution-sdk-python"
        / "src"
        / "aws_durable_execution_sdk_python"
        / "__about__.py"
    ).read_text()
    match = re.search(r'^__version__ = "([^"]+)"', about, re.MULTILINE)
    assert match is not None, "core __about__.py has no __version__ assignment"
    return match.group(1)


def _core_dependency_lower_bound() -> str:
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        dependencies = tomllib.load(pyproject)["project"]["dependencies"]

    bounds = [
        dependency.removeprefix(CORE_DISTRIBUTION + ">=")
        for dependency in dependencies
        if dependency.startswith(CORE_DISTRIBUTION + ">=")
    ]
    assert len(bounds) == 1, f"expected one {CORE_DISTRIBUTION} bound, got {bounds}"
    return bounds[0]


def _major(version: str) -> int:
    return int(version.split(".", 1)[0])


def test_core_dependency_bound_matches_the_core_major_in_this_repository() -> None:
    """The declared bound must not admit a core major that predates the factory contract.

    ``workflow_insight()`` returns a plugin factory, and only the core major that
    introduced factories calls it. A lower bound naming an earlier major is a
    resolution pip accepts and that then fails at handler initialization, so the
    bound tracks the core major this repository builds. The bound may lag within
    that major -- a later core minor still satisfies the contract -- which is why
    only the major is compared and the bound is required not to exceed the core
    version.
    """
    core_version = _core_version()
    lower_bound = _core_dependency_lower_bound()

    assert _major(lower_bound) == _major(core_version)
    assert Version(lower_bound) <= Version(core_version)
