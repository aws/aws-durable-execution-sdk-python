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

from packaging.specifiers import SpecifierSet
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
        dependency.removeprefix(CORE_DISTRIBUTION + ">=").split(",", 1)[0]
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


def test_core_dependency_excludes_the_next_core_major() -> None:
    """A lower bound alone is the same defect one major later.

    The lower bound exists because this package's entry points resolve to plugin
    factories, which the core major below cannot call: pip accepts the resolution
    and the handler fails at initialization. Without a ceiling the next core major
    that changes the plugin contract reproduces exactly that, so the specifier has
    to reject it rather than only reject what came before.
    """
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        dependencies = tomllib.load(pyproject)["project"]["dependencies"]
    specifiers = [
        SpecifierSet(dependency.removeprefix(CORE_DISTRIBUTION))
        for dependency in dependencies
        if dependency.startswith(CORE_DISTRIBUTION)
    ]
    assert len(specifiers) == 1

    core_major = _major(_core_version())
    assert specifiers[0].contains(_core_version(), prereleases=True)
    assert not specifiers[0].contains(f"{core_major + 1}.0.0", prereleases=True)


def test_core_dependency_admits_a_later_core_patch() -> None:
    """The ceiling belongs on the major, not on the version built here.

    A ``<=`` ceiling looks equivalent and is not: it excludes the next core patch,
    so the first core patch release puts this claim out of date for a change that
    cannot have touched the plugin contract.
    """
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        dependencies = tomllib.load(pyproject)["project"]["dependencies"]
    specifier = next(
        SpecifierSet(dependency.removeprefix(CORE_DISTRIBUTION))
        for dependency in dependencies
        if dependency.startswith(CORE_DISTRIBUTION)
    )

    core = Version(_core_version())
    next_patch = f"{core.major}.{core.minor}.{core.micro + 1}"
    assert specifier.contains(next_patch, prereleases=True)
