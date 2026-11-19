"""Protect the Core compatibility boundary for empty-error persistence."""

from pathlib import Path
import tomllib

from packaging.requirements import Requirement
import pytest


@pytest.mark.parametrize("dependency_source", ["runtime", "test"])
def test_core_dependency_requires_empty_error_preservation(
    dependency_source: str,
) -> None:
    package_root = Path(__file__).resolve().parents[1]
    metadata = tomllib.loads((package_root / "pyproject.toml").read_text())
    dependencies = {
        "runtime": metadata["project"]["dependencies"],
        "test": metadata["tool"]["hatch"]["envs"]["test"]["dependencies"],
    }[dependency_source]
    requirements = {
        requirement.name: requirement for requirement in map(Requirement, dependencies)
    }
    core = requirements["aws-durable-execution-sdk-python"]

    # These are fixed compatibility fixtures, not the current release versions.
    # Core 3.0.1 loses empty errors on reload; Core 3.0.2 first preserves them.
    assert not core.specifier.contains("3.0.1")
    assert core.specifier.contains("3.0.2")
