import tomllib
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]
CORE_DEPENDENCY = "aws-durable-execution-sdk-python>=1.9.0"


def _load_pyproject(path: Path) -> dict:
    with path.open("rb") as pyproject:
        return tomllib.load(pyproject)


def test_package_is_marked_production_stable() -> None:
    classifiers = _load_pyproject(PACKAGE_ROOT / "pyproject.toml")["project"][
        "classifiers"
    ]

    assert "Development Status :: 5 - Production/Stable" in classifiers
    assert "Development Status :: 4 - Beta" not in classifiers


def test_package_requires_compatible_core_sdk() -> None:
    dependencies = _load_pyproject(PACKAGE_ROOT / "pyproject.toml")["project"][
        "dependencies"
    ]

    assert CORE_DEPENDENCY in dependencies


def test_pypi_compatibility_environment_uses_compatible_core_sdk() -> None:
    dependencies = _load_pyproject(REPOSITORY_ROOT / "pyproject.toml")["tool"]["hatch"][
        "envs"
    ]["test-pypi-otel"]["dependencies"]

    assert CORE_DEPENDENCY in dependencies
