import tomllib
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_package_is_marked_production_stable() -> None:
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        classifiers = tomllib.load(pyproject)["project"]["classifiers"]

    assert "Development Status :: 5 - Production/Stable" in classifiers
    assert "Development Status :: 4 - Beta" not in classifiers
