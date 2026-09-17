import re
import tomllib
from pathlib import Path

from packaging.version import Version


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]
CORE_DISTRIBUTION = "aws-durable-execution-sdk-python"
CORE_DEPENDENCY = "aws-durable-execution-sdk-python>=3.0.0"
TEST_OTEL_DEPENDENCIES = {
    "opentelemetry-sdk>=1.20.0",
    "opentelemetry-propagator-aws-xray",
}
STANDALONE_OTEL_DEPENDENCIES = {
    "opentelemetry-api>=1.20.0",
    "opentelemetry-sdk>=1.20.0",
    "opentelemetry-propagator-aws-xray",
}


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


def test_package_relies_on_layer_for_opentelemetry_dependencies() -> None:
    dependencies = _load_pyproject(PACKAGE_ROOT / "pyproject.toml")["project"][
        "dependencies"
    ]

    assert not any(
        dependency.startswith("opentelemetry-") for dependency in dependencies
    )


def test_standalone_extra_provides_opentelemetry_dependencies() -> None:
    standalone_dependencies = _load_pyproject(PACKAGE_ROOT / "pyproject.toml")[
        "project"
    ]["optional-dependencies"]["standalone"]

    assert set(standalone_dependencies) == STANDALONE_OTEL_DEPENDENCIES


def test_test_environments_install_layer_provided_dependencies() -> None:
    environments = _load_pyproject(REPOSITORY_ROOT / "pyproject.toml")["tool"]["hatch"][
        "envs"
    ]

    for environment_name in (
        "test",
        "dev-otel",
        "dev-examples",
        "test-pypi-otel",
        "test-pypi-examples",
    ):
        assert TEST_OTEL_DEPENDENCIES <= set(
            environments[environment_name]["dependencies"]
        )
    assert TEST_OTEL_DEPENDENCIES <= set(environments["types"]["extra-dependencies"])


def test_pypi_compatibility_environment_uses_compatible_core_sdk() -> None:
    dependencies = _load_pyproject(REPOSITORY_ROOT / "pyproject.toml")["tool"]["hatch"][
        "envs"
    ]["test-pypi-otel"]["dependencies"]

    assert CORE_DEPENDENCY in dependencies


def _core_version() -> str:
    """The core SDK version this repository builds, read from its source.

    Read from the file rather than imported. The ``test-pypi-otel`` environment
    installs a *published* core alongside this package's source, so an import
    would report that release's version and the checks below would stop saying
    anything about this repository's version story.
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
    dependencies = _load_pyproject(PACKAGE_ROOT / "pyproject.toml")["project"][
        "dependencies"
    ]
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
    """The declared bound must not admit a core major that predates this plugin's contract.

    This package's entry points resolve to plugin factories, and the ``plugins``
    argument only accepts factories from the core major that introduced them. A
    lower bound naming an earlier major is a resolution pip accepts and that then
    fails at handler initialization, so the bound has to track the core major this
    repository builds. The bound may lag within that major -- a later core minor
    still satisfies the contract -- which is why only the major is compared and
    the bound is required not to exceed the core version.
    """
    core_version = _core_version()
    lower_bound = _core_dependency_lower_bound()

    assert _major(lower_bound) == _major(core_version)
    assert Version(lower_bound) <= Version(core_version)


def test_layer_sdk_pin_matches_the_core_version_in_this_repository() -> None:
    """The OTel layer pin selects the core wheel bundled into the published layer.

    A combined SDK and OTel release fails outright when the pin disagrees with the
    released SDK version, and an OTel-only release downloads exactly the pinned
    version from PyPI. A stale pin therefore either blocks the release or ships a
    layer whose core cannot run this plugin, so the pin tracks the core version
    this repository builds.
    """
    metadata_path = REPOSITORY_ROOT / ".github" / "lambda-layer-publish.toml"

    with metadata_path.open("rb") as metadata_file:
        pinned_version = tomllib.load(metadata_file)["layer"]["sdk-version"]

    assert pinned_version == _core_version()
