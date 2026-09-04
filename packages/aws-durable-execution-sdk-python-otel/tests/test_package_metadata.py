import tomllib
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]
CORE_DEPENDENCY = "aws-durable-execution-sdk-python>=2.0.0"
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
