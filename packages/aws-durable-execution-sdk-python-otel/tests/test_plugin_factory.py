"""Tests for the bundled OTel plugin factories and the entry points naming them.

The SDK plugin contract is a factory object whose ``create_plugin`` is called once
per invocation, so what these tests have to establish is that the objects the
package exposes -- and the ones its entry points name -- carry ``create_plugin``,
that it builds a plugin, and that each call builds a NEW plugin. The old
provider-shaped assertions (a declared ``plugin_type`` and an API version) have no
counterpart: the contract carries neither.
"""

from __future__ import annotations

import importlib
import logging
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)

from aws_durable_execution_sdk_python_otel.execution_plugin import (
    ExecutionOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import (
    InvocationOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.log_filter import OtelContextLogFilter
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from aws_durable_execution_sdk_python_otel.plugin_factory import (
    EXECUTION_OTEL_PLUGIN_FACTORY,
    INVOCATION_OTEL_PLUGIN_FACTORY,
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_ENTRY_POINT_GROUP = "aws_durable_execution.plugins"
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _remove_installed_log_filters():
    """Detach any log filter a default-configured plugin installed.

    ``OtelPluginConfig.enrich_logger`` defaults to True, so building a plugin
    from a default-configured factory attaches a filter to the root logger's
    handlers, which outlive the test.
    """
    yield
    for handler in logging.getLogger().handlers:
        for installed in [
            f for f in handler.filters if isinstance(f, OtelContextLogFilter)
        ]:
            handler.removeFilter(installed)


def _invocation_start_info() -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
        is_first_invocation=True,
    )


def _declared_entry_points() -> dict[str, str]:
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        project = tomllib.load(pyproject)["project"]
    return project["entry-points"][PLUGIN_ENTRY_POINT_GROUP]


def _resolve(spec: str) -> object:
    """Resolve a ``module:attribute`` entry-point value the way the SDK does.

    Resolved from the declared value rather than from installed distribution
    metadata so the assertion holds wherever the tests run, installed or not.
    """
    module_name, _, attribute = spec.partition(":")
    return getattr(importlib.import_module(module_name), attribute)


@pytest.mark.parametrize(
    ("factory_type", "plugin_type"),
    [
        (InvocationOtelPluginFactory, InvocationOtelPlugin),
        (ExecutionOtelPluginFactory, ExecutionOtelPlugin),
    ],
)
def test_factory_builds_its_plugin_type(
    factory_type: type, plugin_type: type[DurableInstrumentationPlugin]
) -> None:
    factory = factory_type(OtelPluginConfig(enrich_logger=False))

    assert isinstance(factory.create_plugin(_invocation_start_info()), plugin_type)


@pytest.mark.parametrize(
    "factory_type",
    [InvocationOtelPluginFactory, ExecutionOtelPluginFactory],
)
def test_factory_builds_a_fresh_plugin_per_invocation(factory_type: type) -> None:
    """Two calls must never hand back the same instance.

    This is the whole point of the factory contract: the returned plugin holds
    one invocation's state in ordinary instance fields, so a shared instance
    would leak span registries and context tokens from one invocation into the
    next.
    """
    factory = factory_type(OtelPluginConfig(enrich_logger=False))

    first = factory.create_plugin(_invocation_start_info())
    second = factory.create_plugin(_invocation_start_info())

    assert first is not second


@pytest.mark.parametrize(
    "factory_type",
    [InvocationOtelPluginFactory, ExecutionOtelPluginFactory],
)
def test_factory_passes_its_config_to_every_plugin(factory_type: type) -> None:
    config = OtelPluginConfig(workflow_span_name="Custom", enrich_logger=False)
    factory = factory_type(config)

    assert factory.config is config
    assert factory.create_plugin(_invocation_start_info())._config is config
    assert factory.create_plugin(_invocation_start_info())._config is config


@pytest.mark.parametrize(
    "factory_type",
    [InvocationOtelPluginFactory, ExecutionOtelPluginFactory],
)
def test_factory_without_config_builds_a_default_configured_plugin(
    factory_type: type,
) -> None:
    factory = factory_type()

    assert factory.config is None
    assert factory.create_plugin(_invocation_start_info())._config == OtelPluginConfig()


def test_module_level_factories_are_default_configured() -> None:
    assert INVOCATION_OTEL_PLUGIN_FACTORY.config is None
    assert EXECUTION_OTEL_PLUGIN_FACTORY.config is None


def test_declared_entry_points_name_the_bundled_factories() -> None:
    entry_points = _declared_entry_points()

    assert set(entry_points) == {"otel-invocation", "otel-execution"}
    assert _resolve(entry_points["otel-invocation"]) is INVOCATION_OTEL_PLUGIN_FACTORY
    assert _resolve(entry_points["otel-execution"]) is EXECUTION_OTEL_PLUGIN_FACTORY


def test_declared_entry_points_resolve_to_factories_that_build_plugins() -> None:
    """The entry points must satisfy the SDK's factory contract.

    ``plugin_discovery._load_factory`` requires an object with a callable
    ``create_plugin``, so a target resolving to a plugin class, to a plugin
    instance, or to a plain function now fails at handler initialization. The
    check here mirrors the SDK's own, then calls the method to confirm what it
    builds.
    """
    expected = {
        "otel-invocation": InvocationOtelPlugin,
        "otel-execution": ExecutionOtelPlugin,
    }

    for name, spec in _declared_entry_points().items():
        factory = _resolve(spec)
        create_plugin = getattr(factory, "create_plugin", None)
        assert callable(create_plugin)
        assert not isinstance(factory, type)
        assert isinstance(create_plugin(_invocation_start_info()), expected[name])
