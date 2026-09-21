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
import sys
import threading
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)
from opentelemetry.sdk.trace import Tracer as SdkTracer
from opentelemetry.sdk.trace import TracerProvider

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import DurableSampler
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


# -- concurrent first invocations sharing one cached tracer --------------------

# Trials per factory. A single trial reproduces the loss roughly one time in ten
# (measured at 16 threads with the switch interval below), so a handful of trials
# would pass with an unsynchronized install still in place.
_SHARED_TRACER_TRIALS = 200

# Concurrent create_plugin calls per trial, standing in for concurrent first
# invocations in one Lambda Managed Instances environment.
_SHARED_TRACER_PLUGINS = 16


def _build_plugins_concurrently(
    factory_type: type, provider: TracerProvider, count: int
) -> list[InvocationOtelPlugin | ExecutionOtelPlugin]:
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    factory = factory_type(config)
    ready = threading.Barrier(count)
    plugins: list[InvocationOtelPlugin | ExecutionOtelPlugin | None] = [None] * count

    def build(index: int) -> None:
        ready.wait(10.0)
        plugins[index] = factory.create_plugin(_invocation_start_info())

    workers = [threading.Thread(target=build, args=(index,)) for index in range(count)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(10.0)
    assert not any(worker.is_alive() for worker in workers)
    built = [plugin for plugin in plugins if plugin is not None]
    assert len(built) == count
    return built


@pytest.mark.parametrize(
    "factory_type",
    [InvocationOtelPluginFactory, ExecutionOtelPluginFactory],
)
def test_concurrent_plugins_share_the_wrappers_their_tracer_holds(
    factory_type: type,
) -> None:
    """Every plugin must hold the generator and sampler the tracer actually uses.

    A TracerProvider caches tracers by instrumentation scope, so plugins built for
    concurrent first invocations ask for one instrument name and get one tracer
    object. Installing the deterministic generator and the durable sampler on that
    tracer is a check-then-set: two plugins can each read the original generator,
    each wrap it, and the second assignment replaces the first. The plugin that
    assigned first then holds a wrapper the tracer no longer consults, so its
    deterministic ID overrides are ignored and its workflow and operation span IDs
    come out random, which breaks cross-invocation stitching.

    The switch interval is lowered so the interpreter preempts inside that
    check-then-set often enough for the loss to appear within the trial count; it
    does not create the window, it only makes an existing one likely to be hit.
    """
    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-9)
    try:
        for trial in range(_SHARED_TRACER_TRIALS):
            provider = TracerProvider()
            plugins = _build_plugins_concurrently(
                factory_type, provider, _SHARED_TRACER_PLUGINS
            )
            tracer = provider.get_tracer(OtelPluginConfig().instrument_name)
            assert isinstance(tracer, SdkTracer)
            # One tracer for every plugin: the premise the rest of the assertions
            # rest on, and the reason a lost install is not simply harmless.
            assert all(plugin._tracer is tracer for plugin in plugins)
            assert isinstance(tracer.id_generator, DeterministicIdGenerator)
            assert isinstance(tracer.sampler, DurableSampler)
            # Exactly one wrapper of each kind exists, and it is the tracer's.
            assert {id(plugin._id_generator) for plugin in plugins} == {
                id(tracer.id_generator)
            }, f"trial {trial}: a plugin holds an id generator the tracer discarded"
            assert {id(plugin._sampling_delegate) for plugin in plugins} == {
                id(tracer.sampler.delegate)
            }, f"trial {trial}: a plugin holds a sampling delegate the tracer discarded"
            # The wrapper wraps the provider's original, not another wrapper.
            assert not isinstance(tracer.sampler.delegate, DurableSampler)
            assert not isinstance(
                tracer.id_generator._fallback_id_generator, DeterministicIdGenerator
            )
    finally:
        sys.setswitchinterval(previous_interval)
