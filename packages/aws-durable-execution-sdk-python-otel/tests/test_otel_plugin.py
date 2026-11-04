"""Public factory selection, typing, and installed entry-point coverage."""

from importlib.metadata import entry_points
from typing import Literal, assert_type, cast
from unittest.mock import patch

import pytest
from aws_durable_execution_sdk_python_otel import (
    EXECUTION_OTEL_PLUGIN_FACTORY,
    INVOCATION_OTEL_PLUGIN_FACTORY,
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    create_otel_plugin_factory,
    OtelPluginConfig,
)
from opentelemetry.sdk.trace import TracerProvider


def test_default_factory_type_is_execution() -> None:
    factory = assert_type(create_otel_plugin_factory(), ExecutionOtelPluginFactory)
    assert type(factory) is ExecutionOtelPluginFactory
    assert factory.config is None


def test_explicit_mode_return_types() -> None:
    execution = assert_type(
        create_otel_plugin_factory(mode="execution"), ExecutionOtelPluginFactory
    )
    invocation = assert_type(
        create_otel_plugin_factory(mode="invocation"), InvocationOtelPluginFactory
    )
    assert type(execution) is ExecutionOtelPluginFactory
    assert type(invocation) is InvocationOtelPluginFactory


@pytest.mark.parametrize("mode", ["execution", "invocation"])
@pytest.mark.parametrize("custom_provider", [False, True])
def test_factory_preserves_configuration_and_registration(
    mode: Literal["execution", "invocation"], custom_provider: bool
) -> None:
    provider = TracerProvider() if custom_provider else None
    config = OtelPluginConfig(
        tracer_provider=provider,
        context_extractor=lambda _: None,
        instrument_name="custom-scope",
        workflow_span_name="CustomWorkflow",
        enrich_logger=False,
    )
    try:
        with patch(
            "opentelemetry.trace.get_tracer_provider",
            side_effect=AssertionError("factory selection must not resolve a provider"),
        ):
            factory = create_otel_plugin_factory(config, mode=mode)
        expected = (
            ExecutionOtelPluginFactory
            if mode == "execution"
            else InvocationOtelPluginFactory
        )
        assert type(factory) is expected
        assert factory.config is config
        assert factory.__durable_registration_api__ == 1
        assert factory.exclusive_group == expected.exclusive_group
    finally:
        if provider is not None:
            provider.shutdown()


@pytest.mark.parametrize("mode", [None, "", "EXECUTION", "unknown", 0, False])
def test_invalid_modes_are_rejected(mode: object) -> None:
    with pytest.raises(ValueError, match="mode must be 'execution' or 'invocation'"):
        create_otel_plugin_factory(mode=cast(Literal["execution", "invocation"], mode))


def test_installed_entry_points_preserve_alias_identity() -> None:
    installed = {
        entry.name: entry.load()
        for entry in entry_points(group="aws_durable_execution.plugins")
        if entry.name in {"otel", "otel-execution", "otel-invocation"}
    }
    assert installed == {
        "otel": EXECUTION_OTEL_PLUGIN_FACTORY,
        "otel-execution": EXECUTION_OTEL_PLUGIN_FACTORY,
        "otel-invocation": INVOCATION_OTEL_PLUGIN_FACTORY,
    }
