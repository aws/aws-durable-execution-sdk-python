from aws_durable_execution_sdk_python.plugin import (
    DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
)

from aws_durable_execution_sdk_python_otel.execution_plugin import (
    ExecutionOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import (
    InvocationOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.plugin_provider import (
    EXECUTION_OTEL_PLUGIN_PROVIDER,
    INVOCATION_OTEL_PLUGIN_PROVIDER,
)


def test_invocation_otel_plugin_provider_uses_current_plugin_api() -> None:
    assert INVOCATION_OTEL_PLUGIN_PROVIDER.plugin_type is InvocationOtelPlugin
    assert (
        INVOCATION_OTEL_PLUGIN_PROVIDER.plugin_api_version
        == DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION
    )


def test_invocation_otel_plugin_provider_creates_invocation_plugin() -> None:
    assert isinstance(INVOCATION_OTEL_PLUGIN_PROVIDER.factory(), InvocationOtelPlugin)


def test_execution_otel_plugin_provider_uses_current_plugin_api() -> None:
    assert EXECUTION_OTEL_PLUGIN_PROVIDER.plugin_type is ExecutionOtelPlugin
    assert (
        EXECUTION_OTEL_PLUGIN_PROVIDER.plugin_api_version
        == DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION
    )


def test_execution_otel_plugin_provider_creates_execution_plugin() -> None:
    assert isinstance(EXECUTION_OTEL_PLUGIN_PROVIDER.factory(), ExecutionOtelPlugin)
