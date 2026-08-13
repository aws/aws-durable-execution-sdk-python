from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPluginProvider,
)

from aws_durable_execution_sdk_python_otel.execution_plugin import (
    ExecutionOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import (
    InvocationOtelPlugin,
)


INVOCATION_OTEL_PLUGIN_PROVIDER = DurableInstrumentationPluginProvider(
    plugin_type=InvocationOtelPlugin,
    factory=InvocationOtelPlugin,
    plugin_api_version=2,
)

EXECUTION_OTEL_PLUGIN_PROVIDER = DurableInstrumentationPluginProvider(
    plugin_type=ExecutionOtelPlugin,
    factory=ExecutionOtelPlugin,
    plugin_api_version=2,
)
