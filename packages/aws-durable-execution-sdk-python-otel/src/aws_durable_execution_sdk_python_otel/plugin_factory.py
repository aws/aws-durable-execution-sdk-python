"""Plugin factories for the bundled durable-execution OTel plugins.

The SDK's plugin contract is a factory called once per invocation:
``DurableInstrumentationPluginFactory = Callable[[InvocationStartInfo],
DurableInstrumentationPlugin]``. The instance a factory returns serves exactly
that one invocation and is dropped when the invocation scope exits, so a plugin
keeps its per-invocation state in ordinary instance attributes.

Both factories are callable classes rather than closures so the configuration
they were built with stays inspectable (``factory.config``) and so the entry
points below name an object with a readable type.

Everything else the plugins need is resolved per invocation inside the plugin
itself: the tracer provider (which for the global-provider case may only be
installed after the handler module is imported), the tracer, and the
deterministic id generator and sampler installed on it. Those installs are
idempotent and scoped to the plugin's own tracer, so building a plugin per
invocation neither stacks wrappers nor disturbs other instrumentation scopes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_otel.execution_plugin import (
    ExecutionOtelPlugin,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import (
    InvocationOtelPlugin,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.plugin import InvocationStartInfo

    from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
        OtelPluginConfig,
    )


class InvocationOtelPluginFactory:
    """Builds one :class:`InvocationOtelPlugin` per invocation.

    Register the factory itself, not a plugin, with
    ``@durable_execution(plugins=[...])``::

        @durable_execution(plugins=[InvocationOtelPluginFactory()])
        def handler(event, context): ...

    Args:
        config: Shared plugin configuration handed to every plugin this factory
            builds. When omitted, each plugin uses defaults (globally configured
            tracer provider, X-Ray extractor, "Workflow" span name, log
            enrichment on).
    """

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self.config = config

    def __call__(self, info: InvocationStartInfo) -> InvocationOtelPlugin:
        """Return this invocation's plugin.

        ``info`` is accepted because the SDK passes it, and is unused: the
        plugin reads the same object again in ``on_invocation_start``, which is
        where all of its invocation identity is derived.
        """
        return InvocationOtelPlugin(self.config)


class ExecutionOtelPluginFactory:
    """Builds one :class:`ExecutionOtelPlugin` per invocation.

    Args:
        config: Shared plugin configuration handed to every plugin this factory
            builds. When omitted, each plugin uses defaults.
    """

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self.config = config

    def __call__(self, info: InvocationStartInfo) -> ExecutionOtelPlugin:
        """Return this invocation's plugin. ``info`` is unused; see above."""
        return ExecutionOtelPlugin(self.config)


INVOCATION_OTEL_PLUGIN_FACTORY = InvocationOtelPluginFactory()
"""Default-configured factory named by the ``otel-invocation`` entry point."""

EXECUTION_OTEL_PLUGIN_FACTORY = ExecutionOtelPluginFactory()
"""Default-configured factory named by the ``otel-execution`` entry point."""
