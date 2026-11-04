"""Plugin factories for the bundled durable-execution OTel plugins.

The SDK's plugin contract is a factory object whose ``create_plugin`` is called
once per invocation: ``DurableInstrumentationPluginFactory`` declares
``create_plugin(info: InvocationStartInfo) -> DurableInstrumentationPlugin``. The
instance a factory returns serves exactly that one invocation and is dropped when
the invocation scope exits, so a plugin keeps its per-invocation state in ordinary
instance attributes.

Both factories are classes rather than closures so the configuration they were
built with stays inspectable (``factory.config``) and so the entry points below
name an object with a readable type.

Everything else the plugins need is resolved per invocation inside the plugin
itself: the tracer provider (which for the global-provider case may only be
installed after the handler module is imported), the tracer, and the
deterministic id generator and sampler installed on it. Those installs are
scoped to the plugin's own tracer, so building a plugin per invocation does not
disturb other instrumentation scopes. They are also atomic and return whatever
the tracer holds, which matters because a provider caches tracers by
instrumentation scope: two invocations starting at once get one tracer, and a
plugin that lost the install race must use the wrapper on that tracer rather than
one of its own that the tracer would never consult.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal, overload

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

    __durable_registration_api__: ClassVar[int] = 1
    exclusive_group: ClassVar[str | None] = "aws-durable-execution-otel-view"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self.config = config

    def create_plugin(self, info: InvocationStartInfo) -> InvocationOtelPlugin:
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

    __durable_registration_api__: ClassVar[int] = 1
    exclusive_group: ClassVar[str | None] = "aws-durable-execution-otel-view"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self.config = config

    def create_plugin(self, info: InvocationStartInfo) -> ExecutionOtelPlugin:
        """Return this invocation's plugin. ``info`` is unused; see above."""
        return ExecutionOtelPlugin(self.config)


@overload
def create_otel_plugin_factory(
    config: OtelPluginConfig | None = None,
    *,
    mode: Literal["execution"] = "execution",
) -> ExecutionOtelPluginFactory: ...


@overload
def create_otel_plugin_factory(
    config: OtelPluginConfig | None = None,
    *,
    mode: Literal["invocation"],
) -> InvocationOtelPluginFactory: ...


# A mode read from configuration at runtime is a plain ``str``, which neither
# literal overload accepts. This last overload keeps that call type-checkable;
# the union it returns is what a caller can know without a literal, and an
# unsupported value still raises ``ValueError`` below.
@overload
def create_otel_plugin_factory(
    config: OtelPluginConfig | None = None,
    *,
    mode: str,
) -> ExecutionOtelPluginFactory | InvocationOtelPluginFactory: ...


def create_otel_plugin_factory(
    config: OtelPluginConfig | None = None,
    *,
    mode: str = "execution",
) -> ExecutionOtelPluginFactory | InvocationOtelPluginFactory:
    """Return a new factory for the selected view, defaulting to execution.

    Each call constructs a factory of the selected view's class, carrying that
    view's ``exclusive_group`` and registration metadata. Calls are not
    memoized, so register exactly one: two factories of the same view in
    ``plugins`` are rejected at registration as a duplicate in the
    ``durable-otel-view`` exclusive group. Configuration is passed unchanged to
    that class. Omitted configuration uses the globally configured OpenTelemetry
    provider, resolved per invocation rather than here.

    Args:
        config: Existing OTel plugin configuration.
        mode: ``"execution"`` for Workflow-parented operations, or
            ``"invocation"`` for Invocation-parented operations.

    Raises:
        ValueError: If mode is neither ``"execution"`` nor ``"invocation"``.
    """
    if mode == "execution":
        return ExecutionOtelPluginFactory(config)
    if mode == "invocation":
        return InvocationOtelPluginFactory(config)
    raise ValueError("mode must be 'execution' or 'invocation'")


INVOCATION_OTEL_PLUGIN_FACTORY = InvocationOtelPluginFactory()
"""Default-configured factory named by the ``otel-invocation`` entry point."""

EXECUTION_OTEL_PLUGIN_FACTORY = ExecutionOtelPluginFactory()
"""Default-configured factory named by the ``otel`` and ``otel-execution`` entry points.

Both names resolve to this one object, so ``DURABLE_EXECUTION_PLUGINS=otel,otel-execution``
loads a single factory: the loader skips the second by identity and logs a warning.
``otel`` with ``otel-invocation`` names two different views and is rejected.
"""
