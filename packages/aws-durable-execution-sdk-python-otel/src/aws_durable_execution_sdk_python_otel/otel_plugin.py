"""Public entry point for selecting a durable-execution OTel view."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from aws_durable_execution_sdk_python_otel.plugin_factory import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
        OtelPluginConfig,
    )


class OtelPlugin:
    """Select the OTel factory to register in the handler's ``plugins`` list."""

    @staticmethod
    @overload
    def factory(
        config: OtelPluginConfig | None = None,
        *,
        mode: Literal["execution"] = "execution",
    ) -> ExecutionOtelPluginFactory: ...

    @staticmethod
    @overload
    def factory(
        config: OtelPluginConfig | None = None,
        *,
        mode: Literal["invocation"],
    ) -> InvocationOtelPluginFactory: ...

    @staticmethod
    def factory(
        config: OtelPluginConfig | None = None,
        *,
        mode: Literal["execution", "invocation"] = "execution",
    ) -> ExecutionOtelPluginFactory | InvocationOtelPluginFactory:
        """Return the selected factory, defaulting to the execution view.

        The SDK calls this factory for each invocation, creating an isolated
        plugin while preserving the selected view's registration metadata.
        Configuration is passed unchanged to the existing factory. Omitted
        configuration uses the globally configured OpenTelemetry provider.

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
