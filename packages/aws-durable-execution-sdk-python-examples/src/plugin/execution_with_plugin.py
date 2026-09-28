"""Demonstrates handler execution without any durable operations."""

import logging
from typing import Any

from aws_durable_execution_sdk_python import StepContext
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)


class MyPlugin(DurableInstrumentationPlugin):
    logger = logging.getLogger("MyPlugin")

    def on_operation_start(self, info):
        self.logger.info(f"Operation started: {info}")

    def on_operation_end(self, info):
        self.logger.info(f"Operation ended: {info}")

    def on_invocation_start(self, info):
        self.logger.info(f"Invocation started: {info}")

    def on_invocation_end(self, info):
        self.logger.info(f"Invocation ended: {info}")

    def on_user_function_start(self, info) -> None:
        self.logger.info(f"User function started: {info}")

    def on_user_function_end(self, info) -> None:
        self.logger.info(f"User function ended: {info}")


class MyPluginFactory:
    """Builds one :class:`MyPlugin` for each invocation.

    ``durable_execution(plugins=[...])`` takes factory objects whose
    ``create_plugin`` the SDK calls once per invocation. A bare callable is
    rejected while the handler is being initialized, so registering one would
    stop this handler from importing. This factory exists only to construct the
    plugin.
    """

    def create_plugin(self, info: InvocationStartInfo) -> MyPlugin:
        """Return this invocation's plugin. ``info`` is unused."""
        return MyPlugin()


@durable_step
def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    return a + b


@durable_with_child_context
def add_numbers_in_child(child_context: DurableContext, a: int, b: int):
    result: int = child_context.step(
        add_numbers(a, b),
        name="add-a-and-b",
    )
    return result


@durable_execution(plugins=[MyPluginFactory()])
def handler(_event: Any, context: DurableContext) -> int:
    result: int = context.run_in_child_context(
        add_numbers_in_child(6, 4),
        name="add-6-and-4",
    )
    return context.step(
        add_numbers(result, 2),
        name="add-result-to-2",
    )
