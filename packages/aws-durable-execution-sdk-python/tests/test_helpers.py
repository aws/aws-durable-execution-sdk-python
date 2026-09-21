"""Test helpers for generating expected step IDs."""

import contextlib
from collections.abc import Iterator
from unittest.mock import Mock

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.execution import ExecutionState
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
    PluginExecutor,
)


def operation_id_sequence(parent_id: str | None = None):
    """Generator that yields step IDs in sequence using DurableContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test-arn"

    execution_context = ExecutionContext(durable_execution_arn="test-arn")
    context = DurableContext(
        state=mock_state, execution_context=execution_context, parent_id=parent_id
    )

    while True:
        yield context._create_step_id()  # noqa: SLF001


class _FixedPluginFactory:
    """Returns one plugin instance the test already holds, for every invocation.

    Production factories build a fresh instance per invocation. A test that has
    to read what the plugin recorded needs the instance it passed in, so it
    supplies a factory that returns that one. Only valid for a single
    invocation, which is all these tests run.
    """

    def __init__(self, plugin: DurableInstrumentationPlugin) -> None:
        self._plugin = plugin

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        return self._plugin


def plugin_factory(
    plugin: DurableInstrumentationPlugin,
) -> DurableInstrumentationPluginFactory:
    """Wrap a plugin instance a test already holds a reference to as a factory."""
    return _FixedPluginFactory(plugin)


@contextlib.contextmanager
def plugin_invocation(
    plugin_executor: PluginExecutor,
    *,
    execution_arn: str = "test_arn",
    is_first_invocation: bool = True,
) -> Iterator[None]:
    """Open a plugin executor's per-invocation scope.

    Plugin instances are built by ``on_invocation_start`` and dropped when the
    ``run()`` scope exits, so a hook dispatched outside an invocation reaches no
    plugin at all. Tests that exercise a single hook still have to establish the
    invocation the hook belongs to; this does that and nothing else.

    The invocation-start hook this fires is scaffolding. Tests asserting on an
    exact list of recorded calls should record only the hooks they care about, or
    clear their recorder after entering the scope.
    """
    with plugin_executor.run():
        plugin_executor.on_invocation_start(
            execution_arn=execution_arn,
            is_first_invocation=is_first_invocation,
            execution_start_time=None,
            lambda_context=None,
        )
        yield
