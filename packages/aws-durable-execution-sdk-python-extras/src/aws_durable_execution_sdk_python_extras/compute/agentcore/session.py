# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The types of ``agentcore_session``: run several jobs in one AgentCore
Runtime session."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
    InvocationError,
)

from aws_durable_execution_sdk_python_extras.compute.agentcore.operation import (
    _create_session_id,
    _stop_session,
    _validate_name,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.retry import (
    default_agentcore_retry_strategy,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext

    from aws_durable_execution_sdk_python_extras.compute.agentcore.config import (
        AgentcoreInvokeOptions,
        AgentcoreSessionConfig,
    )


_T = TypeVar("_T")


class AgentcoreSession(Protocol):
    """The handle that an :func:`agentcore_session` handler receives.

    Each :meth:`invoke` creates its durable operations in the context that
    the caller passes. Parallel :meth:`invoke` calls on one session run in
    the same microVM, so the job handlers must allow it.
    """

    @property
    def runtime_session_id(self) -> str:
        """The ``runtimeSessionId`` that every job of the session uses."""
        ...

    def invoke(
        self,
        context: DurableContext,
        name: str,
        *,
        job_input: Any,
        options: AgentcoreInvokeOptions,
    ) -> Any:
        """Send one job to the session, and return the job's result.

        Args:
            context: The context to create the job's durable operations in:
                the session's child context, or a context nested in it.
            name: The job's child context name.
            job_input: The job's input. It must be JSON-serializable, and the
                same on every replay.
            options: The timeouts of this job.

        Returns:
            The value that the agent passed to
            ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

        Raises:
            AgentcoreJobFailedError: When the agent reports a failure.
            AgentcoreTimeoutError: When the job or its heartbeat times out.
            AgentcoreDeliveryError: When the request fails after all
                retries, or the agent does not accept the job.
        """
        ...


def _validate_session_config(name: str, config: AgentcoreSessionConfig) -> None:
    """Validate the session config, before any durable operation starts.

    It uses the rules of :func:`agentcore`. ``agent_runtime_arn`` is a
    required field, and its value is not checked here. The boto3 client and
    the service reject a value that is not valid.

    Raises:
        ValidationError: When ``name`` is empty.
    """
    _validate_name(name)


@dataclass(frozen=True)
class _SessionHandle:
    """The handle that an :func:`agentcore_session` handler receives, until
    the session can send jobs.

    It has the ``runtime_session_id`` of :class:`AgentcoreSession`, and no
    ``invoke`` yet.
    """

    runtime_session_id: str


def agentcore_session(
    context: DurableContext,
    name: str,
    *,
    config: AgentcoreSessionConfig,
    handler: Callable[[_SessionHandle, DurableContext], _T],
) -> _T:
    """Run a handler in one AgentCore Runtime session, and return the
    handler's value.

    The operation runs its durable operations in one child context named
    ``name``:

    1. ``<name>.session`` generates the runtime session ID, and checkpoints
       it. So every retry and replay uses the same session.
    2. The handler runs with the session handle and the child context.
    3. ``<name>.stop`` calls ``StopRuntimeSession`` after the handler
       returns or raises an error that ends the session.

    A stop failure does not fail the operation. AgentCore ends an idle
    session at the agent runtime's idle timeout. So the operation logs an
    error and returns the handler's outcome.

    Some errors do not end the session, and the session is not stopped for
    them:

    1. ``SuspendExecution``: the handler waits, and a later invocation
       replays it.
    2. ``ExecutionError``: the execution fails without a retry, and the
       core SDK records no outcome for the child context.
    3. A retryable ``InvocationError``: Lambda runs the invocation again,
       and the handler replays in the same session.

    A stop for one of them would end a session that the replay still uses,
    or add an operation that the replay does not expect.

    Args:
        context: The context to create the durable operations in.
        name: The child context name. It also prefixes the session's own
            operation names.
        config: The agent runtime.
        handler: Runs in the session. It receives the session handle and the
            session's child context.

    Returns:
        The handler's return value.

    Raises:
        ValidationError: When ``name`` or ``config`` is not valid. No durable
            operation starts.
        ChildContextError: Any error that the handler raised.
    """
    _validate_session_config(name, config)
    retry_strategy = config.retry_strategy or default_agentcore_retry_strategy

    def run(child: DurableContext) -> _T:
        session_id = _create_session_id(child, name)

        def stop() -> None:
            _stop_session(child, name, config, retry_strategy, session_id)

        try:
            result = handler(_SessionHandle(session_id), child)
        except ExecutionError:
            raise
        except InvocationError as error:
            if not error.is_retryable():
                stop()
            raise
        except Exception:
            # SuspendExecution is a BaseException, so a wait in the handler
            # passes through here without stopping the session.
            stop()
            raise
        stop()
        return result

    return context.run_in_child_context(run, name=name)
