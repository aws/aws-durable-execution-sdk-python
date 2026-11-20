# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``agentcore_session`` operation: run several jobs in one AgentCore
Runtime session."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    ExecutionError,
    InvocationError,
)

from aws_durable_execution_sdk_python_extensions.compute.agentcore.config import (
    AgentcoreConfig,
)
from aws_durable_execution_sdk_python_extensions.compute.agentcore.errors import (
    AgentcoreError,
)
from aws_durable_execution_sdk_python_extensions.compute.agentcore.operation import (
    _AgentcoreScope,
    _create_session_id,
    _run_job,
    _stop_session,
    _validate_base,
)
from aws_durable_execution_sdk_python_extensions.compute.agentcore.retry import (
    default_agentcore_retry_strategy,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext

    from aws_durable_execution_sdk_python_extensions.compute.agentcore.config import (
        AgentcoreInvokeOptions,
        AgentcoreSessionConfig,
    )


_T = TypeVar("_T")


class AgentcoreSession:
    """The handle that an :func:`agentcore_session` handler receives.

    The handle holds no context of its own. Each :meth:`invoke` creates its
    durable operations in the context that the caller passes. Parallel
    :meth:`invoke` calls on one session run in the same microVM, so the job
    handlers must allow it.
    """

    def __init__(
        self,
        config: AgentcoreSessionConfig,
        runtime_session_id: str,
        *,
        name: str,
        outer_context: DurableContext,
    ) -> None:
        self._config = config
        self._runtime_session_id = runtime_session_id
        self._name = name
        # The context that agentcore_session was called with. A job created
        # in it would be outside the session's child context.
        self._outer_context = outer_context

    @property
    def runtime_session_id(self) -> str:
        """The ``runtimeSessionId`` that every job of the session uses."""
        return self._runtime_session_id

    def invoke(
        self,
        context: DurableContext,
        name: str,
        *,
        job_input: Any,
        options: AgentcoreInvokeOptions,
    ) -> Any:
        """Send one job to the session, and return the job's result.

        Each call creates a child context named ``name`` in ``context``, with
        a callback ``<name>.callback`` and a request step ``<name>.request``.
        Pass the context that the call runs in: the session's child context,
        or a nested one, such as a ``map`` item's context. A failed job does
        not stop the session. The session stops when its handler returns or
        raises.

        Args:
            context: The context to create the job's durable operations in.
            name: The job's child context name.
            job_input: The job's input. It must be JSON-serializable, and the
                same on every replay.
            options: The timeouts of this job.

        Returns:
            The value that the agent passed to
            ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

        Raises:
            TypeError: When ``context`` is the context that
                ``agentcore_session`` was called with, or ``job_input`` is not
                JSON-serializable. No durable operation starts.
            ValidationError: When ``name`` or ``options`` is not valid, or the
                Region of the durable function cannot be determined. No
                durable operation starts.
            AttributeError: When ``context`` is not a durable context, for
                example a job name passed as the first argument. Python
                raises it at ``context.run_in_child_context``.
            ValueError: When the JSON of ``job_input`` is over
                ``MAX_JOB_INPUT_BYTES`` UTF-8 bytes. No durable operation
                starts.
            AgentcoreJobFailedError: When the agent reports a failure, or
                sends a result that is not valid JSON.
            AgentcoreTimeoutError: When the job or its heartbeat times out.
            AgentcoreDeliveryError: When the request fails after all
                retries, or the agent does not accept the job.
        """
        # The caller's own context is often in scope in the handler's
        # closure. A job created in it lands outside the session's child
        # context. The core SDK does not refuse that, and the next replay
        # fails with NonDeterministicExecutionError after the job has run.
        # So the most likely wrong context is refused here. A full ancestry
        # check would need support in the core SDK.
        if context is self._outer_context:
            msg = (
                f'AgentCore session "{self._name}": invoke received the context '
                "that agentcore_session was called with. Pass the context that "
                "the handler receives, or a context nested in it."
            )
            raise TypeError(msg)
        config = self._config
        scope = _AgentcoreScope.create(
            context,
            name,
            job_input,
            AgentcoreConfig(
                agent_runtime_arn=config.agent_runtime_arn,
                timeout=options.timeout,
                qualifier=config.qualifier,
                heartbeat_timeout=options.heartbeat_timeout,
                retry_strategy=config.retry_strategy,
                client=config.client,
            ),
        )
        session_id = self._runtime_session_id

        def run(job_context: DurableContext) -> Any:
            # The session stops itself after its handler. So a failed job
            # leaves the session running for the next job.
            return _run_job(job_context, scope, session_id, job_input, lambda: None)

        try:
            return context.run_in_child_context(run, name=name)
        except ChildContextError as error:
            raise AgentcoreError.from_child_context_error(name, error) from error


def _validate_session_config(name: str, config: AgentcoreSessionConfig) -> int:
    """Validate the session config, before any durable operation starts.

    It uses the rules of :func:`agentcore`. ``agent_runtime_arn`` is a
    required field, and its value is not checked here. The boto3 client and
    the service reject a value that is not valid.

    Returns:
        The session timeout in seconds.

    Raises:
        ValidationError: When ``name`` is empty, or ``timeout`` is less
            than 1 second.
    """
    return _validate_base(name, config.timeout)


def agentcore_session(
    context: DurableContext,
    name: str,
    *,
    config: AgentcoreSessionConfig,
    handler: Callable[[AgentcoreSession, DurableContext], _T],
) -> _T:
    """Run a handler in one AgentCore Runtime session, and return the
    handler's value.

    The operation runs its durable operations in one child context named
    ``name``:

    1. ``<name>.session`` generates the runtime session ID, and checkpoints
       it. So every retry and replay uses the same session.
    2. The handler runs with an :class:`AgentcoreSession` and the child
       context. Each :meth:`AgentcoreSession.invoke` sends one job to the
       session and waits for its result.
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

    A failed :meth:`AgentcoreSession.invoke` raises inside the handler. The
    handler can catch the error and send the next job to the same session.
    An error that leaves the handler reaches the caller as follows:

    1. An AgentCore error, such as :class:`AgentcoreJobFailedError`, that
       the handler raises itself reaches the caller with its own class, as
       from :func:`agentcore`.
    2. Any other error reaches the caller as ``ChildContextError``, as from
       ``run_in_child_context``. This includes an AgentCore error that
       leaves a child context that the handler created: that child context
       records ``ChildContextError`` as its error type. The caller then gets
       the AgentCore error's message, but not its class.

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
        AgentcoreError: An AgentCore error that the handler did not catch,
            such as :class:`AgentcoreJobFailedError`, with its own class.
        ChildContextError: Any other error that the handler raised.
    """
    _validate_session_config(name, config)
    retry_strategy = config.retry_strategy or default_agentcore_retry_strategy

    def run(child: DurableContext) -> _T:
        session_id = _create_session_id(child, name)

        def stop() -> None:
            _stop_session(child, name, config, retry_strategy, session_id)

        try:
            session = AgentcoreSession(
                config, session_id, name=name, outer_context=context
            )
            result = handler(session, child)
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

    try:
        return context.run_in_child_context(run, name=name)
    except ChildContextError as error:
        # The handler is caller code. So only an AgentCore error is restored
        # to its class. Every other error keeps the SDK's ChildContextError.
        if isinstance(error.__cause__, AgentcoreError):
            raise AgentcoreError.from_child_context_error(name, error) from error
        raise
