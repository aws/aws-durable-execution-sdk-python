# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The errors that the ``agentcore`` operation raises.

Every AgentCore failure is an :class:`AgentcoreError`. Catch the base class to
handle every failure the same way, or a subclass to handle one kind.

The operation raises the same class on the first run and on every replay. The
mechanism is the one of the MicroVM errors:

1. Inside its child context, the operation raises a subclass, such as
   :class:`AgentcoreDeliveryError`.
2. The SDK checkpoints the child context's failure with the class's
   fully-qualified name as the error type.
3. The SDK then raises ``ChildContextError``, and rebuilds the escaping error
   as ``__cause__`` from its registry of error types.
4. This module registers every AgentCore class with that registry. So
   ``__cause__`` is the AgentCore class, and the operation raises it.

The fully-qualified name is part of the checkpoint format. So the classes
must stay in this module, with these names.
"""

from __future__ import annotations

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    register_operation_error,
)

from aws_durable_execution_sdk_python_extras.compute.errors import ComputeError


class AgentcoreError(ComputeError):
    """The base class of every AgentCore failure.

    ``message`` names the operation and keeps the underlying message.
    ``data`` and ``stack_trace`` are those of the failure, when it had any.
    """

    @classmethod
    def from_child_context_error(
        cls, name: str, error: ChildContextError
    ) -> AgentcoreError:
        """Return the AgentCore error that the operation's child context raised.

        1. The SDK rebuilds a registered AgentCore error type as its own class,
           in ``__cause__``. A copy of that error is returned.
        2. Any other error type becomes a plain :class:`AgentcoreError`. It
           keeps the original error type in ``error_type``.

        The caller raises the result ``from error``. So the result must not be
        ``error.__cause__`` itself. Otherwise each error is the other's
        ``__cause__``, and a loop that follows ``__cause__`` never ends.
        """
        cause = error.__cause__
        if isinstance(cause, AgentcoreError):
            return type(cause)(
                message=cause.message,
                error_type=cause.error_type,
                data=cause.data,
                stack_trace=cause.stack_trace,
            )
        return AgentcoreError(
            message=f'AgentCore "{name}": {error.message}',
            error_type=error.error_type,
            data=error.data,
            stack_trace=error.stack_trace,
        )


class AgentcoreDeliveryError(AgentcoreError):
    """The job could not be delivered to the agent runtime.

    ``InvokeAgentRuntime`` failed after all retries, or the agent answered
    something other than ``{"accepted": true}``.
    """


class AgentcoreJobFailedError(AgentcoreError):
    """The job handler in the agent runtime failed.

    The agent reported the failure with
    ``SendDurableExecutionCallbackFailure``. The message includes the error
    type that the agent reported, such as the job handler's exception class.

    The operation also raises it when the agent sent a result that is not
    valid JSON.
    """


class AgentcoreTimeoutError(AgentcoreError):
    """No result arrived within the job's timeout, or no heartbeat arrived
    within its heartbeat timeout.

    An agent session that crashed, or that AgentCore ended at its idle
    timeout, ends this way.
    """


for _error_class in (
    AgentcoreError,
    AgentcoreDeliveryError,
    AgentcoreJobFailedError,
    AgentcoreTimeoutError,
):
    # Each class takes the four keywords that the registry passes: message,
    # error_type, data, and stack_trace.
    register_operation_error(_error_class)
