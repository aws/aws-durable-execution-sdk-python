# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The errors that the MicroVM operations raise.

Every MicroVM failure is a :class:`MicrovmError`. Catch the base class to
handle every failure the same way, or a subclass to handle one kind.

The operation raises the same class on the first run and on every replay:

1. Inside its child context, the operation raises a subclass, such as
   :class:`MicrovmLaunchError`.
2. The SDK checkpoints the child context's failure with the class's
   fully-qualified name as the error type, and the message, data, and stack
   trace.
3. The SDK then raises ``ChildContextError``, on the first run and on replay
   alike. It rebuilds the escaping error as ``__cause__`` from its registry
   of error types.
4. This module registers every MicroVM class with that registry. So
   ``__cause__`` is the MicroVM class, and the operation raises it.

The registry also applies to a child context that the caller wraps around the
operation, and to a ``map`` or ``parallel`` item. The caller then gets
``ChildContextError``, and its ``__cause__`` is the MicroVM error.

The fully-qualified name is part of the checkpoint format. A checkpoint
written by one release is replayed by the next. So the classes must stay in
this module, with these names. A moved or renamed class is not found in the
registry on replay, and the caller gets a plain ``MicrovmError`` instead.
"""

from __future__ import annotations

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    DurableOperationError,
    register_operation_error,
)


class MicrovmError(DurableOperationError):
    """The base class of every MicroVM failure.

    ``message`` names the operation and keeps the underlying message.
    ``data`` and ``stack_trace`` are those of the failure, when it had any.
    """

    @classmethod
    def from_child_context_error(
        cls, name: str, error: ChildContextError
    ) -> MicrovmError:
        """Return the MicroVM error that the operation's child context raised.

        1. The SDK rebuilds a registered MicroVM error type as its own class,
           in ``__cause__``. A copy of that error is returned.
        2. Any other error type becomes a plain :class:`MicrovmError`. It keeps
           the original error type in ``error_type``. The operation contains no
           caller code, so every failure in it is a MicroVM failure.

        The caller raises the result ``from error``. That sets the result's
        ``__cause__`` to ``error``. So the result must not be
        ``error.__cause__`` itself. Otherwise each error is the other's
        ``__cause__``, and a loop that follows ``__cause__`` never ends.

        Args:
            name: The operation name, for the message of a plain MicrovmError.
            error: The error that ``run_in_child_context`` raised.
        """
        cause = error.__cause__
        if isinstance(cause, MicrovmError):
            return type(cause)(
                message=cause.message,
                error_type=cause.error_type,
                data=cause.data,
                stack_trace=cause.stack_trace,
            )
        return MicrovmError(
            message=f'MicroVM "{name}": {error.message}',
            error_type=error.error_type,
            data=error.data,
            stack_trace=error.stack_trace,
        )


class MicrovmLaunchError(MicrovmError):
    """``RunMicrovm`` failed after all retries.

    When the last attempt got an answer, such as a ``ValidationException``,
    no MicroVM runs the job. When the last attempt timed out or lost its
    connection, the service may have created the MicroVM anyway. That MicroVM
    then runs the job, and the operation cannot terminate it, because it never
    learned the MicroVM's ID. The MicroVM ends at its maximum duration. So a
    caller that runs the job again after this error can run it twice.
    """


class MicrovmDeliveryError(MicrovmError):
    """The job could not be delivered to the MicroVM."""


class MicrovmNotRunningError(MicrovmDeliveryError):
    """The session's MicroVM can no longer run a job.

    The MicroVM is terminating, terminated, or removed. A session MicroVM
    lives at most its session ``timeout`` plus 5 minutes, and never longer
    than 8 hours, running or suspended. A ``TerminateMicrovm`` call from
    outside the session ends it too. It is a :class:`MicrovmDeliveryError`,
    so code that catches delivery failures also catches it.

    Only a session that checks the MicroVM state before each job raises it.
    It checks when ``auto_suspend_on_idle`` is ``True``, or when an
    ``idle_policy`` is set. A later attempt finds the same state, so the job
    is not retried. To continue, start a new session.

    Without the check, the session makes no ``GetMicrovm`` call. The endpoint
    of a terminated MicroVM answers 502, and the request step retries a 502
    as a MicroVM that is still starting. So the job fails with a plain
    :class:`MicrovmDeliveryError`, after the retries of both tiers.
    """


class MicrovmJobFailedError(MicrovmError):
    """The job handler in the MicroVM failed.

    The MicroVM reported the failure with
    ``SendDurableExecutionCallbackFailure``. The message includes the error
    type that the MicroVM reported, such as the job handler's exception class.

    The operation also raises it when the MicroVM sent a result that is not
    valid JSON. The JavaScript operation differs here: its SDK fails the
    invocation with ``SERDES_FAILED`` before the operation sees an error.
    """


class MicrovmTimeoutError(MicrovmError):
    """No result arrived within the job's timeout, or no heartbeat arrived
    within its heartbeat timeout.

    A MicroVM that crashed or never booted ends this way. The message keeps
    the service's reason.
    """


for _error_class in (
    MicrovmError,
    MicrovmLaunchError,
    MicrovmDeliveryError,
    MicrovmNotRunningError,
    MicrovmJobFailedError,
    MicrovmTimeoutError,
):
    # Each class takes the four keywords that the registry passes: message,
    # error_type, data, and stack_trace.
    register_operation_error(_error_class)
