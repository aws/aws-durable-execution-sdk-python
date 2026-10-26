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

    A launch attempt that timed out or lost its connection can still have
    created a MicroVM. When the launch step still fails, the operation never
    learns that MicroVM's ID. So it cannot terminate that MicroVM. The
    MicroVM ends at its maximum duration.

    1. With ``run`` hook delivery, the launch request holds the job. So that
       MicroVM runs the job. A caller that runs the job again after this
       error can run it twice.
    2. With HTTP delivery, the launch request holds no job. The request step
       starts only after a launch succeeds. So when you invoke through a
       version or an alias, no MicroVM runs the job.
    3. When no attempt created a MicroVM, no MicroVM runs the job.

    When the service answered, the message ends with
    ``RunMicrovm request ID: <id>.``.
    """


class MicrovmDeliveryError(MicrovmError):
    """The job could not be delivered to the MicroVM.

    When a Lambda MicroVMs call failed, the message ends with its request
    ID, for example ``CreateMicrovmAuthToken request ID: <id>.``.
    """


class MicrovmNotRunningError(MicrovmDeliveryError):
    """The MicroVM can no longer run a job.

    The MicroVM is terminating, terminated, or removed. A MicroVM lives at
    most its ``timeout`` plus 5 minutes, and never longer than 8 hours,
    running or suspended. A ``TerminateMicrovm`` call from outside the
    operation ends it too. It is a :class:`MicrovmDeliveryError`, so code
    that catches delivery failures also catches it. A later attempt finds
    the same state, so the job is not retried. To continue a session, start
    a new session.

    Two checks raise it:

    1. A session that checks the MicroVM state before each job. It checks
       when ``auto_suspend_on_idle`` is ``True``, or when an ``idle_policy``
       is set. ``GetMicrovm`` or ``ResumeMicrovm`` then reports the state.
    2. Every job over HTTP, of a session or of :func:`microvm`. The request
       step calls ``CreateMicrovmAuthToken`` before the first request, and
       again after a 401 or 403. That call can answer
       ``ResourceNotFoundException``.

    The message ends with the request ID of the call that reported the
    state, for example ``GetMicrovm request ID: <id>.``. The core SDK
    records only the message of the failure. So a support case can quote
    the request ID from the message, on the first run and on replay.

    Without the state check, a session makes no ``GetMicrovm`` call. The
    request step still calls ``CreateMicrovmAuthToken`` first. When that
    call answers ``ResourceNotFoundException``, the job fails with this
    error. When it returns a token, the request goes to the endpoint. The
    endpoint of a terminated MicroVM answers 502, and the request step
    retries a 502 as a MicroVM that is still starting. So the job then fails
    with a plain :class:`MicrovmDeliveryError`, after the retries of both
    tiers.

    The live service's answer of ``CreateMicrovmAuthToken`` for a removed
    MicroVM is not verified. The API model lists
    ``ResourceNotFoundException`` for that call.
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
