# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Check whether a MicroVM that gave a sign that it does not run has ended."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_sdk_python_extras.compute.microvm.aws_errors import (
    add_request_id,
    error_code,
    is_microvms_service_error,
    with_request_id,
)
from aws_durable_execution_sdk_python_extras.compute.microvm.request import (
    MicrovmEndpointUnavailableError,
    call_before_deadline,
    reserve_starts_at,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_extras.compute.microvm.config import (
        LambdaMicrovmsClient,
    )


class MicrovmStateError(Exception):
    """The MicroVM is terminating, terminated, or removed.

    The request step raises it. A later attempt finds the same state, so the
    default retry strategy does not retry it. The delivery then raises
    :class:`MicrovmNotRunningError`.

    The message ends with the request ID of the AWS call that reported the
    state, when that call has one. For a MicroVM that no longer exists,
    ``__cause__`` is the botocore error. Only the step retry strategy of a
    live attempt sees ``__cause__``. The core SDK records the message, not
    the cause. So the message carries the request ID.

    The request step records the fully-qualified name of this class as its
    error type, and the operation compares the recorded type with that name.
    So the class stays in this module.
    """

    def __init__(self, message: str, state: str | None = None) -> None:
        super().__init__(message)
        self.state = state
        """The MicroVM state, or ``None`` when the MicroVM no longer exists."""


class MicrovmStateCheckError(Exception):
    """``GetMicrovm`` failed in the check of :func:`check_not_ended`.

    The request step runs that check only after a sign that the MicroVM does
    not run. A failure other than a denied call can be transient, such as
    throttling, a 5xx answer, or a connection error. So the step logs one
    warning per step attempt, and keeps sending the job. With the default
    retry strategy, that is up to 5 warnings. ``__cause__`` is the error of
    the call. A denied call raises :class:`MicrovmPermissionDeniedError`
    instead.
    """


class MicrovmPermissionDeniedError(Exception):
    """``GetMicrovm`` was denied in the check of :func:`check_not_ended`.

    A later call is denied the same way. So the default retry strategy does
    not retry it, and the delivery raises ``MicrovmDeliveryError`` at once.
    The message names the denied action and the permission, and ends with
    the request ID of the call. ``__cause__`` is the error of the call.
    """


def check_not_ended(
    client: LambdaMicrovmsClient,
    microvm_id: str,
    remaining_seconds: Callable[[], float | None],
) -> str | None:
    """Check the MicroVM state once, and return its current endpoint.

    The request step calls it after a sign that the MicroVM does not run: an
    HTTP 502 from the endpoint with ``x-aws-proxy-error:
    MICROVM_CONNECT_FAILED``, or no answer. The endpoint of a terminated
    MicroVM answers that 502, measured in us-east-1. So the check ends the
    job of a dead MicroVM at once, instead of after both retry tiers.

    The endpoint of a suspended MicroVM gives the same sign. The SDK does not
    resume a MicroVM. The service resumes a MicroVM whose idle policy sets
    ``autoResumeEnabled``, on the next request. So a suspending or suspended
    MicroVM is not an error here, and the caller keeps sending the job.

    1. ``GetMicrovm`` runs once. ``AccessDeniedException`` raises
       :class:`MicrovmPermissionDeniedError`. Any other failure except
       ``ResourceNotFoundException`` raises :class:`MicrovmStateCheckError`.
       The caller then keeps sending the job.
    2. ``TERMINATING``, ``TERMINATED``, or not found: it raises
       :class:`MicrovmStateError`.
    3. Any other state: it returns the endpoint.

    The call stops 10 seconds before the invocation times out, with
    ``MicrovmEndpointUnavailableError``.

    Returns:
        The endpoint that ``GetMicrovm`` reports, or ``None`` when it reports
        none.

    Raises:
        MicrovmPermissionDeniedError: When ``GetMicrovm`` is denied.
        MicrovmStateCheckError: When ``GetMicrovm`` fails otherwise.
        MicrovmStateError: When the MicroVM can no longer run a job.
        MicrovmEndpointUnavailableError: When the call does not end before
            the invocation reserve.
    """
    started = time.monotonic()

    def unavailable() -> Exception:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        return MicrovmEndpointUnavailableError(
            f"GetMicrovm for MicroVM {microvm_id} did not end before the "
            f"invocation reserve, after {elapsed_ms} ms."
        )

    try:
        current = call_before_deadline(
            lambda: client.get_microvm(microvmIdentifier=microvm_id),
            reserve_starts_at(remaining_seconds),
            unavailable,
        )
    except ClientError as error:
        code = error_code(error)
        if code == "ResourceNotFoundException":
            raise microvm_not_found(microvm_id, "GetMicrovm", error) from error
        if code == "AccessDeniedException":
            raise _denied(microvm_id, error) from error
        raise MicrovmStateCheckError(
            str(add_request_id("GetMicrovm", error))
        ) from error
    except BotoCoreError as error:
        raise MicrovmStateCheckError(str(error)) from error
    _raise_if_ended(microvm_id, current)
    return current.get("endpoint")


def _denied(microvm_id: str, error: ClientError) -> MicrovmPermissionDeniedError:
    """Return the error for a denied ``GetMicrovm`` call.

    The message names the action and the permission, and ends with the
    request ID of the call. An error of another call, such as STS, carries
    the request ID of that call. So the message then has no request ID.
    """
    msg = (
        f"MicroVM {microvm_id} gave a sign that it does not run. GetMicrovm "
        "was denied, so the operation cannot check its state. The function's "
        "execution role needs lambda:GetMicrovm."
    )
    response = (
        error.response if is_microvms_service_error("GetMicrovm", error) else None
    )
    return MicrovmPermissionDeniedError(with_request_id(msg, "GetMicrovm", response))


def _raise_if_ended(microvm_id: str, current: Any) -> None:
    """Raise :class:`MicrovmStateError` for a terminating or terminated
    MicroVM."""
    state = current.get("state")
    if state in {"TERMINATING", "TERMINATED"}:
        reason = current.get("stateReason")
        # The service's stateReason can end with a period, such as
        # "Success.". The message adds its own period, so one trailing
        # period of stateReason is removed.
        if reason and reason.endswith("."):
            reason = reason[:-1]
        # The GetMicrovm output carries the request ID. The state fails
        # the step, so the message names that request ID.
        msg = f"MicroVM {microvm_id} is {state}{f': {reason}' if reason else ''}."
        raise MicrovmStateError(with_request_id(msg, "GetMicrovm", current), state)


def microvm_not_found(
    microvm_id: str,
    operation: Literal["GetMicrovm", "CreateMicrovmAuthToken"],
    error: ClientError,
) -> MicrovmStateError:
    """Return the :class:`MicrovmStateError` for a call that answered
    ``ResourceNotFoundException``.

    ``GetMicrovm`` and ``CreateMicrovmAuthToken`` each answer
    ``ResourceNotFoundException`` for a removed MicroVM. The request step
    calls both. So each call reports a removed MicroVM with the same error.
    The core SDK records only the message of a step failure. So the message
    names the call and its request ID. The caller raises the result ``from
    error``. The caller matches the error by its code. So the message names
    a request ID only when ``error`` is the answer of ``operation``, as in
    :func:`add_request_id`.

    The caller can be a session or :func:`microvm`. Both configs name the
    MicroVM lifetime ``timeout``. Both launches set
    ``maximumDurationInSeconds`` to ``timeout`` plus 5 minutes, at most 8
    hours. So the message states that limit in words that fit both callers.
    """
    msg = (
        f"MicroVM {microvm_id} no longer exists. A MicroVM lives at most its "
        "`timeout` plus 5 minutes, and never longer than 8 hours."
    )
    response = error.response if is_microvms_service_error(operation, error) else None
    return MicrovmStateError(with_request_id(msg, operation, response))
