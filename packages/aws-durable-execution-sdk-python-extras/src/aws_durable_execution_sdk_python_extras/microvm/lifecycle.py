# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Resume a suspended session MicroVM before its next job."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal

from aws_durable_execution_sdk_python.types import LoggerInterface
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_sdk_python_extras.microvm.aws_errors import (
    add_request_id,
    error_code,
    is_microvms_service_error,
    with_request_id,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    MicrovmEndpointUnavailableError,
    call_before_deadline,
    reserve_starts_at,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_extras.microvm.config import (
        LambdaMicrovmsClient,
    )


_INITIAL_POLL_SECONDS = 0.25
_MAX_POLL_SECONDS = 2.0
"""The first poll comes soon, because a resume measured in us-east-1 took
about 1 second. Later polls back off, so a slow transition costs few
``GetMicrovm`` calls."""


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
    """``GetMicrovm`` failed in the check of :func:`resume_if_suspended`.

    The request step of a session without the resume mode runs that check
    only after a sign of a suspended MicroVM. The session may never have
    asked to suspend. So the step logs one warning per job, and keeps
    sending the job. ``__cause__`` is the error of the call.
    """

    def __init__(self, message: str, *, denied: bool) -> None:
        super().__init__(message)
        self.denied = denied
        """Whether the service answered ``AccessDeniedException``."""


class MicrovmResumeDeniedError(Exception):
    """``ResumeMicrovm`` was denied for a suspending or suspended MicroVM.

    Nothing else resumes the MicroVM, and a later attempt is denied the same
    way. So the default retry strategy does not retry it, and the delivery
    raises ``MicrovmDeliveryError`` at once. The message names the
    permission, and ends with the request ID of the call.
    """


@dataclass(frozen=True, kw_only=True)
class EnsureRunningOptions:
    """The inputs of :func:`ensure_running`."""

    client: LambdaMicrovmsClient
    microvm_id: str
    max_wait_seconds: float
    """The longest time to wait for a transition."""
    remaining_seconds: Callable[[], float | None]
    """The remaining invocation time. ``None`` means that the compute reports
    no deadline, and only ``max_wait_seconds`` applies."""
    logger: LoggerInterface
    sleep: Callable[[float], None] = time.sleep


def ensure_running(options: EnsureRunningOptions) -> str | None:
    """Make sure the MicroVM can take a job, and return its current endpoint.

    A session MicroVM can be suspended between jobs, by its worker or by an
    idle policy. So the next job can find the MicroVM in any of these states:

    1. ``RUNNING``: the function returns at once.
    2. ``PENDING``: the function returns at once. A new MicroVM reports
       ``PENDING`` until it boots. The request's own retries wait for the
       ``run`` hook.
    3. ``SUSPENDED``: it calls ``ResumeMicrovm``, then waits for
       ``RUNNING``. The MicroVM stays ``SUSPENDED`` while its ``resume`` hook
       runs. So after a successful call, the function does not call
       ``ResumeMicrovm`` again for each ``SUSPENDED`` answer.
    4. ``SUSPENDING``: it waits. The MicroVM becomes ``SUSPENDED``, and the
       function then resumes it.
    5. ``TERMINATING``, ``TERMINATED``, or not found: it raises
       :class:`MicrovmStateError`. ``GetMicrovm`` or ``ResumeMicrovm`` can
       report that the MicroVM is not found. The message ends with the
       request ID of the call that reported the state, in the form
       ``GetMicrovm request ID: <id>.``.

    Any other ``GetMicrovm`` or ``ResumeMicrovm`` error, such as
    ``AccessDeniedException``, is raised with its class and its
    ``response``. Its message then ends with the request ID of the call. See
    :func:`add_request_id`. A ``ConflictException`` from ``ResumeMicrovm``
    is not an error: polling continues. The call did not resume the
    MicroVM. So the next ``SUSPENDED`` answer calls ``ResumeMicrovm`` again,
    at most once per poll.

    It polls after 250 milliseconds, and doubles the wait up to 2 seconds. It
    stops after ``max_wait_seconds``, or 10 seconds before the invocation
    times out. It then raises ``MicrovmEndpointUnavailableError``, and the
    step retry strategy takes over. Each ``GetMicrovm`` and ``ResumeMicrovm``
    call also stops 10 seconds before the invocation times out, with the same
    error.

    Returns:
        The endpoint that ``GetMicrovm`` reports, or ``None`` when it reports
        none.

    Raises:
        MicrovmStateError: When the MicroVM can no longer run a job.
        MicrovmEndpointUnavailableError: When the MicroVM does not reach
            ``RUNNING`` in time.
        ClientError: When ``GetMicrovm`` or ``ResumeMicrovm`` fails
            otherwise.
    """
    return _wait_until_running(options, resuming=False, resume_accepted=False)


def resume_if_suspended(options: EnsureRunningOptions) -> str | None:
    """Check the MicroVM state once, resume a suspended MicroVM, and return
    its current endpoint.

    The request step of a session without the resume mode calls it after a
    sign that the MicroVM may be suspended: an HTTP 502 from the endpoint
    with ``x-aws-proxy-error: MICROVM_CONNECT_FAILED``, or no answer. A
    worker can suspend its MicroVM after a ``suspend_after`` job. The
    session can lose the record of that job on replay. So this check runs
    for every session job.

    1. ``GetMicrovm`` runs once. A failure other than
       ``ResourceNotFoundException`` raises :class:`MicrovmStateCheckError`.
       The caller then keeps sending the job.
    2. ``RUNNING`` or ``PENDING``: the function returns at once.
    3. ``SUSPENDING`` or ``SUSPENDED``: it calls ``ResumeMicrovm`` once.
       The service honors a resume during ``SUSPENDING`` too. It then waits
       for ``RUNNING``, as :func:`ensure_running` does after its own resume.
    4. ``TERMINATING``, ``TERMINATED``, or not found: it raises
       :class:`MicrovmStateError`.

    A denied ``ResumeMicrovm`` raises :class:`MicrovmResumeDeniedError`.
    ``ConflictException`` is not an error: polling continues. A resume that
    the service rejects with a conflict while the MicroVM is still
    suspending is sent again once the MicroVM is suspended. Any other
    ``ResumeMicrovm`` error is raised with its class and its ``response``,
    as in :func:`ensure_running`. So is any error of a ``GetMicrovm`` call
    after the resume.

    Returns:
        The endpoint that ``GetMicrovm`` reports, or ``None`` when it reports
        none.

    Raises:
        MicrovmStateCheckError: When the first ``GetMicrovm`` call fails.
        MicrovmStateError: When the MicroVM can no longer run a job.
        MicrovmResumeDeniedError: When ``ResumeMicrovm`` is denied.
        MicrovmEndpointUnavailableError: When the MicroVM does not reach
            ``RUNNING`` in time.
        ClientError: When ``ResumeMicrovm`` fails otherwise, or a later
            ``GetMicrovm`` call fails.
    """
    started = time.monotonic()
    reserve = reserve_starts_at(options.remaining_seconds)
    microvm_id = options.microvm_id

    def unavailable() -> Exception:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        return MicrovmEndpointUnavailableError(
            f"MicroVM {microvm_id} did not reach RUNNING in {elapsed_ms} ms. "
            "Last state: unknown."
        )

    try:
        current = call_before_deadline(
            lambda: options.client.get_microvm(microvmIdentifier=microvm_id),
            reserve,
            unavailable,
        )
    except ClientError as error:
        code = error_code(error)
        if code == "ResourceNotFoundException":
            raise microvm_not_found(microvm_id, "GetMicrovm", error) from error
        raise MicrovmStateCheckError(
            str(add_request_id("GetMicrovm", error)),
            denied=code == "AccessDeniedException",
        ) from error
    except BotoCoreError as error:
        raise MicrovmStateCheckError(str(error), denied=False) from error
    state = current.get("state")
    _raise_if_ended(microvm_id, current)
    if state not in {"SUSPENDING", "SUSPENDED"}:
        return current.get("endpoint")
    try:
        call_before_deadline(
            lambda: options.client.resume_microvm(microvmIdentifier=microvm_id),
            reserve,
            unavailable,
        )
        accepted = True
    except ClientError as error:
        code = error_code(error)
        if code == "ResourceNotFoundException":
            raise microvm_not_found(microvm_id, "ResumeMicrovm", error) from error
        if code == "AccessDeniedException":
            msg = (
                f"MicroVM {microvm_id} is {state}. ResumeMicrovm was denied, "
                "so the operation cannot resume it. The function's execution "
                "role needs lambda:ResumeMicrovm."
            )
            response = (
                error.response
                if is_microvms_service_error("ResumeMicrovm", error)
                else None
            )
            raise MicrovmResumeDeniedError(
                with_request_id(msg, "ResumeMicrovm", response)
            ) from error
        if code != "ConflictException":
            raise add_request_id("ResumeMicrovm", error)
        # A conflict can mean that the suspend has not ended. The MicroVM
        # then becomes SUSPENDED and needs a new resume. So the loop sends
        # ResumeMicrovm again at the next SUSPENDED answer.
        accepted = False
    options.logger.info("resuming MicroVM", extra={"state": state})
    remaining = options.max_wait_seconds - (time.monotonic() - started)
    return _wait_until_running(
        replace(options, max_wait_seconds=remaining),
        resuming=True,
        resume_accepted=accepted,
    )


def _raise_if_ended(microvm_id: str, current: Any) -> None:
    """Raise :class:`MicrovmStateError` for a terminating or terminated
    MicroVM."""
    state = current.get("state")
    if state in {"TERMINATING", "TERMINATED"}:
        reason = current.get("stateReason")
        # The GetMicrovm output carries the request ID. The state fails
        # the step, so the message names that request ID.
        msg = f"MicroVM {microvm_id} is {state}{f': {reason}' if reason else ''}."
        raise MicrovmStateError(with_request_id(msg, "GetMicrovm", current), state)


def _wait_until_running(
    options: EnsureRunningOptions, *, resuming: bool, resume_accepted: bool
) -> str | None:
    """The loop of :func:`ensure_running`.

    ``resuming`` states that the caller has already called
    ``ResumeMicrovm``. A ``PENDING`` answer is then a step of the resume, so
    the loop waits for ``RUNNING``. ``resume_accepted`` states that a
    ``ResumeMicrovm`` call succeeded. A call that answered
    ``ConflictException`` leaves it ``False``. So the next ``SUSPENDED``
    answer sends ``ResumeMicrovm`` again."""
    started = time.monotonic()
    reserve = reserve_starts_at(options.remaining_seconds)
    deadline = started + options.max_wait_seconds
    if reserve is not None:
        deadline = min(deadline, reserve)
    delay = _INITIAL_POLL_SECONDS
    last_state: str | None = None
    microvm_id = options.microvm_id

    def unavailable() -> Exception:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        return MicrovmEndpointUnavailableError(
            f"MicroVM {microvm_id} did not reach RUNNING in {elapsed_ms} ms. "
            f"Last state: {last_state or 'unknown'}."
        )

    # Each call must end before the invocation reserve. A Lambda timeout
    # during the step attempt would record no outcome.
    def get_microvm() -> Any:
        return call_before_deadline(
            lambda: options.client.get_microvm(microvmIdentifier=microvm_id),
            reserve,
            unavailable,
        )

    def resume_microvm() -> Any:
        return call_before_deadline(
            lambda: options.client.resume_microvm(microvmIdentifier=microvm_id),
            reserve,
            unavailable,
        )

    # GetMicrovm and ResumeMicrovm both answer ResourceNotFoundException for
    # a removed MicroVM. So both raise the same MicrovmStateError. Every other
    # error of either call keeps its class and its response, and its message
    # gets the request ID. add_request_id returns the error, or a copy whose
    # __cause__ is the error. So that raise has no `from`.
    while True:
        try:
            current = get_microvm()
        except ClientError as error:
            if error_code(error) == "ResourceNotFoundException":
                raise microvm_not_found(microvm_id, "GetMicrovm", error) from error
            raise add_request_id("GetMicrovm", error)
        state = current.get("state")
        last_state = state
        if state == "RUNNING" or (state == "PENDING" and not resuming):
            if resuming:
                options.logger.info(
                    "MicroVM resumed",
                    extra={"waitedMs": round((time.monotonic() - started) * 1000)},
                )
            return current.get("endpoint")
        _raise_if_ended(microvm_id, current)
        if state == "SUSPENDED" and not resume_accepted:
            try:
                resume_microvm()
                resume_accepted = True
            except ClientError as error:
                # ConflictException means that a transition is in progress,
                # for example a suspend that has not ended, or an
                # auto-resume. The resume stays unaccepted, so the next
                # SUSPENDED answer sends it again. The poll interval and the
                # deadline bound those calls. ResourceNotFoundException
                # means that the MicroVM was removed after the state check.
                code = error_code(error)
                if code == "ResourceNotFoundException":
                    raise microvm_not_found(
                        microvm_id, "ResumeMicrovm", error
                    ) from error
                if code != "ConflictException":
                    raise add_request_id("ResumeMicrovm", error)
            if not resuming:
                options.logger.info("resuming MicroVM", extra={"state": state})
            resuming = True

        if time.monotonic() + delay > deadline:
            raise unavailable()
        options.sleep(delay)
        delay = min(delay * 2, _MAX_POLL_SECONDS)


def microvm_not_found(
    microvm_id: str,
    operation: Literal["GetMicrovm", "ResumeMicrovm", "CreateMicrovmAuthToken"],
    error: ClientError,
) -> MicrovmStateError:
    """Return the :class:`MicrovmStateError` for a call that answered
    ``ResourceNotFoundException``.

    ``GetMicrovm``, ``ResumeMicrovm``, and ``CreateMicrovmAuthToken`` each
    answer ``ResourceNotFoundException`` for a removed MicroVM. The request
    step calls all three. So each call reports a removed MicroVM with the
    same error. The core SDK records only the message of a step failure. So
    the message names the call and its request ID. The caller raises the
    result ``from error``. The caller matches the error by its code. So the
    message names a request ID only when ``error`` is the answer of
    ``operation``, as in :func:`add_request_id`.

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
