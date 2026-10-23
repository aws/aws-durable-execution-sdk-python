# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Resume a suspended session MicroVM before its next job."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from aws_durable_execution_sdk_python.types import LoggerInterface
from botocore.exceptions import ClientError

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
    3. ``SUSPENDED``: it calls ``ResumeMicrovm`` once, then waits for
       ``RUNNING``. The MicroVM stays ``SUSPENDED`` while its ``resume`` hook
       runs. So the function does not call ``ResumeMicrovm`` again for each
       ``SUSPENDED`` answer.
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
    is not an error: polling continues.

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
    started = time.monotonic()
    reserve = reserve_starts_at(options.remaining_seconds)
    deadline = started + options.max_wait_seconds
    if reserve is not None:
        deadline = min(deadline, reserve)
    resume_sent = False
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
        if state == "RUNNING" or (state == "PENDING" and not resume_sent):
            if resume_sent:
                options.logger.info(
                    "MicroVM resumed",
                    extra={"waitedMs": round((time.monotonic() - started) * 1000)},
                )
            return current.get("endpoint")
        if state in {"TERMINATING", "TERMINATED"}:
            reason = current.get("stateReason")
            # The GetMicrovm output carries the request ID. The state fails
            # the step, so the message names that request ID.
            msg = f"MicroVM {microvm_id} is {state}{f': {reason}' if reason else ''}."
            raise MicrovmStateError(with_request_id(msg, "GetMicrovm", current), state)
        if state == "SUSPENDED" and not resume_sent:
            try:
                resume_microvm()
            except ClientError as error:
                # ConflictException means that a transition is already in
                # progress, for example an auto-resume. Polling continues
                # either way. ResourceNotFoundException means that the
                # MicroVM was removed after the state check.
                code = error_code(error)
                if code == "ResourceNotFoundException":
                    raise microvm_not_found(
                        microvm_id, "ResumeMicrovm", error
                    ) from error
                if code != "ConflictException":
                    raise add_request_id("ResumeMicrovm", error)
            resume_sent = True
            options.logger.info("resuming MicroVM", extra={"state": state})

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
