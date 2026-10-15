# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Resume a suspended session MicroVM before its next job."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.types import LoggerInterface
from botocore.exceptions import ClientError

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
    default retry strategy does not retry it. The session job then raises
    :class:`MicrovmNotRunningError`.
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
       :class:`MicrovmStateError`.

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

    def not_found() -> MicrovmStateError:
        msg = (
            f"MicroVM {microvm_id} no longer exists. A session MicroVM lives at "
            "most its session timeout plus 5 minutes, and never longer than 8 "
            "hours."
        )
        return MicrovmStateError(msg)

    while True:
        try:
            current = get_microvm()
        except ClientError as error:
            if _code(error) == "ResourceNotFoundException":
                raise not_found() from error
            raise
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
            msg = f"MicroVM {microvm_id} is {state}{f': {reason}' if reason else ''}."
            raise MicrovmStateError(msg, state)
        if state == "SUSPENDED" and not resume_sent:
            try:
                resume_microvm()
            except ClientError as error:
                # ConflictException means that a transition is already in
                # progress, for example an auto-resume. Polling continues
                # either way. ResourceNotFoundException means that the
                # MicroVM was removed after the state check.
                code = _code(error)
                if code == "ResourceNotFoundException":
                    raise not_found() from error
                if code != "ConflictException":
                    raise
            resume_sent = True
            options.logger.info("resuming MicroVM", extra={"state": state})

        if time.monotonic() + delay > deadline:
            raise unavailable()
        options.sleep(delay)
        delay = min(delay * 2, _MAX_POLL_SECONDS)


def _code(error: ClientError) -> str | None:
    return error.response.get("Error", {}).get("Code")
