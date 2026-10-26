# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The worker's own SuspendMicrovm call, for a session that suspends when idle.

The call can suspend the MicroVM and lose its answer in the freeze. A retry
of the call then runs after the MicroVM resumed. The ``resume`` hook has
already ended the worker's refusal of jobs at that point. So the retry would
suspend the MicroVM with the next job inside it. So the worker stops the
call's attempts after a ``resume`` hook, and its default client makes one
attempt. After a failed call, the worker can call again after a later idle
time, with a new refusal of jobs.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    TRANSIENT_4XX_CODES,
    error_code,
)
from aws_durable_execution_sdk_python_microvm_worker.logger import safe_get


if TYPE_CHECKING:
    from mypy_boto3_lambda_microvms import (
        LambdaMicroVMsClient as LambdaMicrovmsClient,
    )
else:
    LambdaMicrovmsClient = Any


SUSPEND_CALL_TIMEOUT_SECONDS = 10.0
"""No attempt of the worker's own SuspendMicrovm call starts after this time.

An attempt in flight then runs until it ends. The call measured 75 to 94
milliseconds from inside a MicroVM in us-east-1.
"""

SUSPEND_GRACE_SECONDS = 30.0
"""How long the worker refuses jobs after a suspend, when no ``resume`` hook
arrives first.

This applies after the worker's own SuspendMicrovm call returned, after a
call whose outcome is unknown, and after the ``suspend`` hook. The call
returns before the MicroVM freezes, and the worker cannot see the freeze. A
job accepted in between would freeze with the MicroVM, and nothing would
resume it. So the worker answers such a job with HTTP 503, and the durable
function resumes the MicroVM and delivers the job again. The ``resume`` hook
ends the refusal at once. This period covers an image that does not enable
that hook, and a suspend that the service did not carry out. It fits inside
the durable function's default delivery retry window.
"""

_CONNECT_TIMEOUT_SECONDS = 5

_CLIENT_ATTEMPTS = 1
"""The attempts of the default client, the first attempt included.

An attempt can suspend the MicroVM and lose its answer in the freeze. A
retry then runs after the MicroVM resumed, and can suspend it with a job
inside. So the default client makes one attempt. After a failed call, the
worker can call again after a later idle time, with a new refusal of jobs.
"""

_BEFORE_SEND = "before-send.lambda-microvms.SuspendMicrovm"

# Error codes that show that the service did nothing.
_REJECTION_CODES = frozenset(
    {"AccessDeniedException", "ValidationException", "ResourceNotFoundException"}
)


class SuspendOutcome(Enum):
    """What a failed SuspendMicrovm call shows about the MicroVM."""

    REJECTED = "rejected"
    """The service did nothing, and a later call would most likely fail the
    same way, for example for a missing permission."""
    REFUSED_FOR_NOW = "refused_for_now"
    """The service did nothing on the call's first and only attempt, and a
    later call can succeed.

    Examples are expired credentials, a signature error with or without a
    clock offset, and a throttle. The default client makes one attempt, so
    these errors keep this outcome. A caller's client that retries a
    throttle turns it into :attr:`UNKNOWN`."""
    UNKNOWN = "unknown"
    """The service may have suspended the MicroVM."""


class SuspendDeadlineError(TimeoutError):
    """The SuspendMicrovm deadline passed before an attempt could start.

    botocore retries only its own connection and HTTP client errors. So it
    does not retry this one.
    """


class SuspendResumedError(Exception):
    """A ``resume`` hook arrived before an attempt of the SuspendMicrovm call
    could start.

    The ``resume`` hook ends the worker's refusal of jobs. So a job can run
    in the MicroVM from then on. The attempt would suspend the MicroVM with
    that job inside. So the call stops before the attempt. botocore does not
    retry this error.
    """


def create_microvms_client(region: str) -> LambdaMicrovmsClient:
    """Create the default ``lambda-microvms`` client for one call.

    Each call gets its own session, because boto3's default session is not
    safe to use from several threads at once.

    The client makes one attempt. An attempt can suspend the MicroVM and
    lose its answer in the freeze. A retry would then run after the MicroVM
    resumed, and could suspend it with the next job inside. The worker calls
    again after a later idle time instead, with a new refusal of jobs.
    :func:`suspend_microvm` also stops the attempts of a caller's client
    after the MicroVM resumed. One attempt is still the recommended setting
    for such a client.
    """
    return boto3.session.Session().client(
        "lambda-microvms",
        region_name=region,
        config=Config(
            connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            read_timeout=SUSPEND_CALL_TIMEOUT_SECONDS,
            retries={
                "mode": "standard",
                # total_max_attempts counts the first attempt too.
                "total_max_attempts": _CLIENT_ATTEMPTS,
            },
        ),
    )


def suspend_microvm(
    client: LambdaMicrovmsClient,
    microvm_id: str,
    deadline_seconds: float,
    resumed: Callable[[], bool] | None = None,
) -> None:
    """Call SuspendMicrovm in this thread. Start no attempt after
    ``deadline_seconds``, and none after ``resumed()`` returns ``True``.

    botocore has no deadline for a whole call. Its read timeout limits one
    socket read, and it covers neither credential resolution nor its own
    retries. So a ``before-send`` handler on the client raises
    :class:`SuspendDeadlineError` when an attempt would start after the
    deadline. botocore does not retry that error. Without the handler,
    botocore went on retrying, and an attempt started 22.5 seconds after the
    first one.

    The same handler stops an attempt after the MicroVM resumed:

    1. An attempt can suspend the MicroVM and lose its answer in the freeze.
    2. botocore then retries the attempt after the MicroVM resumed.
    3. The ``resume`` hook has ended the worker's refusal of jobs by then.
    4. So the retry would suspend the MicroVM with a new job inside.

    So the handler raises :class:`SuspendResumedError` when ``resumed()``
    returns ``True``. botocore does not retry that error either. The handler
    runs in this thread. So ``resumed`` must not block, and must not take a
    lock that the caller of this function can hold.

    botocore emits ``before-send`` before it opens the connection, and it
    cannot cancel an attempt in flight. So an attempt that started before
    the deadline can still reach the service after it. This function
    returns only when that attempt has ended, so that the caller keeps
    refusing jobs until then. The client's connect and read timeouts bound
    the attempt: 5 and 10 seconds for the default client.

    The handler is removed when the call ends, also when it raises. So a
    client from the caller keeps no handler.

    Raises:
        SuspendDeadlineError: When an attempt would start after the
            deadline.
        SuspendResumedError: When an attempt would start after ``resumed()``
            returned ``True``.
        Exception: The call's own error.
    """
    deadline = time.monotonic() + deadline_seconds
    # A fresh ID per call. botocore ignores a second registration with an ID
    # it already has. id() of a temporary object can repeat at once.
    unique_id = f"aws-durable-execution-microvm-worker-deadline-{uuid.uuid4().hex}"
    events = safe_get(lambda: client.meta.events)

    def stop_attempt(**_: object) -> None:
        if resumed is not None and resumed():
            msg = "the MicroVM resumed before this SuspendMicrovm attempt"
            raise SuspendResumedError(msg)
        if time.monotonic() >= deadline:
            msg = (
                f"the SuspendMicrovm deadline of {deadline_seconds} seconds has passed"
            )
            raise SuspendDeadlineError(msg)

    if events is not None:
        events.register_first(_BEFORE_SEND, stop_attempt, unique_id=unique_id)
    try:
        client.suspend_microvm(microvmIdentifier=microvm_id)
    finally:
        if events is not None:
            events.unregister(_BEFORE_SEND, unique_id=unique_id)


def classify_suspend_error(error: BaseException) -> SuspendOutcome:
    """What a failed SuspendMicrovm call shows about the MicroVM.

    1. A botocore retry inside the call means that an earlier attempt may
       have suspended the MicroVM before its answer was lost. So any error
       after a retry is :attr:`SuspendOutcome.UNKNOWN`. The default client
       makes one attempt. So only a caller's client that retries reaches
       this rule.
    2. Missing credentials are :attr:`SuspendOutcome.REJECTED`, as in the
       JavaScript worker. A later call in the same MicroVM most likely finds
       no credentials either.
    3. A known rejection code is :attr:`SuspendOutcome.REJECTED`.
    4. A conflict is :attr:`SuspendOutcome.UNKNOWN`, whatever its status. It
       can mean that the MicroVM is already suspending.
    5. Expired credentials, a signature error, or a throttle on the first
       attempt is :attr:`SuspendOutcome.REFUSED_FOR_NOW`, whatever its
       status. The service answers them before it acts, and a later call can
       succeed. Some services answer a throttle with HTTP 400. botocore
       retries no credential or signature error, also with a clock offset.
       So these errors reach this rule. A throttle reaches it from the
       default client, and from any client that did not retry it.
    6. Any other 4xx answer except 408, 409, and 429 is
       :attr:`SuspendOutcome.REJECTED`. A 429 is
       :attr:`SuspendOutcome.REFUSED_FOR_NOW`.
    7. Any other error is :attr:`SuspendOutcome.UNKNOWN`. Examples are a
       timeout, a dropped connection, a 408, a server error,
       :class:`SuspendDeadlineError`, and :class:`SuspendResumedError`. The
       last two stop an attempt. When they stop the first attempt, nothing
       was sent. An earlier attempt, if there was one, may have suspended
       the MicroVM.
    """
    if isinstance(error, NoCredentialsError):
        return SuspendOutcome.REJECTED
    if not isinstance(error, ClientError):
        return SuspendOutcome.UNKNOWN
    metadata = safe_get(lambda: error.response.get("ResponseMetadata", {})) or {}
    retries = safe_get(lambda: metadata.get("RetryAttempts"))
    if isinstance(retries, int) and retries > 0:
        return SuspendOutcome.UNKNOWN
    code = error_code(error)
    if code in _REJECTION_CODES:
        return SuspendOutcome.REJECTED
    if code == "ConflictException":
        return SuspendOutcome.UNKNOWN
    if code in TRANSIENT_4XX_CODES:
        return SuspendOutcome.REFUSED_FOR_NOW
    status = safe_get(lambda: metadata.get("HTTPStatusCode"))
    if status == 429:
        return SuspendOutcome.REFUSED_FOR_NOW
    if isinstance(status, int) and 400 <= status < 500 and status not in {408, 409}:
        return SuspendOutcome.REJECTED
    return SuspendOutcome.UNKNOWN
