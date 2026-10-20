# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The worker's own SuspendMicrovm call, for a session that suspends when idle."""

from __future__ import annotations

import time
import uuid
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

_CLIENT_ATTEMPTS = 3
"""The attempts of the default client, the first attempt included.

The JavaScript worker's default client makes 3 attempts too.
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
    """The service did nothing, and a later call can succeed: a throttle,
    expired credentials, or a clock offset."""
    UNKNOWN = "unknown"
    """The service may have suspended the MicroVM."""


class SuspendDeadlineError(TimeoutError):
    """The SuspendMicrovm deadline passed before an attempt could start.

    botocore retries only its own connection and HTTP client errors. So it
    does not retry this one.
    """


def create_microvms_client(region: str) -> LambdaMicrovmsClient:
    """Create the default ``lambda-microvms`` client for one call.

    Each call gets its own session, because boto3's default session is not
    safe to use from several threads at once.
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
    client: LambdaMicrovmsClient, microvm_id: str, deadline_seconds: float
) -> None:
    """Call SuspendMicrovm in this thread, and start no attempt after
    ``deadline_seconds``.

    botocore has no deadline for a whole call. Its read timeout limits one
    socket read, and it covers neither credential resolution nor its own
    retries. So a ``before-send`` handler on the client raises
    :class:`SuspendDeadlineError` when an attempt would start after the
    deadline. botocore does not retry that error. Without the handler,
    botocore went on retrying, and an attempt started 22.5 seconds after the
    first one.

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
        Exception: The call's own error.
    """
    deadline = time.monotonic() + deadline_seconds
    # A fresh ID per call. botocore ignores a second registration with an ID
    # it already has. id() of a temporary object can repeat at once.
    unique_id = f"aws-durable-execution-microvm-worker-deadline-{uuid.uuid4().hex}"
    events = safe_get(lambda: client.meta.events)

    def stop_after_deadline(**_: object) -> None:
        if time.monotonic() >= deadline:
            msg = (
                f"the SuspendMicrovm deadline of {deadline_seconds} seconds has passed"
            )
            raise SuspendDeadlineError(msg)

    if events is not None:
        events.register_first(_BEFORE_SEND, stop_after_deadline, unique_id=unique_id)
    try:
        client.suspend_microvm(microvmIdentifier=microvm_id)
    finally:
        if events is not None:
            events.unregister(_BEFORE_SEND, unique_id=unique_id)


def classify_suspend_error(error: BaseException) -> SuspendOutcome:
    """What a failed SuspendMicrovm call shows about the MicroVM.

    1. A botocore retry inside the call means that an earlier attempt may
       have suspended the MicroVM before its answer was lost. So any error
       after a retry is :attr:`SuspendOutcome.UNKNOWN`.
    2. Missing credentials stop the call before any request. They are
       :attr:`SuspendOutcome.REJECTED`, as in the JavaScript worker.
    3. A known rejection code is :attr:`SuspendOutcome.REJECTED`.
    4. A conflict is :attr:`SuspendOutcome.UNKNOWN`, whatever its status. It
       can mean that the MicroVM is already suspending.
    5. A throttle, expired credentials, or a clock offset on the first
       attempt is :attr:`SuspendOutcome.REFUSED_FOR_NOW`, whatever its
       status. The service answers them before it acts, and a later call can
       succeed. Some services answer a throttle with HTTP 400.
    6. Any other 4xx answer except 408, 409, and 429 is
       :attr:`SuspendOutcome.REJECTED`. A 429 is
       :attr:`SuspendOutcome.REFUSED_FOR_NOW`.
    7. Any other error, such as a timeout, a dropped connection, a 408, or a
       server error, is :attr:`SuspendOutcome.UNKNOWN`.
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
