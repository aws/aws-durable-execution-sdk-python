# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The worker's own SuspendMicrovm call, for a session that suspends when idle."""

from __future__ import annotations

import threading
from concurrent.futures import Future
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
"""The longest time the worker waits for its own SuspendMicrovm call.

The call measured 75 to 94 milliseconds from inside a MicroVM in us-east-1.
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

# Error codes that show that the service did nothing.
_REJECTION_CODES = frozenset(
    {"AccessDeniedException", "ValidationException", "ResourceNotFoundException"}
)

# 4xx error codes that leave open whether the service suspended the MicroVM.
_OPEN_OUTCOME_CODES = TRANSIENT_4XX_CODES | {"ConflictException"}


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
    client: LambdaMicrovmsClient, microvm_id: str, timeout: float
) -> None:
    """Call SuspendMicrovm, and stop waiting after ``timeout`` seconds.

    botocore has no deadline for a whole call. Its read timeout limits one
    socket read, and it covers neither credential resolution nor its own
    retries. So the call runs in a daemon thread, and this function waits
    for the thread. A call that ends after the bound is ignored. The caller
    closes a client that it created, which releases the call's connection.

    Raises:
        TimeoutError: When the call takes longer than ``timeout``.
        Exception: The call's own error.
    """
    future: Future[None] = Future()

    def run() -> None:
        try:
            client.suspend_microvm(microvmIdentifier=microvm_id)
            future.set_result(None)
        except BaseException as error:  # noqa: BLE001
            future.set_exception(error)

    threading.Thread(target=run, name="microvm-suspend-call", daemon=True).start()
    try:
        future.result(timeout)
    except TimeoutError as error:
        if future.done():
            raise
        msg = f"the SuspendMicrovm call took longer than {timeout} seconds"
        raise TimeoutError(msg) from error


def is_rejected_call(error: BaseException) -> bool:
    """Whether a failed SuspendMicrovm call shows that the service did nothing.

    1. A botocore retry inside the call means that an earlier attempt may
       have suspended the MicroVM before its answer was lost. So any error
       after a retry leaves the outcome open.
    2. Missing credentials stop the call before any request.
    3. A known rejection code shows that the service did nothing.
    4. A throttle, a conflict, expired credentials, and a clock offset leave
       the outcome open, whatever their status. Some services answer a
       throttle with HTTP 400.
    5. Any other 4xx answer except 408, 409, and 429 shows that the service
       did nothing.
    6. Any other error, such as a timeout, a dropped connection, or a server
       error, leaves the outcome open.
    """
    if isinstance(error, NoCredentialsError):
        return True
    if not isinstance(error, ClientError):
        return False
    metadata = safe_get(lambda: error.response.get("ResponseMetadata", {})) or {}
    retries = safe_get(lambda: metadata.get("RetryAttempts"))
    if isinstance(retries, int) and retries > 0:
        return False
    code = error_code(error)
    if code in _REJECTION_CODES:
        return True
    if code in _OPEN_OUTCOME_CODES:
        return False
    status = safe_get(lambda: metadata.get("HTTPStatusCode"))
    return (
        isinstance(status, int)
        and 400 <= status < 500
        and status not in {408, 409, 429}
    )
