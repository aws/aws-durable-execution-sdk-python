# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Reports heartbeats and the job outcome to one durable callback."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, TypeVar

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError


from aws_durable_execution_sdk_python_microvm_worker.logger import (
    MicrovmWorkerLogger,
    safe_get,
    safe_logger,
    safe_text,
)


if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_lambda.type_defs import ErrorObjectTypeDef
else:
    LambdaClient = Any

T = TypeVar("T")

MAX_CALLBACK_RESULT_BYTES = 256 * 1024
"""SendDurableExecutionCallbackSuccess accepts a result of at most 256 KB."""

MAX_ERROR_MESSAGE_CHARS = 8 * 1024
"""The longest error message that the worker sends.

A handler error can embed a large response body. The message reaches the
durable execution history, so the worker keeps it short.
"""

MAX_ERROR_TYPE_CHARS = 256
"""The longest error type that the worker sends."""

COMPLETION_ATTEMPTS = 5
"""Completion attempts. The delays between them are 1, 2, 4, and 8 seconds."""

COMPLETION_CALL_TIMEOUT_SECONDS = 30.0
"""How long one completion attempt may take.

A request on a dead connection would otherwise wait for the operating
system's TCP timeout, which is minutes. A timed-out attempt is retried like
any transient failure.
"""

ALREADY_COMPLETE_CODE = "InvalidParameterValueException"
"""The error code that the callback APIs return for a callback that is
already complete. It also covers a callback ID that the service does not
accept."""

_TERMINAL_ERROR_CODES = frozenset(
    {
        # The callback or its heartbeat timed out.
        "CallbackTimeoutException",
        # The service does not accept the callback ID, for example because the
        # callback is already complete.
        ALREADY_COMPLETE_CODE,
        # The callback does not exist.
        "ResourceNotFoundException",
    }
)

_EXPIRED_CREDENTIAL_CODES = frozenset({"ExpiredTokenException", "ExpiredToken"})

# 4xx error codes that a later attempt can fix: expired credentials, a
# signature rejected because of a clock offset, and throttling. The credential
# provider refreshes expired credentials. So a later attempt can succeed,
# although the service answered 403.
_TRANSIENT_4XX_CODES = _EXPIRED_CREDENTIAL_CODES | frozenset(
    {
        # Clock skew.
        "AuthFailure",
        "InvalidSignatureException",
        "RequestExpired",
        "RequestInTheFuture",
        "RequestTimeTooSkewed",
        "SignatureDoesNotMatch",
        # Throttling.
        "BandwidthLimitExceeded",
        "EC2ThrottledException",
        "LimitExceededException",
        "PriorRequestNotComplete",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "SlowDown",
        "ThrottledException",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
        "TransactionInProgressException",
    }
)


class ResultSerializationError(TypeError):
    """The job result cannot be serialized as JSON.

    Examples are a set, an object of a custom class, a cycle, and a float
    that is ``nan`` or infinite. ``json.dumps`` would write ``NaN`` for such
    a float, which is not JSON, so the reporter rejects it. No call is made.
    So the caller can report a failure instead.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(
            "The job result is not JSON-serializable: "
            f"{safe_text(lambda: str(cause), 'unknown error')}"
        )
        self.__cause__ = cause


class ResultTooLargeError(ValueError):
    """The serialized job result is larger than 256 KB. No call is made."""

    def __init__(self, size: int) -> None:
        super().__init__(
            f"The job result is {size} bytes, and "
            "SendDurableExecutionCallbackSuccess accepts at most "
            f"{MAX_CALLBACK_RESULT_BYTES}. Store large results elsewhere, for "
            "example in S3, and return a reference."
        )
        self.size = size


class CallCancelledError(Exception):
    """A callback API call that a :class:`CancelScope` ended."""


class CancelScope:
    """Ends the calls that wait on it at once.

    The heartbeats of one job share one scope. When the job ends, the scope
    is cancelled, and a heartbeat call in flight returns at once. The job
    then does not wait for the call's timeout.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancelled = False
        self._waiters: set[threading.Event] = set()

    @property
    def cancelled(self) -> bool:
        """Whether :meth:`cancel` was called."""
        return self._cancelled

    def cancel(self) -> None:
        """Wake every waiting call. A later call fails at once."""
        with self._lock:
            self._cancelled = True
            waiters = list(self._waiters)
        for waiter in waiters:
            waiter.set()

    def _register(self, waiter: threading.Event) -> bool:
        with self._lock:
            if self._cancelled:
                return False
            self._waiters.add(waiter)
            return True

    def _unregister(self, waiter: threading.Event) -> None:
        with self._lock:
            self._waiters.discard(waiter)


def error_code(error: BaseException) -> str | None:
    """The service error code of a botocore ``ClientError``, or ``None``."""
    if not isinstance(error, ClientError):
        return None
    code = safe_get(lambda: error.response.get("Error", {}).get("Code"))
    return code if isinstance(code, str) else None


def _http_status(error: BaseException) -> int | None:
    if not isinstance(error, ClientError):
        return None
    status = safe_get(
        lambda: error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    )
    return status if isinstance(status, int) else None


def _sdk_retries(error: BaseException) -> int:
    """The retries botocore made inside one call: 0 for a single try."""
    if not isinstance(error, ClientError):
        return 0
    retries = safe_get(
        lambda: error.response.get("ResponseMetadata", {}).get("RetryAttempts")
    )
    return retries if isinstance(retries, int) else 0


def is_terminal_callback_error(error: BaseException) -> bool:
    """Whether a callback API error means the callback no longer accepts results.

    A later attempt fails the same way. So none of these errors is retried.
    """
    return error_code(error) in _TERMINAL_ERROR_CODES


def is_permanent_error(error: BaseException) -> bool:
    """Whether a failed callback call is unlikely to succeed on a later attempt.

    A terminal callback error is permanent. So is any other 4xx answer except
    408, 409, and 429, such as ``AccessDeniedException``. Throttling,
    clock-skew, and expired-credential errors are transient.

    An error without an HTTP status is transient. This covers a timeout, a
    dropped connection, and missing credentials. Missing credentials are
    likely right after a MicroVM starts or resumes, while the credential
    endpoint is not answering yet.

    An ``AccessDeniedException`` can still clear later, for example while an
    IAM change propagates. So a caller that loses a job by giving up, such as
    the heartbeats, keeps trying.
    """
    if is_terminal_callback_error(error):
        return True
    if error_code(error) in _TRANSIENT_4XX_CODES:
        return False
    status = _http_status(error)
    return status is not None and 400 <= status < 500 and status not in {408, 409, 429}


def _is_uncertain_outcome(error: BaseException) -> bool:
    """Whether a failed attempt leaves open whether the service applied it.

    1. A botocore retry inside the attempt means that an earlier try may
       have reached the service, whatever the last try failed with.
    2. Missing credentials stop the call before any request. So the outcome
       is certain.
    3. Otherwise an error without an HTTP status, such as a timeout or a
       dropped connection, or a server error, is uncertain.
    """
    if _sdk_retries(error) > 0:
        return True
    if isinstance(error, NoCredentialsError):
        return False
    status = _http_status(error)
    return status is None or status >= 500


def _create_client(region: str) -> LambdaClient:
    # Each reporter gets its own session. boto3's default session is not safe
    # to use from several threads at once, and the worker runs one thread per
    # job.
    return boto3.session.Session().client(
        "lambda",
        region_name=region,
        config=Config(
            connect_timeout=5,
            read_timeout=COMPLETION_CALL_TIMEOUT_SECONDS,
            retries={"mode": "standard", "max_attempts": 3},
            tcp_keepalive=True,
        ),
    )


class CallbackReporter:
    """Reports heartbeats and the job outcome for one durable callback.

    Create the reporter after the job arrives, not at image build. Lambda
    snapshots the running process when it builds the image. A client created
    then would carry build-time state into every MicroVM.

    Args:
        callback_id: The callback ID from the job document.
        region: The Region of the durable function.
        client: The Lambda client. Defaults to a client for ``region`` that
            uses the default credential chain. Inside a MicroVM, that chain
            resolves the MicroVM's execution role. The reporter closes only a
            client that it created.
        logger: Receives a warning when a completion was probably delivered
            by an earlier attempt whose answer never arrived. Defaults to the
            package's standard library logger.
        sleep: Waits between completion attempts. Tests replace it.
    """

    def __init__(
        self,
        callback_id: str,
        region: str,
        *,
        client: LambdaClient | None = None,
        logger: MicrovmWorkerLogger | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.callback_id = callback_id
        self._owns_client = client is None
        self._client: LambdaClient = (
            _create_client(region) if client is None else client
        )
        self._logger = safe_logger(logger)
        self._sleep = sleep

    def close(self) -> None:
        """Release the connections of a client that the reporter created."""
        if self._owns_client:
            self._client.close()

    def heartbeat(
        self, timeout: float | None = None, cancel: CancelScope | None = None
    ) -> None:
        """Send one heartbeat.

        Args:
            timeout: Ends the call after this many seconds with
                ``TimeoutError``. A stalled call would otherwise hold back
                every later heartbeat.
            cancel: Ends the call at once with :class:`CallCancelledError`
                when the scope is cancelled.

        Raises:
            ClientError: The service error. Use
                :func:`is_terminal_callback_error` to tell a callback that is
                gone from a transient failure.
        """
        self._send(
            lambda: self._client.send_durable_execution_callback_heartbeat(
                CallbackId=self.callback_id
            ),
            timeout,
            "heartbeat",
            cancel,
        )

    def succeed(self, result: Any) -> None:
        """Complete the callback with a result.

        Raises:
            ResultSerializationError: When the result is not JSON-serializable.
                No call is made.
            ResultTooLargeError: When the serialized result exceeds 256 KB.
                No call is made.
            Exception: The last error when every attempt fails. An "already
                complete" answer after an attempt with an unknown outcome
                returns with a warning instead.
        """
        try:
            encoded = _encode_result(result)
        except (TypeError, ValueError, RecursionError) as error:
            raise ResultSerializationError(error) from error
        if len(encoded) > MAX_CALLBACK_RESULT_BYTES:
            raise ResultTooLargeError(len(encoded))
        self._with_retry(
            lambda: self._send(
                lambda: self._client.send_durable_execution_callback_success(
                    CallbackId=self.callback_id, Result=encoded
                ),
                COMPLETION_CALL_TIMEOUT_SECONDS,
                "completion",
            )
        )

    def fail(self, error: object) -> None:
        """Complete the callback with an error.

        The durable function receives the error's class name and message,
        cut to 256 and 8,192 characters. The traceback is not sent, like the
        core SDK, because it would expose the image's file paths in the
        durable execution history.

        Raises:
            Exception: The last error when every attempt fails. An "already
                complete" answer after an attempt with an unknown outcome
                returns with a warning instead.
        """
        # The payload is built once, before the retries. A value whose str()
        # raises must still be reported, not fail every attempt the same way.
        if isinstance(error, BaseException):
            error_type = safe_text(lambda: type(error).__name__, "Error") or "Error"
            message = safe_text(lambda: str(error), "unknown error")
        else:
            error_type = "Error"
            message = safe_text(lambda: str(error), "unknown error")
        payload: ErrorObjectTypeDef = {
            "ErrorType": _truncate(error_type, MAX_ERROR_TYPE_CHARS),
            "ErrorMessage": _truncate(message, MAX_ERROR_MESSAGE_CHARS),
        }
        self._with_retry(
            lambda: self._send(
                lambda: self._client.send_durable_execution_callback_failure(
                    CallbackId=self.callback_id,
                    Error=payload,
                ),
                COMPLETION_CALL_TIMEOUT_SECONDS,
                "completion",
            )
        )

    def _with_retry(self, call: Callable[[], object]) -> None:
        """Retry a completion call up to 5 times.

        A lost completion leaves the durable function waiting until its
        callback timeout. So a transient failure is worth several attempts.
        A permanent error is raised at once.

        One exception: :data:`ALREADY_COMPLETE_CODE` ("already
        complete") after an attempt with an unknown outcome most likely
        means that an earlier try delivered the outcome. The call then warns
        and returns.
        """
        uncertain = False
        attempt = 1
        while True:
            try:
                call()
            except Exception as error:
                if (uncertain or _sdk_retries(error) > 0) and error_code(
                    error
                ) == ALREADY_COMPLETE_CODE:
                    self._logger.warning(
                        "the callback is already complete. An earlier attempt "
                        "whose answer never arrived probably reported the "
                        "outcome.",
                        extra={"callbackId": self.callback_id, "attempt": attempt},
                    )
                    return
                if is_permanent_error(error) or attempt >= COMPLETION_ATTEMPTS:
                    raise
                uncertain = uncertain or _is_uncertain_outcome(error)
                self._sleep(2.0 ** (attempt - 1))
                attempt += 1
            else:
                return

    def _send(
        self,
        call: Callable[[], T],
        timeout: float | None,
        label: str,
        cancel: CancelScope | None = None,
    ) -> T:
        """Run one call, and end it early after ``timeout`` or on ``cancel``.

        botocore has no per-call deadline. Its read timeout limits one socket
        read, and it does not cover credential resolution or its own retries.
        So a bounded call runs in a daemon thread, and this method waits for
        the thread. A call that ends after the bound is ignored. The reporter
        closes its client when the job ends, which releases the call's
        connection.
        """
        if timeout is None and cancel is None:
            return call()
        if cancel is not None and cancel.cancelled:
            msg = f"the {label} call was cancelled"
            raise CallCancelledError(msg)

        # The call's thread settles the future. The waiting thread wakes on
        # `done`, which the future sets when it settles, and which a cancel
        # sets too. A call that settles after the bound settles a future that
        # nobody reads.
        #
        # The future is created here, not by an executor's submit(). Since
        # Python 3.9, the interpreter joins executor threads at exit, so a
        # stalled call would hold up the worker's exit. A daemon thread does
        # not.
        future: Future[T] = Future()
        done = threading.Event()
        future.add_done_callback(lambda _future: done.set())

        def run() -> None:
            try:
                future.set_result(call())
            except BaseException as error:  # noqa: BLE001
                future.set_exception(error)

        if cancel is not None and not cancel._register(done):  # noqa: SLF001
            msg = f"the {label} call was cancelled"
            raise CallCancelledError(msg)
        try:
            threading.Thread(target=run, name=f"callback-{label}", daemon=True).start()
            finished = done.wait(timeout)
        finally:
            if cancel is not None:
                cancel._unregister(done)  # noqa: SLF001
        if future.done():
            return future.result()
        if not finished:
            msg = f"the {label} call took longer than {timeout} seconds"
            raise TimeoutError(msg)
        msg = f"the {label} call was cancelled"
        raise CallCancelledError(msg)


def _encode_result(result: Any) -> bytes:
    """Serialize a result the way ``JSON.stringify`` does, as UTF-8.

    1. The separators have no spaces, and non-ASCII text stays as UTF-8. So
       the size counts the same bytes as the JavaScript worker sends. "日"
       is 3 bytes, where ``\\u65e5`` would be 6.
    2. A string with a lone surrogate cannot be encoded as UTF-8. Then the
       result is serialized again with ``\\uXXXX`` escapes, as
       ``JSON.stringify`` writes a lone surrogate.
    3. ``allow_nan=False`` raises ``ValueError`` for ``nan`` and infinite
       floats, because ``NaN`` and ``Infinity`` are not JSON.
    """
    text = json.dumps(
        result, allow_nan=False, ensure_ascii=False, separators=(",", ":")
    )
    try:
        return text.encode()
    except UnicodeEncodeError:
        return json.dumps(result, allow_nan=False, separators=(",", ":")).encode()


def _truncate(value: str, limit: int) -> str:
    """Cut a string to at most ``limit`` characters, ending in "..."."""
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."
