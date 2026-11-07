# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Delivery of one job to a MicroVM route over HTTP.

This module is the first retry tier of the request step. It runs inside one
step attempt. The step retry strategy is the second tier.
"""

from __future__ import annotations

import functools
import math
import http.client
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

from aws_durable_execution_sdk_python.types import LoggerInterface
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.microvm.aws_errors import (
    add_request_id,
    error_code,
    with_request_id,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_extras.microvm.config import (
        LambdaMicrovmsClient,
    )


DEFAULT_MICROVM_PORT = 8080
"""The MicroVM endpoint forwards to this port when no port header is sent."""

DEFAULT_MICROVM_JOB_PATH = "/durable-execution/v1/job"
"""The path that receives a job over HTTP when the config names no route.

The worker package serves this path with its ``handler``. So the same handler
receives a job whether it arrives in the ``run`` hook or over HTTP. The value
must equal ``MICROVM_JOB_PATH`` in ``aws-durable-execution-sdk-python-microvm-worker``
and ``DEFAULT_MICROVM_JOB_PATH`` in the JavaScript SDK.
"""

DEFAULT_REQUEST_RETRY_WINDOW_SECONDS = 60
"""The default length of the first retry tier."""

MAX_REQUEST_RETRY_WINDOW_SECONDS = 600
"""The longest retry window. A longer window would outlive the auth token
and its one refresh."""

INVOCATION_RESERVE_SECONDS = 10.0
"""The first retry tier stops this long before the invocation times out.

A Lambda timeout during a step attempt records no outcome for the attempt.
So the step keeps time to raise and to checkpoint the second-tier retry.
"""

MIN_CALL_SECONDS = 1.0
"""A call that would start with less than this much time before the reserve
does not start. A shorter call is unlikely to finish, and the step retries
the job in a later invocation anyway."""

_INITIAL_DELAY_SECONDS = 0.25
_MAX_DELAY_SECONDS = 4.0
_REQUEST_TIMEOUT_SECONDS = 10.0
_AUTH_TOKEN_MINUTES = 5
_PORT_HEADER = "X-aws-proxy-port"

_PROXY_ERROR_HEADER = "x-aws-proxy-error"
_CONNECT_FAILED = "MICROVM_CONNECT_FAILED"
"""The endpoint answers HTTP 502 with ``x-aws-proxy-error:
MICROVM_CONNECT_FAILED`` while the MicroVM is suspending or suspended. A
probe measured the answer in about 60 milliseconds."""

_STATE_CHECK_INTERVAL_SECONDS = 2.0
"""The shortest time between two calls of ``SendJobOptions.state_check``."""

_T = TypeVar("_T")


class MicrovmRequestRejectedError(Exception):
    """The endpoint rejected the job with a status that a retry cannot change.

    Examples are 400, 404, and 413. A redirect, 3xx, is rejected too, because
    the operation does not follow redirects. The default step retry strategy
    does not retry it.
    """

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class MicrovmRequestUnauthorizedError(Exception):
    """The endpoint refused the job with 401 or 403 after a new auth token.

    The default step retry strategy retries it, outside the invocation.
    """

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class MicrovmEndpointUnavailableError(Exception):
    """The first retry tier ended without an accepted request.

    The default step retry strategy retries it, outside the invocation.
    """


def reserve_starts_at(
    remaining_seconds: Callable[[], float | None],
) -> float | None:
    """Return the ``time.monotonic()`` time at which the reserve starts.

    Every call of the first tier must end by then. ``None`` means that the
    compute reports no deadline.
    """
    remaining = remaining_seconds()
    if remaining is None:
        return None
    return time.monotonic() + remaining - INVOCATION_RESERVE_SECONDS


def _call_budget(
    hard_deadline: float | None,
    cap: float,
    on_expired: Callable[[], Exception],
) -> float:
    """Return how long the next call may run.

    It is at most ``cap``, and it never runs past ``hard_deadline``.

    Raises:
        Exception: The result of ``on_expired``, when less than
            :data:`MIN_CALL_SECONDS` remains before ``hard_deadline``.
    """
    if hard_deadline is None:
        return cap
    left = hard_deadline - time.monotonic()
    if left < MIN_CALL_SECONDS:
        raise on_expired()
    return min(cap, left)


def call_before_deadline(
    call: Callable[[], _T],
    hard_deadline: float | None,
    on_expired: Callable[[], Exception],
) -> _T:
    """Run one boto3 call that must end before ``hard_deadline``.

    1. Without a deadline, the call runs with the client's own settings.
    2. With a deadline, the call runs in a daemon thread, and this function
       waits for it until the deadline. boto3 cannot cancel a call, and its
       own retries can run longer than the time that remains.
    3. A call that has not ended at the deadline raises the result of
       ``on_expired``. That error is retryable, so the step retries the job
       in a later invocation. The thread keeps running until the call
       returns. Its result is discarded.

    Raises:
        BaseException: The exception that the call raised, or the result of
            ``on_expired``.
    """
    if hard_deadline is None:
        return call()
    budget = _call_budget(hard_deadline, float("inf"), on_expired)
    finished, value = _run_with_limit(call, budget)
    if not finished:
        raise on_expired()
    return cast("_T", value)


def _run_with_limit(call: Callable[[], _T], seconds: float) -> tuple[bool, _T | None]:
    """Run ``call`` in a daemon thread, and wait for it at most ``seconds``.

    Returns:
        ``(True, value)`` when the call returned in time. ``(False, None)``
        when it did not. The thread then keeps running until the call ends,
        and its result is discarded.

    Raises:
        BaseException: The exception that the call raised in time. It is
            re-raised with its own type, a ``BaseException`` included.
    """
    outcome: dict[str, Any] = {}
    done = threading.Event()

    def run() -> None:
        try:
            outcome["value"] = call()
        except BaseException as error:  # noqa: BLE001 - re-raised by the caller below
            outcome["error"] = error
        finally:
            done.set()

    threading.Thread(target=run, name="microvm-call", daemon=True).start()
    if not done.wait(seconds):
        return False, None
    if "error" in outcome:
        raise outcome["error"]
    return True, outcome["value"]


@dataclass(frozen=True, kw_only=True)
class SendJobOptions:
    """The inputs of :func:`send_job`."""

    client: LambdaMicrovmsClient
    microvm_id: str
    endpoint: str
    path: str
    port: int
    body: bytes
    retry_window_seconds: float
    """The first-tier retry window."""
    remaining_seconds: Callable[[], float | None]
    """The remaining invocation time. ``None`` means that the compute reports
    no deadline, and only the retry window applies."""
    logger: LoggerInterface
    opener: urllib.request.OpenerDirector | None = None
    """The urllib opener for the POST. ``None`` uses :func:`default_opener`."""
    sleep: Callable[[float], None] = time.sleep
    state_check: Callable[[], str | None] | None = None
    """Check the MicroVM state once. It raises for a MicroVM that has ended,
    and returns the current endpoint.

    Every job over HTTP sets it, from :func:`microvm` and from a session. A
    suspended, suspending, or terminated MicroVM gets no answer to a
    request, or its endpoint answers HTTP 502 with
    ``x-aws-proxy-error: MICROVM_CONNECT_FAILED``. The answer of a
    terminated MicroVM was measured in us-east-1. So the next attempt calls
    it after either sign, at most once per 2 seconds. A suspended MicroVM
    passes the check, and the next request makes the service resume it. A
    booting MicroVM's endpoint holds the request until the worker listens.
    A boot gives no such sign, unless the worker takes more than 10 seconds
    to start listening. Then the first delivery gets no answer, and the
    operation makes one ``GetMicrovm`` call. So a job whose MicroVM runs
    makes no extra call, except in that case. It runs before the retry
    window and the backoff are checked, so the window does not limit it.
    Only the invocation reserve limits it. ``None`` from it keeps the
    current endpoint.
    """


def require_time_for_call(remaining_seconds: Callable[[], float | None]) -> None:
    """Fail at once when the invocation has too little time for one call.

    A step that starts too close to the end of the invocation cannot make
    one call before the reserve. Without this check, the first tier would
    report that the MicroVM did not accept the job, or did not reach
    ``RUNNING``. The cause is the function timeout. The error stays
    retryable: a step retry runs early in a new invocation, with more time
    left. A function timeout under the needed time fails every attempt, and
    the final error names the fix.

    Raises:
        MicrovmEndpointUnavailableError: When less than 11 seconds remain.
    """
    remaining = remaining_seconds()
    needed = INVOCATION_RESERVE_SECONDS + MIN_CALL_SECONDS
    if remaining is not None and remaining < needed:
        msg = (
            f"The request step had {math.floor(remaining * 10) / 10:.1f} seconds "
            "left in the Lambda "
            f"invocation, and it needs at least {needed:.0f} seconds to send "
            "the job. Raise the function timeout."
        )
        raise MicrovmEndpointUnavailableError(msg)


def send_job(options: SendJobOptions) -> int:
    """Deliver one job to a MicroVM route, with the first retry tier.

    The function handles each failure this way:

    1. A connection error, a timeout, 429, or any 5xx: wait and retry. The
       endpoint answers this way while the MicroVM starts.
    2. 401 or 403: create a new auth token and retry once. A second 401 or
       403 raises :class:`MicrovmRequestUnauthorizedError`, and the step
       retry strategy takes over.
    3. Any other 4xx, or a 3xx: raise :class:`MicrovmRequestRejectedError`
       at once.

    The function does not follow a redirect, and neither does the JavaScript
    operation. A followed 301, 302, or 303 would turn the POST into a GET
    without the job. A followed 307 or 308 to another host would receive the
    auth token. So a redirect fails the delivery at once. The headers are
    added as unredirected headers, so an opener that follows a redirect does
    not send the auth token on either.

    The execution history stores the message. A query in the ``Location``
    can hold a secret, for example a presigned signature. So the message
    names the path, and the status of a 3xx answer. It omits the
    ``Location``, and the URL that a followed redirect reached.

    The delay starts at 250 milliseconds and doubles up to 4 seconds. The
    tier ends at the retry window, or 10 seconds before the invocation times
    out, whichever comes first. It then raises
    :class:`MicrovmEndpointUnavailableError`.

    The two limits differ:

    1. The retry window limits when an attempt may start. An attempt that
       starts inside the window may end after it.
    2. The invocation reserve limits when every call must end. Each request
       and each ``CreateMicrovmAuthToken`` call stops at the reserve. A call
       that would start with less than 1 second before the reserve does not
       start. A Lambda timeout during the attempt would record no outcome,
       and the reserve prevents that.

    Each request runs in a daemon thread, and the function waits for it at
    most 10 seconds, or until the reserve. A socket timeout alone does not
    bound a request: it applies to each socket operation, and each byte
    received starts it again.

    The endpoint can accept a request before Lambda has sent the ``run``
    hook. So the body carries the MicroVM ID too.

    With ``state_check``, a job checks the MicroVM state after a 502 with
    ``x-aws-proxy-error: MICROVM_CONNECT_FAILED`` or a request without an
    answer, at most once per 2 seconds. That check runs first after the
    sign, before the window and backoff rules, and only the invocation
    reserve limits it. When less than 1 second remains before the reserve,
    no check starts, and the tier raises
    :class:`MicrovmEndpointUnavailableError`. A check that finds the
    MicroVM terminating, terminated, or removed raises
    ``MicrovmStateError``, and the job fails with
    ``MicrovmNotRunningError``. Otherwise the function applies the window
    and backoff rules, and sends the job again to the endpoint that the
    check reports.

    For about 5 seconds after a suspend, the endpoint answers that 502, and
    a request does not resume the MicroVM. With the default retry window,
    the retries outlast that window. With a ``retry_window`` under about 9
    seconds, the step retry strategy covers the rest of that window, at the
    cost of one or two more step attempts. A job sent right after a suspend
    can take up to about 10 seconds.

    The same job can reach the route more than once:

    1. A request reaches the route, and its answer is lost.
    2. The function stops waiting for a request, but the request keeps
       running in its thread and reaches the route. The next attempt sends
       the job again.
    3. The invocation ends after the route accepted the job, and before the
       request step checkpointed its success. The step then runs again in
       the next invocation.

    The worker package ignores a second request for a callback ID that runs
    or recently ended. A route served without the worker package must ignore
    it too.

    Returns:
        The status that accepted the job.

    Raises:
        MicrovmRequestRejectedError: When the endpoint rejects the job.
        MicrovmRequestUnauthorizedError: When a new auth token is refused.
        MicrovmEndpointUnavailableError: When the tier ends.
        ClientError: When ``CreateMicrovmAuthToken`` answers
            ``ResourceNotFoundException``. It is the error that the client
            raised, unchanged. The request step turns it into
            ``MicrovmStateError``.
        MicrovmStateError: When ``state_check`` reports that the MicroVM
            can no longer run a job.
        MicrovmPermissionDeniedError: When ``state_check`` is denied
            ``GetMicrovm``.
        Exception: When ``CreateMicrovmAuthToken`` fails otherwise. The
            message of a botocore error ends with the request ID.
    """
    require_time_for_call(options.remaining_seconds)
    started = time.monotonic()
    window_ends = started + options.retry_window_seconds
    attempt = 0
    last_failure = "no attempt"

    def unavailable() -> Exception:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        return MicrovmEndpointUnavailableError(
            f"The MicroVM did not accept the job at {options.path} after "
            f"{attempt} attempts in {elapsed_ms} ms. Last failure: {last_failure}."
        )

    def deadline() -> float:
        reserve = reserve_starts_at(options.remaining_seconds)
        return window_ends if reserve is None else min(window_ends, reserve)

    def new_token() -> Mapping[str, str]:
        return call_before_deadline(
            lambda: _create_auth_token(options),
            reserve_starts_at(options.remaining_seconds),
            unavailable,
        )

    endpoint = options.endpoint
    url = _endpoint_url(endpoint, options.path)
    token = new_token()
    refreshed_token = False
    delay = _INITIAL_DELAY_SECONDS
    last_state_check: float | None = None

    while True:
        attempt += 1
        # The request must end before the reserve, like every call in the
        # tier.
        timeout = _call_budget(
            reserve_starts_at(options.remaining_seconds),
            _REQUEST_TIMEOUT_SECONDS,
            unavailable,
        )
        headers = {"content-type": "application/json", **token}
        if options.port != DEFAULT_MICROVM_PORT:
            headers[_PORT_HEADER] = str(options.port)
        answer = _post_within(
            url, options.body, headers, timeout, options.opener or default_opener()
        )
        status = answer.status
        if answer.followed_redirect:
            # A caller's opener followed a redirect. The route never received
            # the job: a 301, 302, or 303 turns the POST into a GET.
            msg = (
                f"The MicroVM answered the job at {options.path} with a "
                "redirect, and the opener followed it. The operation does not "
                "follow redirects, so set the path that the route serves, and "
                "pass an opener that follows no redirect."
            )
            raise MicrovmRequestRejectedError(msg, status or 0)

        if status is not None and 200 <= status < 300:
            options.logger.info(
                "MicroVM job accepted", extra={"attempt": attempt, "status": status}
            )
            return status
        if status is None:
            last_failure = answer.failure
        else:
            last_failure = f"HTTP {status}"
            if status in {401, 403}:
                if refreshed_token:
                    msg = (
                        f"The MicroVM refused a new auth token at {options.path} "
                        f"with HTTP {status}"
                    )
                    raise MicrovmRequestUnauthorizedError(msg, status)
                refreshed_token = True
                # The refresh checks the reserve like every other call. The
                # next request then checks it again.
                token = new_token()
                options.logger.info(
                    "MicroVM auth token refreshed",
                    extra={"attempt": attempt, "status": status},
                )
                continue
            if 300 <= status < 400:  # noqa: PLR2004
                msg = (
                    f"The MicroVM answered the job at {options.path} with HTTP {status}"
                )
                if answer.has_location:
                    msg += (
                        ", a redirect. The operation does not follow redirects, "
                        "so set the path that the route serves."
                    )
                raise MicrovmRequestRejectedError(msg, status)
            if 400 <= status < 500 and status != 429:  # noqa: PLR2004
                msg = (
                    f"The MicroVM rejected the job at {options.path} with HTTP {status}"
                )
                raise MicrovmRequestRejectedError(msg, status)

        # A job checks the state only after a sign that the MicroVM does not
        # run: it is suspended, or it ended. The check raises for one that
        # ended. A suspended MicroVM passes it, and the service resumes it
        # on a later request. A booting MicroVM gives no such sign.
        # The check runs first, before the window and backoff rules, and
        # only the invocation reserve limits it. A delayed sign can use up
        # the whole window, and the check must still find an ended MicroVM.
        # The check repeats after each such failure, at most once per 2
        # seconds. The MicroVM can end after the job's first state check.
        if options.state_check is not None and (
            status is None or answer.connect_failed
        ):
            now = time.monotonic()
            if (
                last_state_check is None
                or now - last_state_check >= _STATE_CHECK_INTERVAL_SECONDS
            ):
                last_state_check = now
                # With less than 1 second before the reserve, no check
                # starts, and the tier ends.
                _call_budget(
                    reserve_starts_at(options.remaining_seconds), math.inf, unavailable
                )
                endpoint = options.state_check() or endpoint
                url = _endpoint_url(endpoint, options.path)
        if time.monotonic() + delay > deadline():
            raise unavailable()
        options.logger.info(
            "MicroVM job not accepted yet",
            extra={"attempt": attempt, "failure": last_failure},
        )
        options.sleep(delay)
        delay = min(delay * 2, _MAX_DELAY_SECONDS)


def _create_auth_token(options: SendJobOptions) -> Mapping[str, str]:
    """Call ``CreateMicrovmAuthToken``, and return the token headers.

    The function handles a failed call this way:

    1. The API model lists ``ResourceNotFoundException`` for this call. It
       means that the MicroVM was removed after the launch or after the
       state check. The function raises that error unchanged. The request
       step in ``operation.py`` turns it into ``MicrovmStateError``, with
       the error as its ``__cause__``.
    2. Any other error is raised with its class and its ``response``, and
       its message ends with the request ID. See :func:`add_request_id`.

    Raises:
        RuntimeError: When the answer has no ``authToken``.
        ClientError: When the call fails. A ``ResourceNotFoundException``
            is the error that the client raised. Any other error can be the
            copy that :func:`add_request_id` returns.
    """
    try:
        response = options.client.create_microvm_auth_token(
            microvmIdentifier=options.microvm_id,
            expirationInMinutes=_AUTH_TOKEN_MINUTES,
            allowedPorts=[{"port": options.port}],
        )
    except ClientError as error:
        if error_code(error) == "ResourceNotFoundException":
            raise
        # add_request_id returns the error, or a copy whose __cause__ is
        # the error. So the raise has no `from`.
        raise add_request_id("CreateMicrovmAuthToken", error)
    token = response.get("authToken")
    if not token:
        msg = with_request_id(
            "CreateMicrovmAuthToken returned no authToken",
            "CreateMicrovmAuthToken",
            response,
        )
        raise RuntimeError(msg)
    return token


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Do not follow a redirect. urllib then raises ``HTTPError`` with the
    3xx status."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


@functools.cache
def default_opener() -> urllib.request.OpenerDirector:
    """The urllib opener that the POST uses when the config sets none.

    1. It ignores the proxy settings in the environment. The request goes to
       the MicroVM endpoint directly, as the JavaScript SDK's ``fetch`` sends
       it.
    2. It follows no redirect.

    It is built on first use. Building it loads the CA certificates, so a
    function whose jobs all go in the ``run`` hook does not pay for it.
    """
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


@dataclass(frozen=True)
class _Answer:
    """The outcome of one POST."""

    status: int | None
    """The HTTP status. ``None`` when no answer arrived."""
    failure: str = ""
    """Why no answer arrived."""
    has_location: bool = False
    """Whether the answer has a ``Location`` header."""
    followed_redirect: bool = False
    """Whether the opener followed a redirect."""
    connect_failed: bool = False
    """Whether the endpoint answered 502 with ``x-aws-proxy-error:
    MICROVM_CONNECT_FAILED``. It answers so for a suspending or suspended
    MicroVM."""


def _post_within(
    url: str,
    body: bytes,
    headers: Mapping[str, str],
    timeout: float,
    opener: urllib.request.OpenerDirector,
) -> _Answer:
    """POST ``body``, and wait for the answer at most ``timeout`` seconds.

    The POST runs in a daemon thread, because a socket timeout alone does not
    bound the whole request. A POST without an answer in time counts as one
    failed attempt, like a refused connection.
    """
    finished, answer = _run_with_limit(
        lambda: _post(url, body, headers, timeout, opener), timeout
    )
    if not finished:
        return _Answer(None, f"TimeoutError: no answer in {timeout:.1f} s")
    return cast("_Answer", answer)


def _post(
    url: str,
    body: bytes,
    headers: Mapping[str, str],
    timeout: float,
    opener: urllib.request.OpenerDirector,
) -> _Answer:
    """POST ``body``, and return the status, or the failure.

    The body of the response is not used. So the function reads no body, and
    closes the response. ``timeout`` is the socket timeout. It applies to
    each socket operation, so :func:`_post_within` bounds the whole request.
    """
    request = urllib.request.Request(  # noqa: S310 - the URL is https or the endpoint's own
        url, data=body, method="POST"
    )
    # urllib does not copy these headers to a redirected request. So an
    # opener that follows a redirect does not send the auth token on.
    for name, value in headers.items():
        request.add_unredirected_header(name, value)
    try:
        with opener.open(request, timeout=timeout) as response:
            # An opener that follows redirects reports the URL it reached.
            if response.geturl() != url:
                return _Answer(response.status, followed_redirect=True)
            return _Answer(
                response.status,
            )
    except urllib.error.HTTPError as error:
        has_location = bool(error.headers and error.headers.get("Location"))
        connect_failed = (
            error.code == 502  # noqa: PLR2004
            and bool(error.headers)
            and error.headers.get(_PROXY_ERROR_HEADER) == _CONNECT_FAILED
        )
        error.close()
        return _Answer(
            error.code, has_location=has_location, connect_failed=connect_failed
        )
    except (OSError, http.client.HTTPException, ValueError) as error:
        # URLError, a refused connection, and a timeout are OSError. An
        # invalid response from the endpoint raises an HTTPException.
        return _Answer(None, f"{type(error).__name__}: {error}")


def _endpoint_url(endpoint: str, path: str) -> str:
    """Join the endpoint and the path, as the caller wrote the path.

    ``RunMicrovm`` returns the endpoint as a host name without a scheme.
    """
    base = (
        endpoint
        if endpoint.startswith(("https://", "http://"))
        else (f"https://{endpoint}")
    )
    return f"{base.rstrip('/')}{path}"
