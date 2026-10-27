# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the first retry tier of the HTTP delivery."""

from __future__ import annotations

import logging
import socket
import threading
import time
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest
from aws_durable_execution_sdk_python_microvm_worker import MICROVM_JOB_PATH
from aws_durable_execution_sdk_python_microvm_worker import worker

from aws_durable_execution_sdk_python_extras.microvm import (
    DEFAULT_MICROVM_JOB_PATH,
    MicrovmEndpointUnavailableError,
    MicrovmRequestRejectedError,
    MicrovmRequestUnauthorizedError,
    request,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    SendJobOptions,
    call_before_deadline,
    send_job,
)


BODY = b'{"version":1}'


def options(
    client: Any, endpoint: str, sleeps: list[float], **fields: Any
) -> SendJobOptions:
    values: dict[str, Any] = {
        "client": client,
        "microvm_id": "mvm-1",
        "endpoint": endpoint,
        "path": "/durable-execution/v1/job",
        "port": 8080,
        "body": BODY,
        "retry_window_seconds": 60,
        "remaining_seconds": lambda: None,
        "logger": logging.getLogger("test"),
        # The tests record the delays instead of waiting.
        "sleep": sleeps.append,
        **fields,
    }
    return SendJobOptions(**values)


def test_matches_the_job_path_of_the_worker() -> None:
    assert DEFAULT_MICROVM_JOB_PATH == MICROVM_JOB_PATH


def test_matches_the_suspend_after_header_of_the_worker() -> None:
    assert request.SUSPEND_AFTER_HEADER == worker.SUSPEND_AFTER_HEADER
    assert request.SUSPEND_AFTER_ACCEPTED == "accepted"


def test_posts_the_job_with_an_auth_token(fake_client: Any, fake_endpoint: Any) -> None:
    client = fake_client()
    endpoint = fake_endpoint()
    sleeps: list[float] = []

    status = send_job(options(client, endpoint.url, sleeps))

    assert status == 202
    [token_request] = client.requests("create_microvm_auth_token")
    assert token_request == {
        "microvmIdentifier": "mvm-1",
        "expirationInMinutes": 5,
        "allowedPorts": [{"port": 8080}],
    }
    [posted] = endpoint.requests
    assert posted["path"] == "/durable-execution/v1/job"
    assert posted["headers"]["content-type"] == "application/json"
    assert posted["headers"]["x-test-token"] == "token-1"
    # The endpoint forwards to 8080 when no port header is sent.
    assert "x-aws-proxy-port" not in posted["headers"]
    assert posted["body"] == {"version": 1}
    assert sleeps == []


def test_sends_a_port_header_for_another_port(
    fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client()
    endpoint = fake_endpoint()

    send_job(options(client, endpoint.url, [], port=3000))

    assert client.requests("create_microvm_auth_token")[0]["allowedPorts"] == [
        {"port": 3000}
    ]
    assert endpoint.requests[0]["headers"]["x-aws-proxy-port"] == "3000"


def test_retries_while_the_microvm_starts(fake_client: Any, fake_endpoint: Any) -> None:
    # None closes the connection without an answer.
    endpoint = fake_endpoint([503, 429, 500, None])
    sleeps: list[float] = []

    status = send_job(options(fake_client(), endpoint.url, sleeps))

    assert status == 202
    assert len(endpoint.requests) == 5
    assert sleeps == [0.25, 0.5, 1.0, 2.0]


def test_caps_the_delay_at_4_seconds(fake_client: Any, fake_endpoint: Any) -> None:
    endpoint = fake_endpoint([503] * 7)
    sleeps: list[float] = []

    send_job(options(fake_client(), endpoint.url, sleeps))

    assert sleeps == [0.25, 0.5, 1.0, 2.0, 4.0, 4.0, 4.0]


@pytest.mark.parametrize("status", [400, 404, 413])
def test_rejects_the_job_at_once_for_another_4xx(
    fake_client: Any, fake_endpoint: Any, status: int
) -> None:
    endpoint = fake_endpoint([status])

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert raised.value.status == status
    assert str(raised.value) == (
        f"The MicroVM rejected the job at /durable-execution/v1/job with HTTP {status}"
    )
    assert len(endpoint.requests) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_retries_once_with_a_new_auth_token(
    fake_client: Any, fake_endpoint: Any, status: int
) -> None:
    client = fake_client()
    endpoint = fake_endpoint([status])
    sleeps: list[float] = []

    assert send_job(options(client, endpoint.url, sleeps)) == 202

    assert len(client.requests("create_microvm_auth_token")) == 2
    assert [r["headers"]["x-test-token"] for r in endpoint.requests] == [
        "token-1",
        "token-2",
    ]
    # The refresh retries at once.
    assert sleeps == []


def test_raises_unauthorized_when_the_new_token_is_refused(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([401, 403])

    with pytest.raises(MicrovmRequestUnauthorizedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert raised.value.status == 403
    assert len(endpoint.requests) == 2


def test_ends_the_tier_at_the_retry_window(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([503] * 10)
    sleeps: list[float] = []

    # The first delay fits in the window, and the second does not.
    with pytest.raises(MicrovmEndpointUnavailableError) as raised:
        send_job(options(fake_client(), endpoint.url, sleeps, retry_window_seconds=0.4))

    assert sleeps == [0.25]
    message = str(raised.value)
    assert message.startswith(
        "The MicroVM did not accept the job at /durable-execution/v1/job after "
        "2 attempts in "
    )
    assert message.endswith("Last failure: HTTP 503.")


def test_reports_a_connection_failure_as_the_last_failure(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([None, None])

    with pytest.raises(MicrovmEndpointUnavailableError) as raised:
        send_job(options(fake_client(), endpoint.url, [], retry_window_seconds=0.4))

    assert "Last failure: HTTP 503" not in str(raised.value)
    assert "Last failure: " in str(raised.value)


def test_ends_the_tier_before_the_invocation_reserve(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([503] * 10)
    sleeps: list[float] = []

    # The reserve starts 3 seconds from now. A 4-second delay crosses it.
    with pytest.raises(MicrovmEndpointUnavailableError):
        send_job(
            options(fake_client(), endpoint.url, sleeps, remaining_seconds=lambda: 13.0)
        )

    assert sleeps == [0.25, 0.5, 1.0, 2.0]


@pytest.mark.parametrize(
    ("remaining", "shown"),
    [(0.5, "0.5"), (2.9, "2.9"), (10.9, "10.9"), (10.96, "10.9")],
)
def test_names_the_function_timeout_when_too_little_time_is_left(
    fake_client: Any, fake_endpoint: Any, remaining: float, shown: str
) -> None:
    # The Lambda default timeout is 3 seconds. Such a function can never
    # send a job over HTTP.
    client = fake_client()
    endpoint = fake_endpoint()

    with pytest.raises(MicrovmEndpointUnavailableError) as raised:
        send_job(options(client, endpoint.url, [], remaining_seconds=lambda: remaining))

    assert str(raised.value) == (
        f"The request step had {shown} seconds left in the Lambda "
        "invocation, and it needs at least 11 seconds to send the job. Raise "
        "the function timeout."
    )
    assert client.calls == []
    assert endpoint.requests == []


def test_starts_no_call_within_1_second_of_the_reserve(
    fake_client: Any, fake_endpoint: Any
) -> None:
    # The time left falls below the minimum after the first check.
    client = fake_client()
    endpoint = fake_endpoint()
    readings = iter([12.0])

    with pytest.raises(MicrovmEndpointUnavailableError, match="after 0 attempts"):
        send_job(
            options(
                client,
                endpoint.url,
                [],
                remaining_seconds=lambda: next(readings, 10.5),
            )
        )

    assert client.calls == []
    assert endpoint.requests == []


def test_bounds_the_request_timeout_by_the_reserve(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeouts: list[float] = []

    def post(url: str, body: bytes, headers: Any, timeout: float, opener: Any) -> Any:
        timeouts.append(timeout)
        return request._Answer(202)  # noqa: SLF001

    monkeypatch.setattr(request, "_post", post)

    send_job(
        options(fake_client(), "mvm-1.example", [], remaining_seconds=lambda: 15.0)
    )
    send_job(options(fake_client(), "mvm-1.example", []))

    # The reserve starts 5 seconds from now. Without a deadline, the cap is
    # 10 seconds.
    assert 4.5 < timeouts[0] <= 5.0
    assert timeouts[1] == 10.0


@pytest.mark.parametrize(
    ("endpoint", "url"),
    [
        ("mvm-1.example", "https://mvm-1.example/job"),
        ("mvm-1.example/", "https://mvm-1.example/job"),
        ("http://127.0.0.1:1", "http://127.0.0.1:1/job"),
        ("https://mvm-1.example", "https://mvm-1.example/job"),
    ],
)
def test_adds_https_to_an_endpoint_without_a_scheme(endpoint: str, url: str) -> None:
    assert request._endpoint_url(endpoint, "/job") == url  # noqa: SLF001


def test_fails_when_the_service_returns_no_auth_token(fake_client: Any) -> None:
    class NoToken(fake_client):  # type: ignore[misc, valid-type]
        def create_microvm_auth_token(self, **request: Any) -> dict[str, Any]:
            return {}

    with pytest.raises(RuntimeError, match="returned no authToken"):
        send_job(options(NoToken(), "mvm-1.example", []))


def test_passes_on_a_failure_of_the_auth_token_call(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(tokens=[make_error("AccessDeniedException", 403)])

    with pytest.raises(Exception, match="AccessDeniedException"):
        send_job(options(client, "mvm-1.example", [], remaining_seconds=lambda: 60.0))


def test_stops_a_stalled_call_at_the_deadline() -> None:
    release = threading.Event()
    started = time.monotonic()

    with pytest.raises(TimeoutError, match="expired"):
        call_before_deadline(
            lambda: release.wait(30),
            time.monotonic() + 1.2,
            lambda: TimeoutError("expired"),
        )

    assert time.monotonic() - started < 5
    release.set()


def test_returns_the_result_of_a_call_before_the_deadline() -> None:
    assert call_before_deadline(lambda: 7, time.monotonic() + 30, RuntimeError) == 7
    assert call_before_deadline(lambda: 8, None, RuntimeError) == 8


@pytest.fixture
def trickle_server() -> Iterator[str]:
    """A server that answers one byte every 0.3 seconds.

    Each byte starts the socket timeout again. So only a bound on the whole
    request stops the POST in time.
    """
    listener = socket.create_server(("127.0.0.1", 0))
    stop = threading.Event()

    def serve() -> None:
        connection, _ = listener.accept()
        with connection:
            connection.recv(65536)
            for byte in b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n":
                if stop.wait(0.3):
                    return
                try:
                    connection.sendall(bytes([byte]))
                except OSError:
                    return

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    stop.set()
    listener.close()


def test_bounds_the_whole_request_and_not_each_socket_operation(
    trickle_server: str,
) -> None:
    started = time.monotonic()

    answer = request._post_within(  # noqa: SLF001
        f"{trickle_server}/job", BODY, {}, 1.0, request.default_opener()
    )

    assert time.monotonic() - started < 2
    assert answer.status is None
    assert answer.failure == "TimeoutError: no answer in 1.0 s"


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_rejects_a_redirect_without_following_it(
    fake_client: Any, fake_endpoint: Any, status: int
) -> None:
    endpoint = fake_endpoint([status])

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert raised.value.status == status
    assert str(raised.value) == (
        f"The MicroVM answered the job at /durable-execution/v1/job with HTTP "
        f"{status}, a redirect. The operation does not follow redirects, so set "
        "the path that the route serves."
    )
    # A followed 301, 302, or 303 would arrive as a GET without the job.
    assert [r["path"] for r in endpoint.requests] == ["/durable-execution/v1/job"]


@pytest.mark.parametrize("status", [302, 304])
def test_rejects_a_3xx_without_a_location(
    fake_client: Any, fake_endpoint: Any, status: int
) -> None:
    endpoint = fake_endpoint([status], location=None)

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert raised.value.status == status
    assert str(raised.value) == (
        f"The MicroVM answered the job at /durable-execution/v1/job with HTTP {status}"
    )


def test_rejects_a_3xx_with_an_empty_location(
    fake_client: Any, fake_endpoint: Any
) -> None:
    # An empty Location names no target. So it counts as no Location.
    endpoint = fake_endpoint([302], location="")

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert raised.value.status == 302
    assert str(raised.value) == (
        "The MicroVM answered the job at /durable-execution/v1/job with HTTP 302"
    )


def test_omits_the_location_of_a_redirect_from_the_message(
    fake_client: Any, fake_endpoint: Any
) -> None:
    # The execution history stores the message. A query in the Location can
    # hold a secret. So the message must not contain any part of it.
    endpoint = fake_endpoint([302], location="https://example.com/x?token=secret")

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(options(fake_client(), endpoint.url, []))

    assert "example.com" not in str(raised.value)
    assert "secret" not in str(raised.value)


@pytest.mark.parametrize(
    "path", ["/clone%20build", "/caf%C3%A9", "/a/b-c_d.e~f!$&'()*+,;=:@", "/a/"]
)
def test_sends_the_path_as_written(
    fake_client: Any, fake_endpoint: Any, path: str
) -> None:
    # The workers match the raw request path. So the config and the route
    # use the same string only when the path is sent unchanged.
    endpoint = fake_endpoint()

    send_job(options(fake_client(), endpoint.url, [], path=path))

    assert endpoint.requests[0]["path"] == path


def test_uses_the_opener_that_the_caller_passes(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint()
    opened: list[str] = []

    class Recording(urllib.request.BaseHandler):
        def http_request(self, req: urllib.request.Request) -> urllib.request.Request:
            opened.append(req.full_url)
            return req

    opener = urllib.request.build_opener(Recording())

    assert send_job(options(fake_client(), endpoint.url, [], opener=opener)) == 202
    assert opened == [f"{endpoint.url}/durable-execution/v1/job"]


@pytest.mark.parametrize("status", [301, 302, 303])
def test_rejects_a_redirect_that_the_callers_opener_followed(
    fake_client: Any, fake_endpoint: Any, status: int
) -> None:
    # The default urllib opener follows the redirect with a GET, and the GET
    # gets 200. The route never received the job.
    endpoint = fake_endpoint([status])

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(
            options(
                fake_client(), endpoint.url, [], opener=urllib.request.build_opener()
            )
        )

    assert str(raised.value) == (
        "The MicroVM answered the job at /durable-execution/v1/job with a "
        "redirect, and the opener followed it. The operation does not follow "
        "redirects, so set the path that the route serves, and pass an opener "
        "that follows no redirect."
    )
    assert [r["path"] for r in endpoint.requests] == [
        "/durable-execution/v1/job",
        "/moved",
    ]


def test_omits_the_followed_url_from_the_message(
    fake_client: Any, fake_endpoint: Any
) -> None:
    # The opener follows the 302 to the URL with the query. The message must
    # not contain that URL.
    endpoint = fake_endpoint([302], location="/x?token=secret")

    with pytest.raises(MicrovmRequestRejectedError) as raised:
        send_job(
            options(
                fake_client(), endpoint.url, [], opener=urllib.request.build_opener()
            )
        )

    assert endpoint.requests[-1]["path"] == "/x?token=secret"
    assert "?token=" not in str(raised.value)
    assert "secret" not in str(raised.value)


def test_builds_the_default_opener_once() -> None:
    assert request.default_opener() is request.default_opener()


def test_ignores_proxy_settings_in_the_environment(
    fake_client: Any, fake_endpoint: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Port 1 refuses every connection. A request that used the proxy would
    # fail every attempt.
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(name, "")
    endpoint = fake_endpoint()

    assert send_job(options(fake_client(), endpoint.url, [])) == 202
    assert len(endpoint.requests) == 1


def test_passes_on_a_base_exception_of_the_call() -> None:
    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        call_before_deadline(interrupted, time.monotonic() + 30, RuntimeError)


def test_checks_the_state_again_after_a_gateway_error_or_no_answer(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([502, None, 503, 504, 429, 500])
    sleeps: list[float] = []
    windows: list[float] = []

    def recheck(left: float) -> str | None:
        windows.append(left)
        return None

    status = send_job(options(fake_client(), endpoint.url, sleeps, recheck=recheck))

    assert status == 202
    # 502, no answer, 503, and 504 can mean a suspend. 429 and 500 cannot.
    assert len(windows) == 4
    assert all(0 < left <= 60 for left in windows)


def test_sends_the_next_request_to_the_endpoint_that_the_recheck_returns(
    fake_client: Any, fake_endpoint: Any
) -> None:
    before = fake_endpoint([502])
    after = fake_endpoint()

    status = send_job(
        options(fake_client(), before.url, [], recheck=lambda left: after.url)
    )

    assert status == 202
    assert len(before.requests) == 1
    assert len(after.requests) == 1


def test_ends_the_tier_when_the_recheck_has_no_time_left(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([502])
    rechecks: list[float] = []

    def sleep(seconds: float) -> None:
        # The delay uses the rest of the window.
        time.sleep(0.9)

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last failure: HTTP 502"):
        send_job(
            options(
                fake_client(),
                endpoint.url,
                [],
                retry_window_seconds=1,
                window_started_at=time.monotonic() - 0.2,
                sleep=sleep,
                recheck=rechecks.append,
            )
        )

    assert rechecks == []


def test_counts_the_window_from_the_given_start(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([503])

    # The state check before delivery used 59.9 of the 60 seconds. So the
    # first delay of 0.25 seconds does not fit.
    with pytest.raises(MicrovmEndpointUnavailableError, match="after 1 attempts"):
        send_job(
            options(
                fake_client(),
                endpoint.url,
                [],
                window_started_at=time.monotonic() - 59.9,
            )
        )


def test_sends_no_auth_token_on_a_redirect_that_the_callers_opener_follows(
    fake_client: Any, fake_endpoint: Any
) -> None:
    # The opener follows a 302 to another host. That host must not receive
    # the token, because urllib does not copy an unredirected header.
    other_host = fake_endpoint()
    endpoint = fake_endpoint([302], location=f"{other_host.url}/moved")

    with pytest.raises(MicrovmRequestRejectedError):
        send_job(
            options(
                fake_client(), endpoint.url, [], opener=urllib.request.build_opener()
            )
        )

    assert endpoint.requests[0]["headers"]["x-test-token"] == "token-1"
    [followed] = other_host.requests
    assert followed["method"] == "GET"
    assert "x-test-token" not in followed["headers"]
    assert "content-type" not in followed["headers"]


# ---------------------------------------------------------------------------
# The check for a suspended MicroVM, and the suspendAfter acknowledgement
# ---------------------------------------------------------------------------


class FakeClock:
    """A monotonic clock that advances only when the tier sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(request, "time", fake)
    return fake


def test_checks_for_a_suspend_after_a_connect_failure_or_no_answer(
    fake_client: Any, fake_endpoint: Any, connect_failed: Any, clock: FakeClock
) -> None:
    # 1. A suspended MicroVM's endpoint answers 502 with MICROVM_CONNECT_FAILED.
    # 2. A request that the freeze caught gets no answer.
    # 3. The clock passes 2 seconds between the two failures.
    endpoint = fake_endpoint([connect_failed, 503, 503, 503, None])
    windows: list[float] = []

    def check(left: float) -> str | None:
        windows.append(left)
        return None

    status = send_job(
        options(
            fake_client(),
            endpoint.url,
            [],
            sleep=clock.sleep,
            suspend_check=check,
        )
    )

    assert status == 202
    assert clock.sleeps == [0.25, 0.5, 1.0, 2.0, 4.0]
    assert windows == [60 - 0.25, 60 - 7.75]


@pytest.mark.parametrize(
    "answer",
    [502, 503, 504, (502, {"x-aws-proxy-error": "OTHER"}), 429, 500],
    ids=["502", "503", "504", "502-other-proxy-error", "429", "500"],
)
def test_makes_no_suspend_check_for_another_failure(
    fake_client: Any, fake_endpoint: Any, answer: Any
) -> None:
    # A booting MicroVM never gets MICROVM_CONNECT_FAILED. So these answers
    # make no extra call.
    endpoint = fake_endpoint([answer])
    checks: list[float] = []

    status = send_job(
        options(fake_client(), endpoint.url, [], suspend_check=checks.append)
    )

    assert status == 202
    assert checks == []


def test_checks_for_a_suspend_at_most_once_per_2_seconds(
    fake_client: Any, fake_endpoint: Any, connect_failed: Any, clock: FakeClock
) -> None:
    endpoint = fake_endpoint([connect_failed] * 5)
    checked_at: list[float] = []

    def check(left: float) -> str | None:
        checked_at.append(clock.now - 1000)
        return None

    send_job(
        options(fake_client(), endpoint.url, [], sleep=clock.sleep, suspend_check=check)
    )

    # The failures end at 0.25, 0.75, 1.75, 3.75, and 7.75 seconds. A check
    # that would come less than 2 seconds after the last one is skipped.
    assert checked_at == [0.25, 3.75, 7.75]
    assert len(endpoint.requests) == 6


def test_sends_the_next_request_to_the_endpoint_that_the_suspend_check_returns(
    fake_client: Any, fake_endpoint: Any, connect_failed: Any
) -> None:
    before = fake_endpoint([connect_failed])
    after = fake_endpoint()

    status = send_job(
        options(fake_client(), before.url, [], suspend_check=lambda left: after.url)
    )

    assert status == 202
    assert len(before.requests) == 1
    assert len(after.requests) == 1


def test_ends_the_tier_when_the_suspend_check_has_no_time_left(
    fake_client: Any, fake_endpoint: Any, connect_failed: Any
) -> None:
    endpoint = fake_endpoint([connect_failed])
    checks: list[float] = []

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last failure: HTTP 502"):
        send_job(
            options(
                fake_client(),
                endpoint.url,
                [],
                retry_window_seconds=1,
                window_started_at=time.monotonic() - 0.2,
                sleep=lambda seconds: time.sleep(0.9),
                suspend_check=checks.append,
            )
        )

    assert checks == []


def test_adds_the_note_to_the_final_error(
    fake_client: Any, fake_endpoint: Any, connect_failed: Any
) -> None:
    endpoint = fake_endpoint([connect_failed] * 10)

    with pytest.raises(MicrovmEndpointUnavailableError) as raised:
        send_job(
            options(
                fake_client(),
                endpoint.url,
                [],
                retry_window_seconds=1,
                unavailable_note=lambda: "GetMicrovm was denied.",
            )
        )

    assert str(raised.value).endswith("Last failure: HTTP 502. GetMicrovm was denied.")


def test_adds_no_note_when_the_note_is_none(
    fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([503] * 10)

    with pytest.raises(MicrovmEndpointUnavailableError) as raised:
        send_job(
            options(
                fake_client(),
                endpoint.url,
                [],
                retry_window_seconds=1,
                unavailable_note=lambda: None,
            )
        )

    assert str(raised.value).endswith("Last failure: HTTP 503.")


@pytest.mark.parametrize(
    ("headers", "ignored"),
    [
        ({"X-Microvm-Suspend-After": "accepted"}, 0),
        ({}, 1),
        ({"X-Microvm-Suspend-After": "yes"}, 1),
    ],
    ids=["acknowledged", "no-header", "other-value"],
)
def test_reports_a_suspend_after_job_that_the_worker_did_not_acknowledge(
    fake_client: Any, fake_endpoint: Any, headers: dict[str, str], ignored: int
) -> None:
    endpoint = fake_endpoint(accept_headers=headers)
    calls: list[None] = []

    status = send_job(
        options(
            fake_client(),
            endpoint.url,
            [],
            on_suspend_after_ignored=lambda: calls.append(None),
        )
    )

    assert status == 202
    assert len(calls) == ignored


def test_reads_the_acknowledgement_of_the_answer_that_accepted_the_job(
    fake_client: Any, fake_endpoint: Any, suspend_after_ack: dict[str, str]
) -> None:
    # A refused answer that carries the header does not count.
    endpoint = fake_endpoint([(503, suspend_after_ack)])
    calls: list[None] = []

    send_job(
        options(
            fake_client(),
            endpoint.url,
            [],
            on_suspend_after_ignored=lambda: calls.append(None),
        )
    )

    assert calls == [None]
