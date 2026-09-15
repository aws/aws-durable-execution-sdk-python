# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared serialization helpers for the Workflow Insight exporters.

Kept private to the ``exporters`` package: every backend needs the same
JS-compatible compact JSON encoding and the same key/file-name sanitizer, so
they live here rather than being duplicated per exporter module.
"""

from __future__ import annotations

import datetime
import http.client
import json
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlsplit


_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# Upper bound on how much of a non-2xx response body is read for diagnostics.
_MAX_ERROR_BODY_BYTES = 64 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect so a 3xx surfaces as a failed status.

    Following a redirect would re-send a POST as a body-less GET and forward
    configured credential headers to the new location.
    """

    def redirect_request(  # type: ignore[override]  # stdlib signature has no hints
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


class _Deadline:
    """A whole-request time budget shared by the timer and the connection.

    ``expired()`` is judged by the monotonic clock, so a response that lands
    after the budget is never accepted even if the timer callback runs late.
    The timer's job is only to wake a blocked socket.
    """

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.expires_at = time.monotonic() + seconds
        self._fired = threading.Event()
        self._lock = threading.Lock()
        self._sockets: list[socket.socket] = []

    def expired(self) -> bool:
        return self._fired.is_set() or time.monotonic() >= self.expires_at

    def remaining(self) -> float:
        # Never hand the socket layer zero or a negative value: those mean
        # non-blocking / blocking, not "no time left".
        return max(self.expires_at - time.monotonic(), 0.001)

    def register(self, sock: socket.socket) -> None:
        """Make ``sock`` reachable by ``fire`` (an in-flight connect or the live socket)."""
        with self._lock:
            self._sockets.append(sock)
            already_fired = self._fired.is_set()
        if already_fired:
            _shutdown(sock)

    def fire(self) -> None:
        self._fired.set()
        with self._lock:
            sockets = list(self._sockets)
        for sock in sockets:
            _shutdown(sock)


def _shutdown(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _connect_within(
    deadline: _Deadline,
    address: tuple[str, int],
    timeout: float | None,
    source_address: tuple[str, int] | None = None,
) -> socket.socket:
    """``socket.create_connection`` with the remaining budget per attempt.

    Every candidate socket is registered with the deadline before connecting,
    so the timer can abort an attempt that is still waiting for the peer. Name
    resolution itself cannot be interrupted.
    """
    del timeout  # the deadline, not the connection's static timeout, rules here
    host, port = address
    if deadline.expired():
        msg = "deadline expired before connecting"
        raise TimeoutError(msg)
    last_error: OSError | None = None
    for family, kind, proto, _, sockaddr in socket.getaddrinfo(
        host, port, 0, socket.SOCK_STREAM
    ):
        if deadline.expired():
            msg = "deadline expired while connecting"
            raise TimeoutError(msg)
        sock = socket.socket(family, kind, proto)
        try:
            sock.settimeout(deadline.remaining())
            if source_address:
                sock.bind(source_address)
            deadline.register(sock)
            sock.connect(sockaddr)
        except OSError as exc:
            sock.close()
            last_error = exc
            continue
        return sock
    if last_error is not None:
        raise last_error
    msg = f"getaddrinfo returned no addresses for {host!r}"
    raise OSError(msg)


def _bind_deadline(
    conn: http.client.HTTPConnection, deadline: _Deadline | None
) -> None:
    # ``_create_connection`` is the connection's socket factory; swapping it
    # keeps the stdlib connect (TLS wrapping, ALPN, tunnelling) intact while
    # every TCP attempt is budgeted and interruptible.
    if deadline is not None and hasattr(conn, "_create_connection"):
        conn._create_connection = (  # type: ignore[attr-defined]  # stdlib hook
            lambda address, timeout=None, source_address=None: _connect_within(
                deadline, address, timeout, source_address
            )
        )


def _after_connect(
    conn: http.client.HTTPConnection, deadline: _Deadline | None
) -> None:
    if deadline is None:
        return
    if conn.sock is not None:
        deadline.register(conn.sock)  # the (possibly TLS-wrapped) live socket
    if deadline.expired():
        conn.close()
        msg = "deadline expired while connecting"
        raise TimeoutError(msg)


class _HTTPConnection(http.client.HTTPConnection):
    def __init__(self, *args: Any, deadline: _Deadline | None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._deadline = deadline
        _bind_deadline(self, deadline)

    def connect(self) -> None:
        super().connect()
        _after_connect(self, self._deadline)


class _HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args: Any, deadline: _Deadline | None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._deadline = deadline
        _bind_deadline(self, deadline)

    def connect(self) -> None:
        super().connect()
        _after_connect(self, self._deadline)


class _HTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, deadline: _Deadline | None) -> None:
        super().__init__()
        self._deadline = deadline

    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        deadline = self._deadline

        def factory(*args: Any, **kwargs: Any) -> _HTTPConnection:
            return _HTTPConnection(*args, deadline=deadline, **kwargs)

        return self.do_open(factory, req)  # type: ignore[arg-type]  # factory, not class


class _HTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, deadline: _Deadline | None) -> None:
        super().__init__()
        self._deadline = deadline

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        deadline = self._deadline

        def factory(*args: Any, **kwargs: Any) -> _HTTPSConnection:
            return _HTTPSConnection(*args, deadline=deadline, **kwargs)

        # No context is configured on this handler, so the connection builds the
        # stdlib default (certificate verification, ALPN http/1.1).
        return self.do_open(factory, req)  # type: ignore[arg-type]  # factory, not class


def _opener(deadline: _Deadline | None) -> urllib.request.OpenerDirector:
    # build_opener keeps the default handlers (proxy discovery, header merging,
    # IPv6 hosts, error handling) and swaps in ours where classes overlap.
    return urllib.request.build_opener(
        _NoRedirect(), _HTTPHandler(deadline), _HTTPSHandler(deadline)
    )


def compact_dumps(value: Any) -> str:
    """Serialize ``value`` as compact JSON (no whitespace, non-ASCII preserved).

    Matches the JS exporters' ``JSON.stringify`` output so the wire bytes are
    identical across SDKs.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def sanitize(value: str) -> str:
    """Replace characters unsafe for object keys / file names with ``_``."""
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def sql_identifier(name: str) -> str:
    """Return ``name`` if it is a plain SQL identifier, else raise ``ValueError``.

    Table and schema names are interpolated into SQL text, so only letters,
    digits, and underscores are accepted.
    """
    if not _SQL_IDENTIFIER.match(name):
        msg = (
            f'Invalid SQL identifier: "{name}". '
            "Only letters, digits, and underscores are allowed."
        )
        raise ValueError(msg)
    return name


def parse_iso_datetime(value: str) -> datetime.datetime:
    """Parse an ISO-8601 timestamp (``Z`` or offset) into an aware UTC datetime."""
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC)


def http_send(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """Send one HTTP request and return ``(status, reason, error_text)``.

    A non-2xx status is returned, not raised, so callers build their own error
    message. Redirects are not followed: a 3xx is returned like any other
    failure. ``error_text`` is the first ``_MAX_ERROR_BODY_BYTES`` of a non-2xx
    response body and empty on success; a success body is never read. Network
    errors propagate.

    ``timeout`` (seconds) is a deadline for the whole request: connecting,
    sending, and receiving the status, headers and any error body. On expiry
    the live socket is shut down and ``TimeoutError`` is raised, even against
    a peer that keeps the connection alive by trickling bytes, and a response
    that completes after the deadline is never reported as success. Name
    resolution cannot be interrupted. ``None`` means no limit.
    """
    scheme = urlsplit(url).scheme
    if scheme not in ("http", "https"):
        msg = f"Unsupported URL scheme {scheme!r} in {url!r} (need http or https)"
        raise ValueError(msg)
    request = urllib.request.Request(url, data=body, method=method)
    for key, value in headers.items():
        request.add_header(key, value)

    deadline = _Deadline(timeout) if timeout is not None else None
    timer: threading.Timer | None = None
    if deadline is not None:
        timer = threading.Timer(deadline.seconds, deadline.fire)
        timer.daemon = True
        timer.start()
    timed_out = f"request to {url} exceeded {timeout}s"
    try:
        try:
            with _opener(deadline).open(request, timeout=timeout) as response:  # noqa: S310
                status = int(response.status)
                reason = str(response.reason or "")
                detail = ""
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            reason = str(exc.reason or "")
            try:
                detail = exc.read(_MAX_ERROR_BODY_BYTES).decode(
                    "utf-8", errors="replace"
                )
            except Exception:  # noqa: BLE001 - the body is best-effort detail only
                detail = ""
    except Exception as exc:
        if deadline is not None and deadline.expired():
            raise TimeoutError(timed_out) from exc
        raise
    finally:
        if timer is not None:
            timer.cancel()
    # Whatever arrived after the deadline is not a delivery.
    if deadline is not None and deadline.expired():
        raise TimeoutError(timed_out)
    return status, reason, detail
