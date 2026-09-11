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
import json
import re
import urllib.error
import urllib.request
from typing import Any


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


_OPENER = urllib.request.build_opener(_NoRedirect())


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
    errors and timeouts propagate.
    """
    request = urllib.request.Request(url, data=body, method=method)
    for key, value in headers.items():
        request.add_header(key, value)
    try:
        with _OPENER.open(request, timeout=timeout) as response:  # noqa: S310
            return int(response.status), str(response.reason or ""), ""
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read(_MAX_ERROR_BODY_BYTES).decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - the body is best-effort detail only
            detail = ""
        return int(exc.code), str(exc.reason or ""), detail
