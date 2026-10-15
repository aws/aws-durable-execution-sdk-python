# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The job document that the operation sends to the MicroVM.

The worker in the MicroVM reads the same document in every SDK language. The
Python worker in ``aws-durable-execution-sdk-python-microvm-worker`` and the
JavaScript worker in ``@aws/durable-execution-sdk-js-microvm-worker`` parse
it. A change to the document must be made in every language.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


PAYLOAD_VERSION = 1
"""The version of the job document."""

MAX_RUN_HOOK_PAYLOAD_LENGTH = 4_096
"""``RunMicrovm`` accepts a ``runHookPayload`` of at most 4,096 code points.

The service counts Unicode code points, not UTF-8 bytes. A probe against the
service from the JavaScript SDK accepted 4,096 code points of an emoji, and
rejected 4,097. A Python ``str`` has one item per code point. So ``len()``
counts what the service counts.
"""


@dataclass(frozen=True, kw_only=True)
class RunHookPayload:
    """The ``runHookPayload`` document that carries one job in ``RunMicrovm``.

    Wire names: ``version``, ``region``, and ``job``, whose fields are
    ``callbackId``, ``heartbeatTimeoutSeconds``, and ``input``.

    A job that goes over HTTP needs a ``runHookPayload`` too, because the
    worker rejects a payload without ``version`` and ``region``. See
    :func:`run_hook_payload_without_job`.
    """

    region: str
    """The Region of the durable function, for the callback API calls."""
    callback_id: str
    """The callback that the worker completes with the job's outcome."""
    job_input: Any
    """The caller's input. It must be JSON-serializable."""
    heartbeat_timeout_seconds: int | None = None
    """The heartbeat timeout, when the caller set one."""

    def to_json(self) -> str:
        """Serialize the payload as compact JSON.

        1. Non-ASCII text stays as it is, as ``JSON.stringify`` writes it. So
           the length counts the same code points as the JavaScript SDK's
           payload.
        2. A lone surrogate is not valid in the UTF-8 request body. So each
           one is written as a ``\\uXXXX`` escape, as ``JSON.stringify``
           writes it. A Python ``str`` holds an astral character as one code
           point, so every surrogate in it is a lone one.
        3. ``allow_nan=False`` rejects ``nan`` and infinite floats, because
           the worker rejects the ``NaN`` and ``Infinity`` tokens.

        Raises:
            TypeError: When the input is not JSON-serializable.
            ValueError: When the input holds ``nan`` or an infinite float.
        """
        job = _job_fields(
            self.callback_id, self.job_input, self.heartbeat_timeout_seconds
        )
        return dumps({"version": PAYLOAD_VERSION, "region": self.region, "job": job})


@dataclass(frozen=True, kw_only=True)
class JobRequest:
    """The body of the HTTP request that delivers one job to a route.

    Wire names: ``version``, ``region``, ``microvmId``, ``callbackId``,
    ``heartbeatTimeoutSeconds``, and ``input``. The job fields sit at the top
    level, next to ``version``, ``region``, and ``microvmId``.

    The endpoint can accept a request before Lambda has sent the ``run``
    hook. The worker learns the MicroVM ID from the ``run`` hook. So the
    request carries the MicroVM ID too.
    """

    region: str
    """The Region of the durable function, for the callback API calls."""
    microvm_id: str
    """The MicroVM that runs the job."""
    callback_id: str
    """The callback that the worker completes with the job's outcome."""
    job_input: Any
    """The caller's input. It must be JSON-serializable."""
    heartbeat_timeout_seconds: int | None = None
    """The heartbeat timeout, when the caller set one."""

    def to_json(self) -> str:
        """Serialize the request as compact JSON, as :meth:`RunHookPayload.to_json` does.

        Raises:
            TypeError: When the input is not JSON-serializable.
            ValueError: When the input holds ``nan`` or an infinite float.
        """
        return dumps(
            {
                "version": PAYLOAD_VERSION,
                "region": self.region,
                "microvmId": self.microvm_id,
                **_job_fields(
                    self.callback_id, self.job_input, self.heartbeat_timeout_seconds
                ),
            }
        )


def run_hook_payload_without_job(region: str) -> str:
    """The ``runHookPayload`` of a launch whose job goes over HTTP.

    The worker rejects a ``run`` hook payload without ``version`` and
    ``region``. So the payload keeps those two fields, and omits the job.
    """
    return dumps({"version": PAYLOAD_VERSION, "region": region})


def dumps(document: Any) -> str:
    """Serialize ``document`` as compact JSON, as ``JSON.stringify`` writes it.

    1. Non-ASCII text stays as it is.
    2. A lone surrogate is written as a ``\\uXXXX`` escape.
    3. ``nan`` and infinite floats are rejected.

    Raises:
        TypeError: When the document is not JSON-serializable.
        ValueError: When the document holds ``nan`` or an infinite float.
    """
    text = json.dumps(
        document, allow_nan=False, ensure_ascii=False, separators=(",", ":")
    )
    # A surrogate appears only inside a JSON string. So the escape is valid
    # JSON, and it decodes to the same surrogate.
    return _SURROGATE.sub(lambda match: f"\\u{ord(match.group()):04x}", text)


def _job_fields(
    callback_id: str, job_input: Any, heartbeat_timeout_seconds: int | None
) -> dict[str, Any]:
    job: dict[str, Any] = {"callbackId": callback_id}
    if heartbeat_timeout_seconds is not None:
        job["heartbeatTimeoutSeconds"] = heartbeat_timeout_seconds
    job["input"] = job_input
    return job


_SURROGATE = re.compile("[\ud800-\udfff]")


def fits_run_hook(payload: str) -> bool:
    """Whether ``payload`` fits in the ``runHookPayload`` of ``RunMicrovm``."""
    return len(payload) <= MAX_RUN_HOOK_PAYLOAD_LENGTH
