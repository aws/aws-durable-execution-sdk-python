# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The job document contract between the durable function and the worker.

The durable function sends a job in one of two ways:

1. In the ``runHookPayload`` of RunMicrovm. Lambda passes the payload to the
   worker's ``run`` lifecycle hook when the MicroVM starts.
2. As the body of an HTTP request to a route in the MicroVM.

Both carry the same job fields. The contract is the same for every SDK
language, so a durable function in one language can drive a worker in
another. The JavaScript worker in ``@aws/durable-execution-sdk-js-microvm-worker``
defines the same documents. A change to the contract must be made in every
language.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any


SUPPORTED_PAYLOAD_VERSION = 1
"""The payload version that this package can process."""

MAX_MICROVM_LIFETIME_SECONDS = 8 * 60 * 60
"""A MicroVM lives at most 8 hours. A longer idle time could never end."""

_MISSING_ID_MESSAGE = (
    "run hook body must be an object with a non-empty string microvmId"
)


class InvalidRunHookPayloadError(ValueError):
    """A ``run`` hook body or a job request that does not match the contract.

    ``callback_id`` and ``region`` are set when the document had a non-empty
    value for them. The worker then reports the error to that callback. So
    the durable function fails at once, instead of at the callback timeout.
    """

    def __init__(
        self,
        message: str,
        callback_id: str | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(message)
        self.callback_id = callback_id
        self.region = region


@dataclass(frozen=True, kw_only=True)
class MicrovmJobDocument:
    """One job: the fields that both deliveries share.

    Wire names: ``callbackId``, ``heartbeatTimeoutSeconds``, and ``input``.
    """

    callback_id: str
    """The callback that the worker completes with the job's outcome."""
    input: Any
    """The caller's input, as parsed from JSON."""
    heartbeat_timeout_seconds: float | None = None
    """The heartbeat timeout in seconds, when the caller set one."""

    @classmethod
    def from_dict(cls, data: object, *, region: str, label: str) -> MicrovmJobDocument:
        """Validate the job fields of a document.

        Args:
            data: The parsed JSON object that holds the job fields.
            region: The document's Region. An error carries it, so the worker
                can fail the callback.
            label: Names the document in error messages.

        Raises:
            InvalidRunHookPayloadError: When the job fields do not match.
        """
        callback_id = (
            _non_empty_string(data.get("callbackId"))
            if isinstance(data, dict)
            else None
        )
        if not isinstance(data, dict) or callback_id is None:
            msg = f"{label} must be an object with a non-empty callbackId"
            raise InvalidRunHookPayloadError(msg)
        heartbeat = data.get("heartbeatTimeoutSeconds")
        # A shorter heartbeat timeout would need heartbeats many times a second.
        if heartbeat is not None and not (
            is_finite_number(heartbeat) and heartbeat >= 1
        ):
            msg = f"{label} heartbeatTimeoutSeconds must be a number of at least 1"
            raise InvalidRunHookPayloadError(msg, callback_id, region)
        return cls(
            callback_id=callback_id,
            input=data.get("input"),
            heartbeat_timeout_seconds=None
            if heartbeat is None
            else _to_float(heartbeat),
        )


@dataclass(frozen=True, kw_only=True)
class MicrovmRunHookPayload:
    """The JSON document in ``runHookPayload``."""

    version: int
    region: str
    """The Region of the durable function, for the callback API calls."""
    job: MicrovmJobDocument | None = None
    """The job, when the durable function delivers it in the ``run`` hook."""
    auto_suspend_idle_seconds: float | None = None
    """Suspend the MicroVM after it ran no job for this long. Only a session
    sets it. Without it, the worker never suspends the MicroVM."""

    @classmethod
    def from_json(cls, text: str) -> MicrovmRunHookPayload:
        """Parse and validate the ``runHookPayload`` string.

        Raises:
            InvalidRunHookPayloadError: When the payload does not match the
                contract, or its version is not supported.
        """
        try:
            payload = loads_strict(text)
        except ValueError:
            msg = "runHookPayload is not valid JSON"
            raise InvalidRunHookPayloadError(msg) from None
        if not isinstance(payload, dict):
            msg = "runHookPayload must be an object"
            raise InvalidRunHookPayloadError(msg)

        region = _non_empty_string(payload.get("region"))
        raw_job = payload.get("job")
        job_callback_id = (
            _non_empty_string(raw_job.get("callbackId"))
            if isinstance(raw_job, dict)
            else None
        )
        _check_version(payload.get("version"), job_callback_id, region)
        if region is None:
            msg = "runHookPayload must have a non-empty region"
            raise InvalidRunHookPayloadError(msg, job_callback_id)
        job = (
            None
            if raw_job is None
            else MicrovmJobDocument.from_dict(
                raw_job, region=region, label="runHookPayload job"
            )
        )

        idle = payload.get("autoSuspendIdleSeconds")
        if idle is not None and not (
            is_finite_number(idle) and 0 < idle <= MAX_MICROVM_LIFETIME_SECONDS
        ):
            msg = (
                "runHookPayload autoSuspendIdleSeconds must be a positive number "
                f"of at most {MAX_MICROVM_LIFETIME_SECONDS}"
            )
            raise InvalidRunHookPayloadError(msg, job_callback_id, region)
        return cls(
            version=SUPPORTED_PAYLOAD_VERSION,
            region=region,
            job=job,
            auto_suspend_idle_seconds=idle,
        )


@dataclass(frozen=True, kw_only=True)
class RunHookRequest:
    """The body that Lambda sends to the ``run`` lifecycle hook."""

    microvm_id: str
    payload: MicrovmRunHookPayload | None = None
    """The decoded ``runHookPayload``. It is ``None`` when the body has none,
    which happens when no durable operation started the MicroVM."""

    @classmethod
    def from_dict(cls, body: object) -> RunHookRequest:
        """Parse and validate a ``run`` lifecycle hook body.

        The payload is checked before the MicroVM ID. So an error for a
        missing ID carries the job's callback ID and Region, when the payload
        delivered a valid job, and the worker can fail that callback.

        Args:
            body: The parsed JSON body of the ``run`` hook request.

        Raises:
            InvalidRunHookPayloadError: When the body or the payload does not
                match the contract, or the payload version is not supported.
        """
        if not isinstance(body, dict):
            raise InvalidRunHookPayloadError(_MISSING_ID_MESSAGE)
        microvm_id = _non_empty_string(body.get("microvmId"))
        raw_payload = body.get("runHookPayload")
        if raw_payload is None or raw_payload == "":
            if microvm_id is None:
                raise InvalidRunHookPayloadError(_MISSING_ID_MESSAGE)
            return cls(microvm_id=microvm_id)
        if not isinstance(raw_payload, str):
            msg = "runHookPayload must be a string"
            raise InvalidRunHookPayloadError(msg)
        payload = MicrovmRunHookPayload.from_json(raw_payload)
        if microvm_id is None:
            raise InvalidRunHookPayloadError(
                _MISSING_ID_MESSAGE,
                None if payload.job is None else payload.job.callback_id,
                payload.region,
            )
        return cls(microvm_id=microvm_id, payload=payload)


@dataclass(frozen=True, kw_only=True)
class MicrovmJobRequest:
    """The body of the HTTP request that delivers one job to a route.

    On the wire, the job fields sit at the top level of the body, next to
    ``version``, ``region``, and ``microvmId``. Here they are in ``job``.
    """

    version: int
    region: str
    """The Region of the durable function, for the callback API calls."""
    job: MicrovmJobDocument
    """The job."""
    microvm_id: str | None = None
    """The MicroVM identifier. A job request can reach the worker before the
    ``run`` hook, which also carries the identifier. Until the ``run`` hook
    arrives, the worker takes it from the first job request that has one."""

    @classmethod
    def from_dict(cls, body: object) -> MicrovmJobRequest:
        """Parse and validate the body of a job request to a route.

        Args:
            body: The parsed JSON body of the request.

        Raises:
            InvalidRunHookPayloadError: When the body does not match the
                contract, or its version is not supported.
        """
        if not isinstance(body, dict):
            msg = "job request body must be an object"
            raise InvalidRunHookPayloadError(msg)
        callback_id = _non_empty_string(body.get("callbackId"))
        region = _non_empty_string(body.get("region"))
        _check_version(body.get("version"), callback_id, region)
        if region is None:
            msg = "job request must have a non-empty region"
            raise InvalidRunHookPayloadError(msg, callback_id)
        job = MicrovmJobDocument.from_dict(body, region=region, label="job request")
        raw_microvm_id = body.get("microvmId")
        microvm_id = _non_empty_string(raw_microvm_id)
        if raw_microvm_id is not None and microvm_id is None:
            msg = "job request microvmId must be a non-empty string"
            raise InvalidRunHookPayloadError(msg, callback_id, region)
        return cls(
            version=SUPPORTED_PAYLOAD_VERSION,
            region=region,
            job=job,
            microvm_id=microvm_id,
        )


def loads_strict(text: str | bytes) -> Any:
    """Parse JSON, and reject ``NaN``, ``Infinity``, and ``-Infinity``.

    Python's ``json`` module accepts those three tokens by default. They are
    not JSON, and the durable function in another language never sends them.
    A heartbeat timeout of ``Infinity`` would pass a check such as ``>= 1``.
    So the worker rejects them when it parses.

    Raises:
        ValueError: When the text is not valid JSON.
    """

    def reject(token: str) -> Any:
        msg = f"{token} is not valid JSON"
        raise ValueError(msg)

    return json.loads(text, parse_constant=reject)


def _check_version(
    version: object, callback_id: str | None, region: str | None
) -> None:
    # True == 1 in Python. A boolean is not a version, so it is checked too.
    if isinstance(version, bool) or version != SUPPORTED_PAYLOAD_VERSION:
        msg = (
            f"payload version {version} is not supported. "
            f"This worker supports version {SUPPORTED_PAYLOAD_VERSION}."
        )
        raise InvalidRunHookPayloadError(msg, callback_id, region)


def _to_float(value: float) -> float:
    """Convert a JSON number to a float.

    An int too large for a float becomes ``math.inf``. The heartbeat
    interval is capped at 15 minutes, so an infinite heartbeat timeout gives
    the same schedule as any timeout of 45 minutes or more.
    """
    try:
        return float(value)
    except OverflowError:
        return math.inf


def _non_empty_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def is_finite_number(value: object) -> bool:
    """Whether a value is a JSON number that is finite.

    1. A ``bool`` is an ``int`` in Python, and JSON ``true`` is not a number.
       So a ``bool`` is not a number here.
    2. An ``int`` is always finite. ``math.isfinite`` would raise
       ``OverflowError`` for an ``int`` too large for a float. So only a
       ``float`` goes through it.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)
