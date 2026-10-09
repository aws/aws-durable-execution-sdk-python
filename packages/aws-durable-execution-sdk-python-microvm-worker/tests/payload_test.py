# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the job document contract."""

from __future__ import annotations

import json
import math
from typing import Any

import pytest

from aws_durable_execution_sdk_python_microvm_worker.payload import (
    InvalidRunHookPayloadError,
    MicrovmJobDocument,
    MicrovmJobRequest,
    RunHookRequest,
    loads_strict,
)


JOB = {"callbackId": "cb-1", "heartbeatTimeoutSeconds": 60, "input": {"n": 1}}


def run_hook(payload: Any, microvm_id: Any = "mvm-1") -> dict[str, Any]:
    """A run hook body whose runHookPayload is the JSON text of payload."""
    body: dict[str, Any] = {"runHookPayload": json.dumps(payload)}
    if microvm_id is not None:
        body["microvmId"] = microvm_id
    return body


def job_request(**overrides: Any) -> dict[str, Any]:
    """An HTTP job request body."""
    body: dict[str, Any] = {"version": 1, "region": "us-east-1", **JOB}
    body.update(overrides)
    return body


# region run hook


def test_run_hook_with_job():
    parsed = RunHookRequest.from_dict(
        run_hook({"version": 1, "region": "us-east-1", "job": JOB})
    )

    assert parsed.microvm_id == "mvm-1"
    assert parsed.payload is not None
    assert parsed.payload.region == "us-east-1"
    assert parsed.payload.job == MicrovmJobDocument(
        callback_id="cb-1", input={"n": 1}, heartbeat_timeout_seconds=60
    )
    assert parsed.payload.auto_suspend_idle_seconds is None


def test_run_hook_without_job_or_payload():
    without_job = RunHookRequest.from_dict(
        run_hook({"version": 1, "region": "eu-west-1"})
    )
    assert without_job.payload is not None
    assert without_job.payload.job is None

    for body in ({"microvmId": "mvm-1"}, {"microvmId": "mvm-1", "runHookPayload": ""}):
        parsed = RunHookRequest.from_dict(body)
        assert parsed.microvm_id == "mvm-1"
        assert parsed.payload is None


def test_run_hook_auto_suspend_idle_seconds():
    parsed = RunHookRequest.from_dict(
        run_hook({"version": 1, "region": "us-east-1", "autoSuspendIdleSeconds": 60})
    )
    assert parsed.payload is not None
    assert parsed.payload.auto_suspend_idle_seconds == 60


@pytest.mark.parametrize("idle", [0, -1, 28_801, True, "60", 1e309])
def test_run_hook_rejects_invalid_auto_suspend(idle):
    payload = {
        "version": 1,
        "region": "us-east-1",
        "job": JOB,
        "autoSuspendIdleSeconds": idle,
    }
    # 1e309 is infinite as a float. json.dumps writes it as Infinity, which
    # the strict parser rejects as invalid JSON before the range check.
    with pytest.raises(InvalidRunHookPayloadError) as raised:
        RunHookRequest.from_dict(run_hook(payload))
    if idle != 1e309:
        assert raised.value.callback_id == "cb-1"
        assert raised.value.region == "us-east-1"


@pytest.mark.parametrize(
    "body",
    [None, [], "x", {}, {"microvmId": ""}, {"microvmId": 3}],
)
def test_run_hook_rejects_body_without_microvm_id(body):
    with pytest.raises(InvalidRunHookPayloadError, match="microvmId"):
        RunHookRequest.from_dict(body)


def test_run_hook_missing_id_error_names_the_job():
    """The payload is checked first, so the job's callback can still be failed."""
    with pytest.raises(InvalidRunHookPayloadError, match="microvmId") as raised:
        RunHookRequest.from_dict(
            run_hook({"version": 1, "region": "us-east-1", "job": JOB}, microvm_id=None)
        )
    assert raised.value.callback_id == "cb-1"
    assert raised.value.region == "us-east-1"


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (3, "must be a string"),
        ("{", "not valid JSON"),
        ("NaN", "not valid JSON"),
        ("[]", "must be an object"),
    ],
)
def test_run_hook_rejects_malformed_payload(raw, message):
    with pytest.raises(InvalidRunHookPayloadError, match=message):
        RunHookRequest.from_dict({"microvmId": "mvm-1", "runHookPayload": raw})


def test_invalid_json_error_keeps_the_parse_position():
    """The payload comes from another SDK language, so the position matters."""
    with pytest.raises(InvalidRunHookPayloadError) as raised:
        RunHookRequest.from_dict({"microvmId": "mvm-1", "runHookPayload": '{"a":'})
    cause = raised.value.__cause__
    assert isinstance(cause, json.JSONDecodeError)
    assert (cause.lineno, cause.colno) == (1, 6)


@pytest.mark.parametrize("version", [None, 2, "1", True, 1.5])
def test_run_hook_rejects_unsupported_version(version):
    payload = {"version": version, "region": "us-east-1", "job": JOB}
    with pytest.raises(InvalidRunHookPayloadError, match="not supported") as raised:
        RunHookRequest.from_dict(run_hook(payload))
    assert raised.value.callback_id == "cb-1"
    assert raised.value.region == "us-east-1"


def test_run_hook_accepts_version_as_float_one():
    """JSON has one number type. 1.0 is the same version as 1."""
    parsed = RunHookRequest.from_dict(run_hook({"version": 1.0, "region": "us-east-1"}))
    assert parsed.payload is not None


def test_run_hook_rejects_missing_region():
    with pytest.raises(InvalidRunHookPayloadError, match="region") as raised:
        RunHookRequest.from_dict(run_hook({"version": 1, "job": JOB}))
    assert raised.value.callback_id == "cb-1"
    assert raised.value.region is None


@pytest.mark.parametrize("job", [[], {"callbackId": ""}, {"input": 1}])
def test_run_hook_rejects_job_without_callback_id(job):
    with pytest.raises(InvalidRunHookPayloadError, match="callbackId") as raised:
        RunHookRequest.from_dict(
            run_hook({"version": 1, "region": "us-east-1", "job": job})
        )
    assert raised.value.callback_id is None


@pytest.mark.parametrize("heartbeat", [0, 0.5, "60", True, None])
def test_run_hook_heartbeat_timeout(heartbeat):
    job = {"callbackId": "cb-1", "input": None, "heartbeatTimeoutSeconds": heartbeat}
    payload = {"version": 1, "region": "us-east-1", "job": job}
    if heartbeat is None:
        parsed = RunHookRequest.from_dict(run_hook(payload))
        assert parsed.payload is not None
        assert parsed.payload.job is not None
        assert parsed.payload.job.heartbeat_timeout_seconds is None
        return
    with pytest.raises(
        InvalidRunHookPayloadError, match="heartbeatTimeoutSeconds"
    ) as raised:
        RunHookRequest.from_dict(run_hook(payload))
    assert raised.value.callback_id == "cb-1"
    assert raised.value.region == "us-east-1"


# endregion run hook


# region job request


def test_job_request():
    parsed = MicrovmJobRequest.from_dict(job_request(microvmId="mvm-1"))

    assert parsed.job == MicrovmJobDocument(
        callback_id="cb-1", input={"n": 1}, heartbeat_timeout_seconds=60
    )
    assert parsed.version == 1
    assert parsed.region == "us-east-1"
    assert parsed.microvm_id == "mvm-1"


def test_job_request_without_microvm_id_or_input():
    parsed = MicrovmJobRequest.from_dict(
        {"version": 1, "region": "us-east-1", "callbackId": "cb-1"}
    )
    assert parsed.microvm_id is None
    assert parsed.job.input is None


@pytest.mark.parametrize(
    ("body", "message", "callback_id", "region"),
    [
        ([], "must be an object", None, None),
        (job_request(version=2), "not supported", "cb-1", "us-east-1"),
        (job_request(region=""), "region", "cb-1", None),
        (job_request(callbackId=""), "callbackId", None, None),
        (
            job_request(heartbeatTimeoutSeconds=0),
            "heartbeatTimeoutSeconds",
            "cb-1",
            "us-east-1",
        ),
        (job_request(microvmId=""), "microvmId", "cb-1", "us-east-1"),
        (job_request(microvmId=3), "microvmId", "cb-1", "us-east-1"),
    ],
)
def test_job_request_rejects(body, message, callback_id, region):
    with pytest.raises(InvalidRunHookPayloadError, match=message) as raised:
        MicrovmJobRequest.from_dict(body)
    assert raised.value.callback_id == callback_id
    assert raised.value.region == region


# endregion job request

# region strict JSON


@pytest.mark.parametrize("text", ["NaN", "[Infinity]", '{"a": -Infinity}'])
def test_loads_strict_rejects_non_json_constants(text):
    with pytest.raises(ValueError, match="not valid JSON"):
        loads_strict(text)


def test_loads_strict_parses_json():
    assert loads_strict(b'{"a": [1, 2.5, null, true]}') == {"a": [1, 2.5, None, True]}


def test_large_integer_heartbeat_is_accepted():
    """An int too large for a float is a finite number. It becomes infinity."""
    parsed = MicrovmJobRequest.from_dict(job_request(heartbeatTimeoutSeconds=10**400))
    assert parsed.job.heartbeat_timeout_seconds == math.inf


# endregion strict JSON


# region dataclasses and callback IDs


def test_documents_take_keyword_arguments_only():
    with pytest.raises(TypeError):
        MicrovmJobDocument("cb-1", None)  # type: ignore[misc]
    with pytest.raises(TypeError, match="region"):
        MicrovmJobRequest(  # type: ignore[call-arg]
            version=1, job=MicrovmJobDocument(callback_id="cb-1", input=None)
        )


@pytest.mark.parametrize("callback_id", ["", 3, None])
def test_an_unusable_callback_id_is_not_attached_to_errors(callback_id):
    """Only a non-empty callback ID can be failed, so only that one is attached."""
    job = {"callbackId": callback_id, "input": None}
    with pytest.raises(InvalidRunHookPayloadError) as raised:
        RunHookRequest.from_dict(
            run_hook({"version": 2, "region": "us-east-1", "job": job})
        )
    assert raised.value.callback_id is None
    with pytest.raises(InvalidRunHookPayloadError) as raised:
        MicrovmJobRequest.from_dict(job_request(version=2, callbackId=callback_id))
    assert raised.value.callback_id is None


# endregion dataclasses and callback IDs
