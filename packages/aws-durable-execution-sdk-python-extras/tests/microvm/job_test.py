# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the job document."""

from __future__ import annotations

import json
import math
from typing import Any

import pytest
from aws_durable_execution_sdk_python_microvm_worker import (
    MicrovmJobRequest,
    RunHookRequest,
)

from aws_durable_execution_sdk_python_extras.microvm.job import (
    MAX_JOB_INPUT_BYTES,
    MAX_RUN_HOOK_PAYLOAD_LENGTH,
    JobRequest,
    RunHookPayload,
    fits_run_hook,
    run_hook_payload_without_job,
)


def test_writes_compact_json_with_the_job_fields() -> None:
    payload = RunHookPayload(
        region="us-east-1",
        callback_id="cb-1",
        job_input={"repo": "org/app"},
        heartbeat_timeout_seconds=60,
    ).to_json()

    assert payload == (
        '{"version":1,"region":"us-east-1","job":{"callbackId":"cb-1",'
        '"heartbeatTimeoutSeconds":60,"input":{"repo":"org/app"}}}'
    )


def test_leaves_out_an_unset_heartbeat_timeout() -> None:
    payload = RunHookPayload(region="r", callback_id="cb", job_input=None).to_json()

    assert json.loads(payload)["job"] == {"callbackId": "cb", "input": None}


def test_keeps_non_ascii_text_so_the_length_counts_code_points() -> None:
    payload = RunHookPayload(region="r", callback_id="cb", job_input="日😀").to_json()

    assert "日😀" in payload


def test_escapes_only_a_lone_surrogate() -> None:
    # JSON.stringify escapes a lone surrogate and keeps other text. So the
    # same job has the same length in both languages.
    payload = RunHookPayload(
        region="r", callback_id="cb", job_input="é" * 1000 + "\ud800"
    ).to_json()
    plain = RunHookPayload(region="r", callback_id="cb", job_input="é" * 1000).to_json()

    payload.encode()
    assert "\\ud800" in payload
    assert "é" * 1000 in payload
    assert len(payload) == len(plain) + len("\\ud800")
    assert json.loads(payload)["job"]["input"] == "é" * 1000 + "\ud800"


@pytest.mark.parametrize("value", [math.nan, math.inf])
def test_rejects_a_float_that_is_not_json(value: float) -> None:
    with pytest.raises(ValueError, match="JSON compliant"):
        RunHookPayload(region="r", callback_id="cb", job_input=value).to_json()


def test_counts_code_points_against_the_run_hook_limit() -> None:
    assert fits_run_hook("😀" * MAX_RUN_HOOK_PAYLOAD_LENGTH)
    assert not fits_run_hook("😀" * (MAX_RUN_HOOK_PAYLOAD_LENGTH + 1))


def test_the_worker_reads_the_payload() -> None:
    # The worker package parses the same contract. A change on one side must
    # be made on the other.
    payload = RunHookPayload(
        region="us-east-1",
        callback_id="cb-1",
        job_input={"n": 1},
        heartbeat_timeout_seconds=30,
    ).to_json()

    request = RunHookRequest.from_dict(
        {"microvmId": "mvm-1", "runHookPayload": payload}
    )

    assert request.payload is not None
    assert request.payload.region == "us-east-1"
    job = request.payload.job
    assert job is not None
    assert (job.callback_id, job.input, job.heartbeat_timeout_seconds) == (
        "cb-1",
        {"n": 1},
        30.0,
    )


def test_writes_the_job_request_with_the_job_fields_at_the_top_level() -> None:
    body = JobRequest(
        region="us-east-1",
        microvm_id="mvm-1",
        callback_id="cb-1",
        job_input={"n": "日"},
        heartbeat_timeout_seconds=30,
    ).to_json()

    assert body == (
        '{"version":1,"region":"us-east-1","microvmId":"mvm-1",'
        '"callbackId":"cb-1","heartbeatTimeoutSeconds":30,"input":{"n":"日"}}'
    )


def test_the_worker_reads_the_job_request() -> None:
    body = JobRequest(
        region="us-east-1", microvm_id="mvm-1", callback_id="cb-1", job_input=[1]
    ).to_json()

    request = MicrovmJobRequest.from_dict(json.loads(body))

    assert request.microvm_id == "mvm-1"
    assert request.region == "us-east-1"
    assert (request.job.callback_id, request.job.input) == ("cb-1", [1])
    assert request.job.heartbeat_timeout_seconds is None


def test_the_worker_reads_a_run_hook_payload_without_a_job() -> None:
    payload = run_hook_payload_without_job("us-east-1")

    request = RunHookRequest.from_dict(
        {"microvmId": "mvm-1", "runHookPayload": payload}
    )

    assert payload == '{"version":1,"region":"us-east-1"}'
    assert request.payload is not None
    assert request.payload.job is None


def test_the_worker_reads_the_idle_time_of_a_session() -> None:
    payload = run_hook_payload_without_job("us-east-1", 60)

    request = RunHookRequest.from_dict(
        {"microvmId": "mvm-1", "runHookPayload": payload}
    )

    assert payload == '{"version":1,"region":"us-east-1","autoSuspendIdleSeconds":60}'
    assert request.payload is not None
    assert request.payload.auto_suspend_idle_seconds == 60


def test_writes_suspend_after_only_when_it_is_set() -> None:
    fields: dict[str, Any] = {
        "region": "us-east-1",
        "microvm_id": "mvm-1",
        "callback_id": "cb-1",
        "job_input": 1,
    }

    assert JobRequest(**fields, suspend_after=True).to_json() == (
        '{"version":1,"region":"us-east-1","microvmId":"mvm-1",'
        '"suspendAfter":true,"callbackId":"cb-1","input":1}'
    )
    assert "suspendAfter" not in JobRequest(**fields).to_json()


def test_the_worker_reads_suspend_after() -> None:
    body = JobRequest(
        region="us-east-1",
        microvm_id="mvm-1",
        callback_id="cb-1",
        job_input=None,
        suspend_after=True,
    ).to_json()

    assert MicrovmJobRequest.from_dict(json.loads(body)).suspend_after is True


def test_the_job_fields_fit_in_the_margin_of_the_input_limit() -> None:
    # 1. The longest MicroVM ID is 256 characters, each escaped in 6 bytes.
    # 2. The longest callback ID is 1,024 characters of 1 byte.
    # 3. The heartbeat timeout is at most 8 hours, and suspendAfter is set.
    # So the body of the largest input is at most the 6 MiB that the worker
    # reads.
    body = JobRequest(
        region="ap-southeast-5",
        microvm_id="\x01" * 256,
        callback_id="A" * 1024,
        job_input="x" * (MAX_JOB_INPUT_BYTES - 2),
        heartbeat_timeout_seconds=28_800,
        suspend_after=True,
    ).to_json()

    fixed = len(body.encode()) - MAX_JOB_INPUT_BYTES
    assert fixed == 112 + len("ap-southeast-5") + 256 * 6 + 1024 + len("28800")
    assert fixed < 3 * 1024
    assert len(body.encode()) <= 6 * 1024 * 1024
