"""Regressions for the real-cloud 403 and premature-loser false positives."""

import io
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber
import pytest

from lmi_tests.fixture import Trace, workflow
from lmi_tests import evidence
from lmi_tests.tests.regressions.test_issue741 import invocation, lambda_context
from aws_durable_execution_sdk_python import durable_execution


@pytest.fixture
def trace():
    value = Trace.__new__(Trace)
    value.s3 = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    value.bucket = "test-bucket"
    value.emit = Mock()
    return value


@pytest.mark.parametrize("state,released", [(b"hold", False), (b"release", True)])
def test_control_reads_explicit_state_and_closes_body(trace, state, released):
    body = io.BytesIO(state)
    with Stubber(trace.s3) as stub:
        stub.add_response(
            "get_object",
            {"Body": body},
            {"Bucket": "test-bucket", "Key": "control/gate"},
        )
        assert trace.released("gate") is released
    assert body.closed
    trace.emit.assert_not_called()


@pytest.mark.parametrize("code,status", [("403", 403), ("NoSuchKey", 404)])
def test_missing_or_forbidden_control_is_not_a_hold_or_release(trace, code, status):
    with Stubber(trace.s3) as stub:
        stub.add_client_error(
            "get_object", service_error_code=code, http_status_code=status
        )
        with pytest.raises(ClientError):
            trace.gate("gate", effects=True, ready="ready")
    phases = [call.args[0] for call in trace.emit.call_args_list]
    assert "CONTROL_ERROR" in phases
    assert not {"BLOCKED", "ALIVE", "EFFECT"} & set(phases)


def test_invalid_control_content_cannot_release_work(trace):
    with Stubber(trace.s3) as stub:
        stub.add_response("get_object", {"Body": io.BytesIO(b"unexpected")})
        with pytest.raises(ValueError, match="Invalid control state"):
            trace.released("gate")
    assert trace.emit.call_args.args[0] == "CONTROL_ERROR"


def test_winner_is_signalled_only_after_loser_actually_enters_held_io(trace):
    controls = {
        "control/loser": b"hold",
        "control/ready": b"hold",
        "control/release-all": b"hold",
    }
    trace.s3 = Mock()
    trace.s3.get_object.side_effect = lambda **kw: {
        "Body": io.BytesIO(controls[kw["Key"]])
    }

    def signal(**kwargs):
        assert kwargs["Key"] == "control/ready" and kwargs["Body"] == b"release"
        assert [c.args[0] for c in trace.emit.call_args_list] == ["BLOCKED", "EFFECT"]
        controls[kwargs["Key"]] = kwargs["Body"]
        controls["control/loser"] = b"release"

    trace.s3.put_object.side_effect = signal
    trace.gate("loser", effects=True, ready="ready")
    assert [c.args[0] for c in trace.emit.call_args_list] == [
        "BLOCKED",
        "EFFECT",
        "IO_EXIT",
    ]
    assert controls["control/release-all"] == b"hold"


def test_emitted_records_are_partitioned_by_case_and_request(trace):
    trace.s3 = Mock()
    trace.identity = {"marker": "case", "request": "request"}
    trace.sequence = 0
    trace.deadline = 10
    trace.lock = threading.Lock()
    Trace.emit(trace, "BODY")
    assert (
        trace.s3.put_object.call_args.kwargs["Key"] == "events/case/request/000001.json"
    )


@pytest.mark.parametrize("scenario", ["parallel", "map", "nested"])
def test_full_fixture_establishes_a_real_blocked_loser(scenario):
    """Exercise fixture wiring and SDK APIs; valid evidence survives future SDK fixes."""
    objects = {
        "control/release-all": b"hold",
        "control/local-loser": b"hold",
        "control/local-loser-started": b"hold",
    }
    events = []
    computed = threading.Event()
    lock = threading.Lock()

    def put(**kwargs):
        with lock:
            objects[kwargs["Key"]] = kwargs["Body"]
            if kwargs["Key"].startswith("events/"):
                event = json.loads(kwargs["Body"])
                events.append(event)
                if event["phase"] == "WINNER_SELECTED":
                    computed.set()

    def get(**kwargs):
        with lock:
            return {"Body": io.BytesIO(objects[kwargs["Key"]])}

    trace = Trace.__new__(Trace)
    trace.s3, trace.bucket = Mock(), "test-bucket"
    trace.s3.put_object.side_effect, trace.s3.get_object.side_effect = put, get
    trace.identity = {"marker": "local", "request": "request"}
    trace.lock, trace.sequence, trace.deadline = threading.Lock(), 0, time.time() + 30
    handler = durable_execution(
        lambda _e, c: workflow({"marker": "local", "scenario": scenario}, c, trace)
    )
    with ThreadPoolExecutor(1) as caller:
        future = caller.submit(handler, invocation(), lambda_context(30000))
        try:
            assert computed.wait(3), (
                "The controlled fixture did not reach winner selection"
            )
            with lock:
                snapshot = list(events)
            evidence.held_loser(snapshot)
            assert any(e["phase"] == "EFFECT" for e in snapshot)
        finally:
            put(Key="control/local-loser", Body=b"release")
            assert future.result(timeout=3)["Status"] == "SUCCEEDED"
