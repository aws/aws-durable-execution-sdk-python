"""Regressions for the real-cloud 403 and premature-loser false positives."""

import io
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber
import pytest

from lmi_tests.fixture import Trace
from lmi_tests import fixture


@pytest.fixture
def trace():
    value = Trace.__new__(Trace)
    value.capture_lock, value.capture_sequence = threading.Lock(), 0
    value.s3 = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    value.bucket = "test-bucket"
    value.identity = {"run": "unit"}
    value.payload = {"scenario": "control"}
    value.shared = False
    value.control_prefix = "runs/unit/control/"
    value.emit = Mock()
    return value


@pytest.mark.parametrize("state,released", [(b"hold", False), (b"release", True)])
def test_control_reads_explicit_state_and_closes_body(trace, state, released):
    body = io.BytesIO(state)
    with Stubber(trace.s3) as stub:
        stub.add_response(
            "get_object",
            {"Body": body},
            {"Bucket": "test-bucket", "Key": "runs/unit/control/gate"},
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


@pytest.mark.parametrize("elapsed,escaped", [(90, False), (180, True)])
def test_fault_gate_outlives_invocation_deadline_but_remains_bounded(
    trace, monkeypatch, elapsed, escaped
):
    trace.deadline = 60
    times = iter([0, elapsed])
    trace.released = Mock(side_effect=[False, False, True])
    monkeypatch.setattr(
        fixture,
        "time",
        SimpleNamespace(
            monotonic=lambda: next(times),
            sleep=lambda _: None,
            time=time.time,
            time_ns=time.time_ns,
        ),
    )
    if escaped:
        with pytest.raises(TimeoutError, match="emergency release"):
            trace.gate("fault", effects=True)
    else:
        trace.gate("fault", effects=True)
    phases = [call.args[0] for call in trace.emit.call_args_list]
    assert ("ESCAPE" in phases) is escaped
    assert ("EFFECT" in phases) is not escaped


def test_winner_is_signalled_only_after_loser_actually_enters_held_io(trace):
    controls = {
        "runs/unit/control/loser": b"hold",
        "runs/unit/control/ready": b"hold",
        "runs/unit/control/release-all": b"hold",
    }
    trace.s3 = Mock()
    trace.s3.get_object.side_effect = lambda **kw: {
        "Body": io.BytesIO(controls[kw["Key"]])
    }

    def signal(**kwargs):
        assert (
            kwargs["Key"] == "runs/unit/control/ready" and kwargs["Body"] == b"release"
        )
        assert [c.args[0] for c in trace.emit.call_args_list] == ["BLOCKED", "EFFECT"]
        controls[kwargs["Key"]] = kwargs["Body"]
        controls["runs/unit/control/loser"] = b"release"

    trace.s3.put_object.side_effect = signal
    trace.gate("loser", effects=True, ready="ready")
    assert [c.args[0] for c in trace.emit.call_args_list] == [
        "BLOCKED",
        "EFFECT",
        "IO_EXIT",
    ]
    assert controls["runs/unit/control/release-all"] == b"hold"


def test_emitted_records_are_partitioned_by_case_and_request(trace):
    trace.s3 = Mock()
    trace.identity = {"run": "unit", "marker": "case", "request": "request"}
    trace.sequence = 0
    trace.deadline = 10
    trace.lock = threading.Lock()
    Trace.emit(trace, "BODY")
    assert (
        trace.s3.put_object.call_args.kwargs["Key"]
        == "runs/unit/events/case/request/000001.json"
    )
