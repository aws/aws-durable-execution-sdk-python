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
    value.state_lock, value.capture_sequence = threading.Lock(), 0
    value.source_failed = False
    value.publication_seconds = 0.0
    value.closed = False
    value.call_sequence, value.sdk_request_count = 0, 0
    value.lock = threading.Lock()
    value.observe = Mock()
    value.s3 = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    value.bucket = "test-bucket"
    value.identity = {"run": "unit", "request": "unit-request"}
    value.payload = {"scenario": "control"}
    value.control_prefix = "runs/unit/cases/owner/control/"
    value.emit = Mock()
    return value


@pytest.mark.parametrize("state,released", [(b"hold", False), (b"release", True)])
def test_control_reads_explicit_state_and_closes_body(trace, state, released):
    body = io.BytesIO(state)
    with Stubber(trace.s3) as stub:
        stub.add_response(
            "get_object",
            {"Body": body},
            {"Bucket": "test-bucket", "Key": "runs/unit/cases/owner/control/gate"},
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
            trace.gate("gate", effects=True)
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


def held_deadline_gate(
    trace, monkeypatch, *, seconds_past_deadline, monotonic=lambda: 0
):
    """A held deadline-fixture gate polled at a fixed offset from the deadline."""
    trace.payload = {"scenario": "deadline"}
    trace.deadline = 1_000.0
    monkeypatch.setattr(
        fixture,
        "time",
        SimpleNamespace(
            monotonic=monotonic,
            sleep=lambda _: None,
            time=lambda: trace.deadline + seconds_past_deadline,
            time_ns=time.time_ns,
        ),
    )


def test_deadline_fixture_cooperatively_exits_held_io_after_the_deadline(
    trace, monkeypatch
):
    # Lifecycle cases 3 and 4 assert cleanup across a real platform timeout.
    # Without this exit the held step outlives the timeout until the 150 s
    # escape, which reports a harness failure instead of SDK cleanup. The
    # clock jumps past that escape after the first read, so a missing exit
    # fails here on the escape's message rather than holding forever.
    readings = iter([0])
    held_deadline_gate(
        trace,
        monkeypatch,
        seconds_past_deadline=0.2,
        monotonic=lambda: next(readings, 1_000),
    )
    trace.released = Mock(return_value=False)
    with pytest.raises(TimeoutError, match="crossed the runtime deadline"):
        trace.gate("fault", effects=True)
    phases = [call.args[0] for call in trace.emit.call_args_list]
    assert phases == ["IO_INTERRUPTED", "IO_EXIT"]


def test_deadline_exit_waits_one_poll_interval_past_the_deadline(trace, monkeypatch):
    # Exiting at the deadline itself could precede the platform's timeout
    # record, and the shared validator requires timeout <= task exit.
    held_deadline_gate(trace, monkeypatch, seconds_past_deadline=0.1)
    trace.released = Mock(side_effect=[False, False, True])
    trace.gate("fault", effects=True)
    phases = [call.args[0] for call in trace.emit.call_args_list]
    assert "IO_INTERRUPTED" not in phases
    assert phases[-1] == "IO_EXIT"


def test_preparation_gate_is_not_cancelled_by_the_deadline(trace, monkeypatch):
    # Preparation holds the healthy cohort before the fault is admitted; only
    # the fault itself may be cut short by the deadline.
    held_deadline_gate(trace, monkeypatch, seconds_past_deadline=5)
    trace.released = Mock(side_effect=[False, False, True])
    trace.gate("prepare", preparation=True)
    phases = [call.args[0] for call in trace.emit.call_args_list]
    assert "IO_INTERRUPTED" not in phases


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
