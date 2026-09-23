import json
import io
from unittest.mock import Mock

import boto3
from botocore.stub import ANY, Stubber
from botocore.exceptions import ClientError
import pytest

from lmi_tests import cloud as module
from lmi_tests.evidence import CollectionError
from lmi_tests.summary import summarize


@pytest.fixture
def driver(monkeypatch, tmp_path):
    lam = boto3.client(
        "lambda",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    monkeypatch.setattr(module, "client", lambda _: lam)
    monkeypatch.setattr(module, "ARTIFACTS", tmp_path)
    # CloudRunner.__init__ normally discovers credentials; the real client below
    # is still used for invoke serialization/validation through botocore Stubber.
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python_testing.runner.boto3.client",
        lambda *a, **kw: lam,
    )
    driver = module.Cloud(
        {
            "functions": {
                "normal": "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
            },
            "region": "us-west-2",
            "run": "unit",
            "bucket": "unit-bucket",
            "commit": "commit",
        }
    )
    driver.s3 = Mock()
    return driver


def test_async_cloud_runner_sends_json_to_qualified_lmi_target(driver):
    arn = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED/durable-execution/name/id"
    with Stubber(driver.lam) as stub:
        stub.add_response(
            "invoke",
            {"StatusCode": 202, "DurableExecutionArn": arn},
            {
                "FunctionName": driver.manifest["functions"]["normal"],
                "InvocationType": "Event",
                "Payload": ANY,
            },
        )
        item = driver.start("success")
        assert item["arn"] == arn
        stub.assert_no_pending_responses()


def test_missing_execution_arn_is_collection_error_not_pass(driver):
    with Stubber(driver.lam) as stub:
        stub.add_response("invoke", {"StatusCode": 202})
        with pytest.raises(CollectionError, match="API failed"):
            driver.start("success")


@pytest.mark.parametrize(
    "message", ["Runtime.Timeout", "Task timed out after 10 seconds"]
)
def test_requires_request_specific_service_timeout(driver, message):
    good = {
        "InvocationCompletedDetails": {
            "RequestId": "request",
            "Error": {"Payload": message},
        }
    }
    driver.history = Mock(return_value=[good])
    driver.poll = lambda predicate, **_: (
        predicate() or (_ for _ in ()).throw(CollectionError())
    )
    assert driver.platform_timeout({}, "request") == good
    for events in (
        [{"EventType": "ExecutionTimedOut"}],
        [
            {
                "InvocationCompletedDetails": {
                    "RequestId": "different",
                    "Error": {"Payload": message},
                }
            }
        ],
    ):
        driver.history.return_value = events
        with pytest.raises(CollectionError):
            driver.platform_timeout({}, "request")


def test_summary_never_claims_unexecuted_or_skipped_cloud_success(tmp_path):
    assert "did not run" in summarize(tmp_path)
    (tmp_path / "cloud.xml").write_text(
        '<testsuites><testsuite><testcase name="placement"><properties><property name="lmi_outcome" value="PlacementError"/></properties><failure/></testcase><testcase name="not-run"><skipped/></testcase></testsuite></testsuites>'
    )
    summary = summarize(tmp_path)
    assert "placement: PlacementError" in summary
    assert "not-run: SKIPPED" in summary
    assert "PASS" not in summary
    (tmp_path / "deploy-error.json").write_text(
        json.dumps({"type": "ProvisioningError"})
    )
    assert "ProvisioningError" in summarize(tmp_path)


@pytest.mark.parametrize(
    "scenario", ["parallel", "return-inflight", "failure-inflight", "late-operation"]
)
def test_controls_exist_before_invocation_and_shared_barriers_are_not_reset(
    driver, scenario
):
    driver.lam.invoke = Mock(
        return_value={"StatusCode": 202, "DurableExecutionArn": "arn"}
    )
    calls = []
    driver.s3.put_object.side_effect = lambda **kw: calls.append(
        ("hold", kw["Key"], kw["Body"])
    )
    driver.lam.invoke.side_effect = lambda **_: (
        calls.append(("invoke",)) or {"StatusCode": 202, "DurableExecutionArn": "arn"}
    )
    item = driver.start(scenario)
    assert calls == [
        ("hold", "control/" + item["marker"] + "-loser", b"hold"),
        ("hold", "control/" + item["marker"] + "-loser-started", b"hold"),
        ("invoke",),
    ]
    calls.clear()
    driver.start("barrier", gate="shared")
    driver.start("barrier", gate="shared")
    assert calls == [("hold", "control/shared", b"hold"), ("invoke",), ("invoke",)]
    assert "release-all" not in driver.gates


def test_finally_control_is_initialized_before_invocation(driver):
    calls = []
    driver.s3.put_object.side_effect = lambda **kw: calls.append(
        ("hold", kw["Key"], kw["Body"])
    )
    driver.lam.invoke = Mock(
        side_effect=lambda **_: (
            calls.append(("invoke",))
            or {"StatusCode": 202, "DurableExecutionArn": "arn"}
        )
    )
    item = driver.start("suspend-cleanup")
    assert calls == [
        ("hold", "control/" + item["marker"] + "-cleanup", b"hold"),
        ("invoke",),
    ]


def test_polling_reads_only_current_case_and_surfaces_control_errors(driver):
    driver.invocations = [{"marker": "current"}]
    event = {
        "run": "unit",
        "commit": "commit",
        "marker": "current",
        "request": "r",
        "sequence": 1,
        "time": 1,
        "phase": "CONTROL_ERROR",
        "gate": "g",
        "error": "403 Forbidden",
    }
    key = "events/current/r/000001.json"
    driver.s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": key}]}
    ]
    driver.s3.get_object.side_effect = lambda **_: {
        "Body": io.BytesIO(json.dumps(event).encode())
    }
    with pytest.raises(CollectionError, match="403"):
        driver.refresh()
    driver.s3.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="unit-bucket", Prefix="events/current/"
    )
    driver.s3.get_object.assert_called_once_with(Bucket="unit-bucket", Key=key)
    # Full diagnostic collection preserves errors without turning itself into a
    # second failing test or omitting evidence from earlier cases.
    assert driver.refresh(markers=[]) == [event]
    driver.s3.get_paginator.return_value.paginate.assert_called_with(
        Bucket="unit-bucket", Prefix="events/"
    )


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def throttle_error():
    return ClientError(
        {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
        "GetDurableExecutionHistory",
    )


def test_history_retries_preserve_pages_and_pace_requests(driver, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    driver.lam.get_durable_execution_history = Mock(
        side_effect=[
            throttle_error(),
            {"Events": [{"EventId": 1}], "NextMarker": "page2"},
            {"Events": [{"EventId": 2}]},
        ]
    )
    assert driver.history({"arn": "execution", "marker": "case"}) == [
        {"EventId": 1},
        {"EventId": 2},
    ]
    calls = driver.lam.get_durable_execution_history.call_args_list
    assert calls[0] == calls[1]
    assert calls[2].kwargs["Marker"] == "page2"
    assert clock.now >= 2


def test_history_throttling_has_a_finite_budget(driver, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    driver.lam.get_durable_execution_history = Mock(side_effect=throttle_error())
    with pytest.raises(CollectionError, match="throttle budget"):
        driver.history_page(DurableExecutionArn="execution")
    assert driver.lam.get_durable_execution_history.call_count == 5
    assert clock.now < 20


def test_history_does_not_retry_permission_errors_or_invocations(driver, monkeypatch):
    monkeypatch.setattr(module, "time", Clock())
    driver.lam.invoke = Mock(
        return_value={"StatusCode": 202, "DurableExecutionArn": "execution"}
    )
    item = driver.start("success")
    error = ClientError(
        {"Error": {"Code": "AccessDeniedException"}}, "GetDurableExecutionHistory"
    )
    driver.lam.get_durable_execution_history = Mock(side_effect=error)
    with pytest.raises(ClientError):
        item["runner"].lambda_client.get_durable_execution_history(
            DurableExecutionArn="execution"
        )
    driver.lam.invoke.assert_called_once()
    driver.lam.get_durable_execution_history.assert_called_once()


def test_case_collection_is_scoped_but_final_collection_is_complete(driver):
    driver.invocations = [{"arn": "execution", "marker": "case"}]
    driver.refresh = Mock(return_value=[{"execution": "execution", "marker": "case"}])
    driver.history = Mock(return_value=[])
    driver.lam.get_durable_execution = Mock(return_value={"Status": "SUCCEEDED"})
    driver.logs = Mock()
    driver.collect(full_run=False)
    driver.refresh.assert_called_once_with(markers=["case"], validate_controls=False)
    driver.history.assert_called_once()
    driver.logs.get_paginator.assert_not_called()
    driver.manifest["created"] = 0
    driver.lam.get_function_configuration = Mock(
        return_value={"LoggingConfig": {"LogGroup": "group"}}
    )
    driver.logs.get_paginator.return_value.paginate.return_value = []
    driver.collect()
    driver.refresh.assert_called_with(markers=[], validate_controls=False)
    driver.logs.get_paginator.assert_called_once_with("filter_log_events")
