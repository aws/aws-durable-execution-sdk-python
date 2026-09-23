import json
from unittest.mock import Mock

import boto3
from botocore.stub import ANY, Stubber
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
        }
    )
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
