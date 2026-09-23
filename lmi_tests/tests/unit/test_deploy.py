import copy
import json

from botocore.exceptions import ClientError
from unittest.mock import Mock

import pytest

from lmi_tests import deploy
from lmi_tests.evidence import ProvisioningError


@pytest.fixture
def manifest():
    return {
        "run": "unit",
        "stack": "python-lmi-e2e",
        "bucket": "test-bucket",
        "role": "role",
        "provider": "provider",
        "runtime": "python3.14",
        "concurrencies": deploy.CONCURRENCIES,
        "functions": {},
        "commit": "sha",
        "codeKey": "code/sha.zip",
        "codeSha256": "hash",
        "owned": True,
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }


def test_one_deployment_shares_two_functions_across_all_scenarios(manifest):
    runtime = "python3.14"
    template = deploy.template(manifest)
    functions = {
        key: resource
        for key, resource in template["Resources"].items()
        if resource["Type"] == "AWS::Lambda::Function"
    }
    assert set(functions) == {"c1Function", "c2Function"}
    assert (
        functions["c1Function"]["Properties"]["Code"]
        == functions["c2Function"]["Properties"]["Code"]
    )
    for key, concurrency in deploy.CONCURRENCIES.items():
        config = template["Resources"][key + "Function"]["Properties"]
        assert config["FunctionScalingConfig"] == deploy.SCALING
        assert config["Runtime"] == runtime
        assert (
            config["CapacityProviderConfig"][
                "LambdaManagedInstancesCapacityProviderConfig"
            ]["PerExecutionEnvironmentMaxConcurrency"]
            == concurrency
        )
        assert config["DurableConfig"]["ExecutionTimeout"] == 240
        assert config["Timeout"] == 60
        assert config["Handler"] == "lmi_tests.fixture.handler"
        assert (
            template["Outputs"][key]["Value"]["Fn::Join"][1][-1] == ":$LATEST.PUBLISHED"
        )
    assert not any(
        r["Type"] in {"AWS::Lambda::Version", "AWS::Lambda::CapacityProvider"}
        for r in template["Resources"].values()
    )


def test_verify_rejects_standard_lambda_wrong_artifact_and_unbounded_placement(
    manifest,
):
    config = copy.deepcopy(
        deploy.template(manifest)["Resources"]["c1Function"]["Properties"]
    )
    config.update(State="Active", Version=deploy.QUALIFIER, CodeSha256="hash")
    scaling = {"AppliedFunctionScalingConfig": deploy.SCALING}
    deploy.verify(config, scaling, manifest, "c1")
    with pytest.raises(ProvisioningError):
        deploy.verify(config, scaling, manifest, "c2")
    for field in (
        "CapacityProviderConfig",
        "CodeSha256",
        "Runtime",
        "Timeout",
        "DurableConfig",
        "Environment",
    ):
        bad = copy.deepcopy(config)
        del bad[field]
        with pytest.raises(ProvisioningError):
            deploy.verify(bad, scaling, manifest, "c1")
    with pytest.raises(ProvisioningError):
        deploy.verify(
            config,
            {"AppliedFunctionScalingConfig": {"MaxExecutionEnvironments": 2}},
            manifest,
            "c1",
        )


def test_cleanup_refuses_foreign_owner_and_failed_create(manifest, monkeypatch):
    cfn = Mock()
    cfn.describe_stacks.return_value = {
        "Stacks": [{"Tags": [{"Key": "Suite", "Value": "other"}]}]
    }
    monkeypatch.setattr(deploy, "client", lambda _: cfn)
    with pytest.raises(ProvisioningError, match="Refusing"):
        deploy.cleanup(manifest)
    cfn.delete_stack.assert_not_called()
    deploy.cleanup({**manifest, "owned": False})
    cfn.delete_stack.assert_not_called()


def stack(manifest, status="UPDATE_COMPLETE"):
    return {
        "StackStatus": status,
        "Tags": [
            {"Key": "Suite", "Value": deploy.OWNER},
            {"Key": "Stack", "Value": manifest["stack"]},
            {"Key": "Persistent", "Value": "true"},
        ],
        "Outputs": [
            {
                "OutputKey": key,
                "OutputValue": f"arn:aws:lambda:us-west-2:123456789012:function:{manifest['stack']}-{key}:$LATEST.PUBLISHED",
            }
            for key in deploy.CONCURRENCIES
        ],
    }


@pytest.fixture
def apis(monkeypatch, tmp_path):
    clients = {name: Mock() for name in ("cloudformation", "s3", "lambda")}
    monkeypatch.setattr(deploy, "client", clients.__getitem__)
    monkeypatch.setattr(deploy, "ARTIFACTS", tmp_path)
    return clients


def test_later_run_updates_fixed_functions_without_recreating_resources(
    manifest, apis, monkeypatch
):
    cfn, lam = apis["cloudformation"], apis["lambda"]
    monkeypatch.setattr(
        deploy, "owned_stack", Mock(side_effect=[None, stack(manifest)])
    )
    monkeypatch.setattr(deploy, "wait_stack", Mock(return_value=stack(manifest)))
    idle = Mock()
    monkeypatch.setattr(deploy, "wait_for_idle_functions", idle)
    verify = Mock()
    monkeypatch.setattr(deploy, "verify", verify)
    deploy.deploy_stack(manifest, b"first-code")
    second = {
        **manifest,
        "run": "second",
        "codeKey": "code/new-digest.zip",
        "functions": {},
    }
    deploy.deploy_stack(second, b"second-code")
    cfn.create_stack.assert_called_once()
    assert cfn.update_stack.call_count == 2
    templates = [
        json.loads(call.kwargs["TemplateBody"])
        for call in cfn.update_stack.call_args_list
    ]
    assert templates[0]["Outputs"] == templates[1]["Outputs"]
    assert templates[0]["Resources"].keys() == templates[1]["Resources"].keys()
    for key in deploy.CONCURRENCIES:
        before = templates[0]["Resources"][key + "Function"]["Properties"]
        after = templates[1]["Resources"][key + "Function"]["Properties"]
        assert before["FunctionName"] == after["FunctionName"]
        assert before["Code"]["S3Key"] != after["Code"]["S3Key"]
        assert after["Environment"]["Variables"]["LMI_RUN_ID"] == "second"
    idle.assert_called_once()
    assert verify.call_count == 4
    assert lam.get_function_configuration.call_count == 4
    cfn.delete_stack.assert_not_called()
    lam.delete_function.assert_not_called()


def test_unchanged_update_still_verifies_functions(manifest, apis, monkeypatch):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.update_stack.side_effect = ClientError(
        {
            "Error": {
                "Code": "ValidationError",
                "Message": "No updates are to be performed.",
            }
        },
        "UpdateStack",
    )
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    wait = Mock()
    monkeypatch.setattr(deploy, "wait_stack", wait)
    verify = Mock()
    monkeypatch.setattr(deploy, "verify", verify)
    deploy.deploy_stack(manifest, b"code")
    wait.assert_not_called()
    assert verify.call_count == 2
    cfn.create_stack.assert_not_called()


@pytest.mark.parametrize(
    "status", ["ROLLBACK_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"]
)
def test_failed_or_busy_persistent_stack_is_retained(manifest, apis, status):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest, status)]}
    with pytest.raises(ProvisioningError, match="retained"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    cfn.delete_stack.assert_not_called()
    apis["s3"].put_object.assert_not_called()


def test_foreign_stack_cannot_be_adopted(manifest, apis):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [{"Tags": []}]}
    with pytest.raises(ProvisioningError, match="not owned"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    cfn.create_stack.assert_not_called()


def test_failed_creation_does_not_trigger_deletion(manifest, apis):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "Stack does not exist"}},
        "DescribeStacks",
    )
    cfn.create_stack.side_effect = RuntimeError("create failed")
    with pytest.raises(RuntimeError, match="create failed"):
        deploy.deploy_stack(manifest, b"code")
    cfn.delete_stack.assert_not_called()


def test_prior_executions_must_finish_before_code_updates(manifest, apis, monkeypatch):
    lam = apis["lambda"]
    one_function = {"Outputs": stack(manifest)["Outputs"][:1]}
    lam.get_paginator.return_value.paginate.side_effect = [
        [{"DurableExecutions": [{"DurableExecutionArn": "old"}]}],
        [{"DurableExecutions": []}],
    ]
    sleep = Mock()
    monkeypatch.setattr(deploy.time, "sleep", sleep)
    deploy.wait_for_idle_functions(one_function)
    sleep.assert_called_once_with(2)
    lam.stop_durable_execution.assert_not_called()
    lam.get_paginator.return_value.paginate.assert_called_with(
        FunctionName=one_function["Outputs"][0]["OutputValue"].rsplit(":", 1)[0],
        Statuses=["RUNNING"],
    )


@pytest.mark.parametrize(
    "page",
    [
        {"DurableExecutions": [{"DurableExecutionArn": "old"}]},
        {"DurableExecutions": [], "NextMarker": "more"},
    ],
)
def test_busy_or_incompletely_listed_functions_block_updates(
    manifest, apis, monkeypatch, page
):
    apis["lambda"].get_paginator.return_value.paginate.return_value = [page]
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    wait = deploy.wait_for_idle_functions
    monkeypatch.setattr(
        deploy, "wait_for_idle_functions", lambda value: wait(value, seconds=0)
    )
    with pytest.raises(ProvisioningError, match="code was not updated"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    apis["s3"].put_object.assert_not_called()
    apis["lambda"].stop_durable_execution.assert_not_called()


def test_only_run_data_expires_and_code_is_retained(manifest):
    rules = deploy.template(manifest)["Resources"]["Bucket"]["Properties"][
        "LifecycleConfiguration"
    ]["Rules"]
    assert all(rule.get("Prefix") == "runs/" for rule in rules)
    assert all(rule["ExpirationInDays"] == 1 for rule in rules)


def test_cleanup_releases_only_current_run_and_keeps_infrastructure(
    manifest, apis, monkeypatch, tmp_path
):
    from lmi_tests import cloud

    cfn, s3, lam = apis["cloudformation"], apis["s3"], apis["lambda"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.describe_stack_resources.return_value = {
        "StackResources": [
            {
                "ResourceType": "AWS::S3::Bucket",
                "PhysicalResourceId": manifest["bucket"],
                "ResourceStatus": "CREATE_COMPLETE",
            }
        ]
    }
    driver = Mock()
    driver.run_invocations.return_value = [{"arn": "this-run"}]
    monkeypatch.setattr(cloud, "Cloud", Mock(return_value=driver))
    deploy.cleanup(manifest)
    s3.put_object.assert_called_once_with(
        Bucket=manifest["bucket"], Key="runs/unit/control/release-all", Body=b"release"
    )
    assert driver.invocations == [{"arn": "this-run"}]
    driver.settle_case.assert_called_once()
    cfn.delete_stack.assert_not_called()
    lam.delete_function.assert_not_called()
    s3.delete_objects.assert_not_called()
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "RETAINED"
