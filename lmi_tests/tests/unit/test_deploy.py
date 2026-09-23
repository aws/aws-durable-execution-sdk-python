import copy
from unittest.mock import Mock

import pytest

from lmi_tests import deploy
from lmi_tests.evidence import ProvisioningError


@pytest.fixture
def manifest():
    return {
        "run": "unit",
        "stack": "py-lmi-unit-313-c2",
        "bucket": "test-bucket",
        "role": "role",
        "provider": "provider",
        "runtime": "python3.13",
        "concurrency": 2,
        "commit": "sha",
        "codeKey": "code/sha.zip",
        "codeSha256": "hash",
        "owned": True,
        "invocationTimeout": 60,
        "deadlineTimeout": 10,
        "executionTimeout": 240,
    }


@pytest.mark.parametrize("runtime", ["python3.13", "python3.14"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_template_sets_native_scaling_durability_and_published_target(
    manifest, runtime, concurrency
):
    manifest.update(runtime=runtime, concurrency=concurrency)
    template = deploy.template(manifest)
    for key in ("normal", "deadline"):
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
        assert config["Timeout"] == (10 if key == "deadline" else 60)
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
        deploy.template(manifest)["Resources"]["normalFunction"]["Properties"]
    )
    config.update(State="Active", Version=deploy.QUALIFIER, CodeSha256="hash")
    scaling = {"AppliedFunctionScalingConfig": deploy.SCALING}
    deploy.verify(config, scaling, manifest, "normal")
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
            deploy.verify(bad, scaling, manifest, "normal")
    with pytest.raises(ProvisioningError):
        deploy.verify(
            config,
            {"AppliedFunctionScalingConfig": {"MaxExecutionEnvironments": 2}},
            manifest,
            "normal",
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


def test_cleanup_releases_stops_and_retires_before_bucket_deletion(
    manifest, monkeypatch
):
    calls = []
    cfn, s3, lam = Mock(), Mock(), Mock()
    monkeypatch.setattr(
        deploy,
        "client",
        lambda name: {"cloudformation": cfn, "s3": s3, "lambda": lam}[name],
    )
    monkeypatch.setattr(deploy, "owned_stack", lambda *_: {})
    monkeypatch.setattr(deploy, "wait_stack", lambda *_: calls.append("stack-gone"))
    monkeypatch.setattr(deploy, "save", lambda *_: None)
    cfn.describe_stack_resources.return_value = {
        "StackResources": [
            {
                "ResourceType": "AWS::S3::Bucket",
                "PhysicalResourceId": manifest["bucket"],
                "ResourceStatus": "CREATE_COMPLETE",
            },
            {
                "ResourceType": "AWS::Lambda::Function",
                "PhysicalResourceId": "function",
                "ResourceStatus": "CREATE_COMPLETE",
            },
        ]
    }
    s3.put_object.side_effect = lambda **_: calls.append("release")
    lam.get_paginator.return_value.paginate.return_value = [
        {"DurableExecutions": [{"DurableExecutionArn": "execution"}]}
    ]
    lam.stop_durable_execution.side_effect = lambda **_: calls.append("stop")
    lam.delete_function.side_effect = lambda **_: calls.append("retire")
    s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "evidence"}]}
    ]
    s3.delete_objects.side_effect = lambda **_: calls.append("empty") or {}
    cfn.delete_stack.side_effect = lambda **_: calls.append("delete-stack")
    deploy.cleanup(manifest)
    assert calls == ["release", "stop", "retire", "empty", "delete-stack", "stack-gone"]


def test_reconcile_only_deletes_expired_suite_owned_runs(monkeypatch):
    cfn = Mock()
    monkeypatch.setattr(deploy, "client", lambda _: cfn)
    cfn.get_caller_identity.return_value = {"Account": "123"}

    def stack(name, owner, expires):
        return {
            "StackName": name,
            "Tags": [
                {"Key": "Suite", "Value": owner},
                {"Key": "RunId", "Value": "run"},
                {"Key": "Expires", "Value": expires},
            ],
        }

    cfn.get_paginator.return_value.paginate.return_value = [
        {
            "Stacks": [
                stack("py-lmi-old", deploy.OWNER, "1"),
                stack("py-lmi-active", deploy.OWNER, "9999999999"),
                stack("py-lmi-other", "other", "1"),
                stack("unrelated", deploy.OWNER, "1"),
            ]
        }
    ]
    cleanup = Mock()
    monkeypatch.setattr(deploy, "cleanup", cleanup)
    deploy.reconcile()
    assert cleanup.call_count == 1
    assert cleanup.call_args.args[0]["stack"] == "py-lmi-old"
