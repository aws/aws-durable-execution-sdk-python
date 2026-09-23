# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Build, provision, verify, collect, and retire run-owned LMI resources."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from lmi_tests.evidence import ProvisioningError


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "lmi_tests/artifacts"
OWNER = "python-sdk-lmi-e2e"
SCALING = {"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1}
QUALIFIER = "$LATEST.PUBLISHED"


def client(service, region=None):
    return boto3.client(
        service,
        region_name=region or os.environ["AWS_REGION"],
        config=Config(
            connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2}
        ),
    )


def scrub(value):
    if isinstance(value, dict):
        return {
            k: "<redacted>" if k in {"CheckpointToken", "CallbackId"} else scrub(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [scrub(v) for v in value]
    return value


def save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(scrub(data), indent=2, default=str) + "\n")


def build():
    directory = ROOT / "lmi_tests/build"
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir()
    # Hatch builds this checkout's wheel; pip resolves only its dependencies.
    subprocess.run(
        ["hatch", "build", "-t", "wheel", str(directory)],
        cwd=ROOT / "packages/aws-durable-execution-sdk-python",
        env={k: v for k, v in os.environ.items() if k != "HATCH_ENV_ACTIVE"},
        check=True,
    )
    wheel = next(directory.glob("*.whl"))
    target = directory / "package"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--only-binary=:all:",
            "--no-compile",
            "--target",
            str(target),
            str(wheel),
        ],
        check=True,
    )
    (target / "lmi_tests").mkdir()
    for name in ("__init__.py", "fixture.py"):
        shutil.copy(ROOT / "lmi_tests" / name, target / "lmi_tests" / name)
    artifact = directory / "function.zip"
    with zipfile.ZipFile(artifact, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(target.rglob("*")):
            if path.is_file():
                info = zipfile.ZipInfo(path.relative_to(target).as_posix())
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, path.read_bytes())
    save(
        ARTIFACTS / "build.json",
        {
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "wheel": wheel.name,
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        },
    )


def template(manifest, functions=True):
    bucket = manifest["bucket"]
    resources = {
        "Bucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                "BucketName": bucket,
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                    ]
                },
                "LifecycleConfiguration": {
                    "Rules": [
                        {
                            "Id": "expiry",
                            "Status": "Enabled",
                            "ExpirationInDays": 2,
                            "AbortIncompleteMultipartUpload": {
                                "DaysAfterInitiation": 1
                            },
                        }
                    ]
                },
            },
        },
        "BucketPolicy": {
            "Type": "AWS::S3::BucketPolicy",
            "Properties": {
                "Bucket": {"Ref": "Bucket"},
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": manifest["role"]},
                            "Action": ["s3:GetObject", "s3:PutObject"],
                            "Resource": [
                                f"arn:aws:s3:::{bucket}/events/*",
                                f"arn:aws:s3:::{bucket}/control/*",
                            ],
                        },
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:*",
                            "Resource": [
                                f"arn:aws:s3:::{bucket}",
                                f"arn:aws:s3:::{bucket}/*",
                            ],
                            "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                        },
                    ],
                },
            },
        },
    }
    outputs = {}
    if functions:
        for key, timeout in (
            ("normal", manifest["invocationTimeout"]),
            ("deadline", manifest["deadlineTimeout"]),
        ):
            name = manifest["stack"] + "-" + key
            resources[key + "Logs"] = {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {
                    "LogGroupName": "/aws/lambda/" + name,
                    "RetentionInDays": 1,
                },
            }
            resources[key + "Function"] = {
                "Type": "AWS::Lambda::Function",
                "DependsOn": "BucketPolicy",
                "Properties": {
                    "FunctionName": name,
                    "Runtime": manifest["runtime"],
                    "Architectures": ["arm64"],
                    "Handler": "lmi_tests.fixture.handler",
                    "Role": manifest["role"],
                    "MemorySize": 2048,
                    "Timeout": timeout,
                    "Code": {"S3Bucket": bucket, "S3Key": manifest["codeKey"]},
                    "FunctionScalingConfig": SCALING,
                    "DurableConfig": {
                        "ExecutionTimeout": manifest["executionTimeout"],
                        "RetentionPeriodInDays": 1,
                    },
                    "CapacityProviderConfig": {
                        "LambdaManagedInstancesCapacityProviderConfig": {
                            "CapacityProviderArn": manifest["provider"],
                            "PerExecutionEnvironmentMaxConcurrency": manifest[
                                "concurrency"
                            ],
                            "ExecutionEnvironmentMemoryGiBPerVCpu": 2,
                        }
                    },
                    "Environment": {
                        "Variables": {
                            "LMI_BUCKET": bucket,
                            "LMI_COMMIT": manifest["commit"],
                            "LMI_RUN_ID": manifest["run"],
                            "AWS_RETRY_MODE": "standard",
                        }
                    },
                    "LoggingConfig": {
                        "LogFormat": "JSON",
                        "ApplicationLogLevel": "INFO",
                        "SystemLogLevel": "INFO",
                        "LogGroup": {"Ref": key + "Logs"},
                    },
                },
            }
            # LMI publishes this qualified version automatically, including its
            # scaling settings. An extra numbered version would allocate more capacity.
            outputs[key] = {
                "Value": {
                    "Fn::Join": [
                        "",
                        [{"Fn::GetAtt": [key + "Function", "Arn"]}, ":" + QUALIFIER],
                    ]
                }
            }
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": resources,
        "Outputs": outputs,
    }


def wait_stack(cfn, name, expected, seconds=1500):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = cfn.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError as error:
            if expected == "DELETE_COMPLETE" and "does not exist" in str(error):
                return None
            raise
        status = result["StackStatus"]
        if status == expected:
            return result
        if "FAILED" in status or "ROLLBACK" in status:
            save(
                ARTIFACTS / "stack-events.json",
                cfn.describe_stack_events(StackName=name),
            )
            raise ProvisioningError(f"{name}: {status}; no fallback to standard Lambda")
        time.sleep(3)
    raise ProvisioningError(f"{name}: provisioning/retirement deadline exceeded")


def verify(config, scaling, manifest, key):
    actual = config.get("CapacityProviderConfig", {}).get(
        "LambdaManagedInstancesCapacityProviderConfig", {}
    )
    checks = [
        actual.get("CapacityProviderArn") == manifest["provider"],
        actual.get("PerExecutionEnvironmentMaxConcurrency") == manifest["concurrency"],
        config.get("Runtime") == manifest["runtime"],
        config.get("Architectures") == ["arm64"],
        config.get("Version") == QUALIFIER,
        config.get("CodeSha256") == manifest["codeSha256"],
        config.get("MemorySize") == 2048,
        config.get("State") == "Active",
        config.get("DurableConfig", {}).get("ExecutionTimeout")
        == manifest["executionTimeout"],
        config.get("Timeout")
        == manifest["deadlineTimeout" if key == "deadline" else "invocationTimeout"],
        scaling.get("AppliedFunctionScalingConfig") == SCALING,
        config.get("Environment", {}).get("Variables", {}).get("LMI_COMMIT")
        == manifest["commit"],
        config.get("Environment", {}).get("Variables", {}).get("LMI_RUN_ID")
        == manifest["run"],
    ]
    if not all(checks):
        raise ProvisioningError(
            f"{key}: LMI configuration/artifact/scaling readback mismatch"
        )


def deploy(args):
    if not args.run_id or not re.fullmatch(r"[a-z0-9-]{1,24}", args.run_id):
        raise ProvisioningError(
            "run-id must be 1-24 lowercase letters, digits or hyphens"
        )
    provider = os.environ["CAPACITY_PROVIDER_ARN"]
    account = client("sts").get_caller_identity()["Account"]
    if account != os.environ["TEST_ACCOUNT_ID"] or provider.split(":")[3:5] != [
        os.environ["AWS_REGION"],
        account,
    ]:
        raise ProvisioningError(
            "Authenticated account/region must match the test account and capacity provider"
        )
    capacity = client("lambda").get_capacity_provider(
        CapacityProviderName=provider.rsplit(":", 1)[-1].split("/")[-1]
    )
    save(ARTIFACTS / "capacity-provider.json", capacity)
    provider_config = capacity["CapacityProvider"]
    if (
        not 2
        <= provider_config.get("CapacityProviderScalingConfig", {}).get(
            "MaxVCpuCount", 0
        )
        <= 128
    ):
        raise ProvisioningError(
            "Use a bounded, test-owned capacity provider (2-128 maximum vCPUs)"
        )
    data = (ROOT / "lmi_tests/build/function.zip").read_bytes()
    digest = hashlib.sha256(data).digest()
    built = json.loads((ARTIFACTS / "build.json").read_text())
    if built["sha256"] != digest.hex():
        raise ProvisioningError("Artifact changed since build")
    suffix = (
        args.runtime.replace("python", "").replace(".", "") + f"-c{args.concurrency}"
    )
    name = f"py-lmi-{args.run_id}-{suffix}"
    manifest = {
        "run": args.run_id,
        "stack": name,
        "bucket": name + "-" + account,
        "account": account,
        "region": os.environ["AWS_REGION"],
        "provider": provider,
        "role": os.environ["TEST_LAMBDA_EXECUTION_ROLE_ARN"],
        "runtime": args.runtime,
        "concurrency": args.concurrency,
        "commit": built["commit"],
        "codeKey": "code/" + digest.hex() + ".zip",
        "codeSha256": base64.b64encode(digest).decode(),
        "invocationTimeout": 60,
        "deadlineTimeout": 10,
        "executionTimeout": 240,
        "driverTimeout": 120,
        "cleanupGrace": 5,
        "created": time.time(),
        "expires": int(time.time()) + 4 * 3600,
        "functions": {},
    }
    save(
        ARTIFACTS / "manifest.json", manifest
    )  # enough to clean up partial provisioning
    cfn = client("cloudformation")
    tags = [
        {"Key": "Suite", "Value": OWNER},
        {"Key": "RunId", "Value": args.run_id},
        {"Key": "Expires", "Value": str(manifest["expires"])},
    ]
    # Create only. A collision cannot take over another run or mutate shared fixtures.
    cfn.create_stack(
        StackName=name, TemplateBody=json.dumps(template(manifest, False)), Tags=tags
    )
    manifest["owned"] = True
    save(ARTIFACTS / "manifest.json", manifest)
    wait_stack(cfn, name, "CREATE_COMPLETE", 180)
    client("s3").put_object(
        Bucket=manifest["bucket"], Key=manifest["codeKey"], Body=data
    )
    client("s3").put_object(
        Bucket=manifest["bucket"], Key="control/release-all", Body=b"hold"
    )
    cfn.update_stack(
        StackName=name, TemplateBody=json.dumps(template(manifest)), Tags=tags
    )
    stack = wait_stack(cfn, name, "UPDATE_COMPLETE")
    for output in stack["Outputs"]:
        key, arn = output["OutputKey"], output["OutputValue"]
        manifest["functions"][key] = arn
        save(ARTIFACTS / "manifest.json", manifest)
        config = client("lambda").get_function_configuration(FunctionName=arn)
        scaling = client("lambda").get_function_scaling_config(
            FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER
        )
        save(
            ARTIFACTS / f"configuration/{key}.json",
            {"function": config, "scaling": scaling},
        )
        verify(config, scaling, manifest, key)
    save(
        ARTIFACTS / "function-name-map.json",
        {"lmi_tests.fixture.handler": manifest["functions"]["normal"]},
    )


def owned_stack(cfn, manifest):
    try:
        stack = cfn.describe_stacks(StackName=manifest["stack"])["Stacks"][0]
    except ClientError as error:
        if "does not exist" in str(error):
            return None
        raise
    tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
    if (
        tags.get("Suite") != OWNER
        or tags.get("RunId") != manifest["run"]
        or not manifest["stack"].startswith("py-lmi-")
    ):
        raise ProvisioningError(
            "Refusing to delete resources not owned by this suite/run"
        )
    return stack


def cleanup(manifest):
    if not manifest.get("owned"):
        return  # create failed (including a name collision): do not delete that stack
    cfn, s3, lam = client("cloudformation"), client("s3"), client("lambda")
    stack = owned_stack(cfn, manifest)
    if stack is None:
        return
    resources = cfn.describe_stack_resources(StackName=manifest["stack"])[
        "StackResources"
    ]
    bucket_exists = any(
        r["ResourceType"] == "AWS::S3::Bucket"
        and r.get("PhysicalResourceId") == manifest["bucket"]
        and r["ResourceStatus"] != "DELETE_COMPLETE"
        for r in resources
    )
    if bucket_exists:
        s3.put_object(
            Bucket=manifest["bucket"], Key="control/release-all", Body=b"release"
        )
    errors = []
    stop_errors = []
    for resource in resources:
        if (
            resource["ResourceType"] != "AWS::Lambda::Function"
            or resource["ResourceStatus"] == "DELETE_COMPLETE"
        ):
            continue
        try:
            for page in lam.get_paginator(
                "list_durable_executions_by_function"
            ).paginate(
                FunctionName=resource["PhysicalResourceId"], Statuses=["RUNNING"]
            ):
                for execution in page.get("DurableExecutions", []):
                    lam.stop_durable_execution(
                        DurableExecutionArn=execution["DurableExecutionArn"]
                    )
        except ClientError as error:
            stop_errors.append(str(error))
        try:
            # Delete functions BEFORE emptying the bucket. This is the retirement
            # path; stopping a logical execution alone cannot stop LMI Python code.
            lam.delete_function(FunctionName=resource["PhysicalResourceId"])
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                errors.append(str(error))
    if errors:
        raise ProvisioningError("Function retirement failed: " + "; ".join(errors))
    if stop_errors:
        save(ARTIFACTS / "cleanup-stop-errors.json", stop_errors)
    # Bounded retries account for already in-flight diagnostic PUTs during retirement.
    for _attempt in range(6):
        if bucket_exists:
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=manifest["bucket"]
            ):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    deleted = s3.delete_objects(
                        Bucket=manifest["bucket"], Delete={"Objects": keys}
                    )
                    if deleted.get("Errors"):
                        raise ProvisioningError(
                            "Could not empty run-owned bucket: "
                            + str(deleted["Errors"])
                        )
        cfn.delete_stack(StackName=manifest["stack"])
        try:
            wait_stack(cfn, manifest["stack"], "DELETE_COMPLETE", 180)
            save(
                ARTIFACTS / "cleanup.json",
                {"stack": manifest["stack"], "status": "DELETED"},
            )
            return
        except ProvisioningError:
            # A non-bucket failure stays visible after this finite retry budget.
            time.sleep(2)
    raise ProvisioningError(
        "Run-owned stack retirement failed; use reconcile after investigating stack-events.json"
    )


def reconcile():
    """Only expired, suite-tagged stacks; never mutate/delete the shared provider."""
    cfn = client("cloudformation")
    for page in cfn.get_paginator("describe_stacks").paginate():
        for stack in page["Stacks"]:
            tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
            if (
                tags.get("Suite") != OWNER
                or int(tags.get("Expires", "0")) >= time.time()
            ):
                continue
            if not tags.get("Expires") or not stack["StackName"].startswith("py-lmi-"):
                continue
            account = client("sts").get_caller_identity()["Account"]
            cleanup(
                {
                    "owned": True,
                    "stack": stack["StackName"],
                    "bucket": stack["StackName"] + "-" + account,
                    "run": tags["RunId"],
                }
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["build", "deploy", "collect", "cleanup", "reconcile"]
    )
    parser.add_argument("--run-id")
    parser.add_argument(
        "--runtime", choices=["python3.13", "python3.14"], default="python3.13"
    )
    parser.add_argument("--concurrency", type=int, choices=[1, 2], default=2)
    args = parser.parse_args()
    try:
        if args.command == "build":
            build()
        elif args.command == "deploy":
            deploy(args)
        elif args.command == "reconcile":
            reconcile()
        else:
            manifest = json.loads((ARTIFACTS / "manifest.json").read_text())
            if args.command == "cleanup":
                cleanup(manifest)
            else:
                from lmi_tests.cloud import Cloud

                Cloud(manifest).collect()
    except Exception as error:
        save(
            ARTIFACTS / f"{args.command}-error.json",
            {"type": type(error).__name__, "message": str(error)},
        )
        raise


if __name__ == "__main__":
    main()
