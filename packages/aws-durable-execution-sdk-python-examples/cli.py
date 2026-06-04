#!/usr/bin/env python3

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


try:
    import boto3
    from aws_durable_execution_sdk_python.lambda_service import LambdaClient
except ImportError:
    sys.exit(1)


def load_catalog():
    """Load examples catalog."""
    catalog_path = Path(__file__).parent / "examples-catalog.json"
    with open(catalog_path) as f:
        return json.load(f)


def build_examples():
    """Build examples with SDK dependencies."""

    build_dir = Path(__file__).parent / "build"
    build_script = Path(__file__).parent / "scripts" / "build_deployment_artifacts.py"
    return (
        subprocess.run(
            [sys.executable, str(build_script), str(build_dir)],
            check=False,
        ).returncode
        == 0
    )


def create_kms_key(kms_client, account_id):
    """Create KMS key for durable functions encryption."""
    key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                "Action": "kms:*",
                "Resource": "*",
            },
            {
                "Sid": "Allow Lambda service",
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": ["kms:Decrypt", "kms:Encrypt", "kms:CreateGrant"],
                "Resource": "*",
            },
        ],
    }

    try:
        response = kms_client.create_key(
            Description="KMS key for Lambda Durable Functions environment variable encryption",
            KeyUsage="ENCRYPT_DECRYPT",
            KeySpec="SYMMETRIC_DEFAULT",
            Policy=json.dumps(key_policy),
        )

        return response["KeyMetadata"]["Arn"]

    except (kms_client.exceptions.ClientError, KeyError):
        return None


def bootstrap_account():
    """Bootstrap account with necessary IAM role and KMS key."""
    account_id = os.getenv("AWS_ACCOUNT_ID")
    region = os.getenv("AWS_REGION", "us-west-2")

    if not account_id:
        return False

    # Create KMS key first
    kms_client = boto3.client("kms", region_name=region)
    kms_key_arn = create_kms_key(kms_client, account_id)
    if not kms_key_arn:
        return False

    iam_client = boto3.client("iam", region_name=region)
    role_name = "DurableFunctionsIntegrationTestRole"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": ["lambda.amazonaws.com", "devo.lambda.aws.internal"]
                },
                "Action": "sts:AssumeRole",
            }
        ],
    }

    lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "lambda:CheckpointDurableExecution",
                    "lambda:GetDurableExecutionState",
                ],
                "Resource": "*",
                "Effect": "Allow",
            }
        ],
    }

    logs_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "*",
                "Effect": "Allow",
            }
        ],
    }

    kms_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": ["kms:CreateGrant", "kms:Decrypt", "kms:Encrypt"],
                "Resource": kms_key_arn,
                "Effect": "Allow",
            }
        ],
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for AWS Durable Functions integration testing",
        )

        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="LambdaPolicy",
            PolicyDocument=json.dumps(lambda_policy),
        )

        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="LogsPolicy",
            PolicyDocument=json.dumps(logs_policy),
        )

        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="DurableFunctionsLambdaStagingKMSPolicy",
            PolicyDocument=json.dumps(kms_policy),
        )

    except iam_client.exceptions.EntityAlreadyExistsException:
        pass
    except iam_client.exceptions.ClientError:
        return False
    else:
        return True

    return True


def create_deployment_package(example_name: str) -> Path:
    """Create deployment package for example."""

    build_dir = Path(__file__).parent / "build"
    if not build_dir.exists() and not build_examples():
        msg = "Failed to build examples"
        raise ValueError(msg)

    zip_path = Path(__file__).parent / f"{example_name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add SDK dependencies
        for file_path in build_dir.rglob("*"):
            if file_path.is_file() and not file_path.is_relative_to(build_dir / "src"):
                zf.write(file_path, file_path.relative_to(build_dir))

        # Add example files at root level
        src_dir = build_dir / "src"
        for file_path in src_dir.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(src_dir))

    return zip_path


def get_aws_config():
    """Get AWS configuration from environment."""
    return {
        "region": os.getenv("AWS_REGION", "us-west-2"),
        "lambda_endpoint": os.getenv(
            "LAMBDA_ENDPOINT", "https://lambda.us-west-2.amazonaws.com"
        ),
        "account_id": os.getenv("AWS_ACCOUNT_ID"),
        "kms_key_arn": os.getenv("KMS_KEY_ARN"),
    }


def get_lambda_client():
    """Get configured Lambda client."""
    config = get_aws_config()
    return boto3.client(
        "lambda",
        endpoint_url=config["lambda_endpoint"],
        region_name=config["region"],
        config=boto3.session.Config(parameter_validation=False),
    )


def retry_on_resource_conflict(func, *args, max_retries=5, **kwargs):
    """Retry function on ResourceConflictException."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if (
                hasattr(e, "response")
                and e.response.get("Error", {}).get("Code")
                == "ResourceConflictException"
                and attempt < max_retries - 1
            ):
                wait_time = 2**attempt  # Exponential backoff
                logger.info(
                    "ResourceConflictException on attempt %d, retrying in %ds...",
                    attempt + 1,
                    wait_time,
                )
                time.sleep(wait_time)
                continue
            raise
    return None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_command(command: list[str], *, cwd: Path) -> None:
    logger.info("Running: %s", " ".join(command))
    subprocess.run(command, cwd=cwd, check=True)


def _stack_name_for_function(function_name: str) -> str:
    stack_name = re.sub(r"[^A-Za-z0-9-]", "-", f"{function_name}-stack")
    stack_name = re.sub(r"-+", "-", stack_name).strip("-")
    if not stack_name or not stack_name[0].isalpha():
        stack_name = f"Durable-{stack_name}"
    return stack_name[:128]


def _write_sam_template(
    *,
    example_name: str | None = None,
    function_name: str | None = None,
) -> Path:
    examples_dir = Path(__file__).parent
    sam_dir = examples_dir / ".aws-sam"
    sam_dir.mkdir(exist_ok=True)
    source_dir = sam_dir / "source"
    template_path = sam_dir / "template.generated.json"
    generator = examples_dir / "scripts" / "generate_sam_template.py"
    source_builder = examples_dir / "scripts" / "build_deployment_artifacts.py"

    _run_command(
        [sys.executable, str(source_builder), str(source_dir)],
        cwd=_repo_root(),
    )

    command = [
        sys.executable,
        str(generator),
        "--output",
        str(template_path),
        "--code-uri",
        str(source_dir.resolve()),
    ]
    if example_name:
        command.extend(["--example-name", example_name])
    if function_name:
        command.extend(["--function-name", function_name])

    _run_command(command, cwd=_repo_root())
    return template_path


def sam_build(
    *,
    example_name: str | None = None,
    function_name: str | None = None,
    use_container: bool = True,
) -> Path:
    """Build examples with SAM."""
    examples_dir = Path(__file__).parent
    template_path = _write_sam_template(
        example_name=example_name,
        function_name=function_name,
    )
    build_dir = examples_dir / ".aws-sam" / "build"

    command = [
        "sam",
        "build",
        "--template-file",
        str(template_path),
        "--build-dir",
        str(build_dir),
    ]
    if use_container:
        command.append("--use-container")

    _run_command(command, cwd=_repo_root())
    return build_dir / "template.yaml"


def deploy_function(
    example_name: str,
    function_name: str | None = None,
    stack_name: str | None = None,
    use_container: bool = True,
):
    """Deploy function to AWS Lambda using SAM."""
    catalog = load_catalog()

    example_config = None
    for example in catalog["examples"]:
        if example["name"] == example_name:
            example_config = example
            break

    if not example_config:
        logger.error("Example not found: '%s'", example_name)
        list_examples()
        return False

    if not function_name:
        function_name = f"{example_name.replace(' ', '')}-Python"

    config = get_aws_config()
    stack_name = stack_name or _stack_name_for_function(function_name)
    built_template = sam_build(
        example_name=example_name,
        function_name=function_name,
        use_container=use_container,
    )

    parameter_overrides = [f"LambdaEndpoint={config['lambda_endpoint']}"]
    if config["kms_key_arn"]:
        parameter_overrides.append(f"KmsKeyArn={config['kms_key_arn']}")

    command = [
        "sam",
        "deploy",
        "--template-file",
        str(built_template),
        "--stack-name",
        stack_name,
        "--capabilities",
        "CAPABILITY_IAM",
        "--resolve-s3",
        "--no-confirm-changeset",
        "--no-fail-on-empty-changeset",
        "--region",
        config["region"],
        "--parameter-overrides",
        *parameter_overrides,
    ]
    _run_command(command, cwd=_repo_root())

    logger.info("Function deployed successfully! %s", function_name)
    logger.info("SAM stack: %s", stack_name)
    return True


def invoke_function(function_name: str, payload: str = "{}"):
    """Invoke a deployed function."""
    lambda_client = get_lambda_client()

    try:
        response = lambda_client.invoke(FunctionName=function_name, Payload=payload)

        result = json.loads(response["Payload"].read())

        if "DurableExecutionArn" in result:
            pass

        return result.get("DurableExecutionArn")

    except lambda_client.exceptions.ClientError:
        return None


def get_execution(execution_arn: str):
    """Get execution details."""
    lambda_client = get_lambda_client()

    try:
        return lambda_client.get_durable_execution(DurableExecutionArn=execution_arn)
    except lambda_client.exceptions.ClientError:
        return None


def get_execution_history(execution_arn: str):
    """Get execution history."""
    lambda_client = get_lambda_client()

    try:
        return lambda_client.get_durable_execution_history(
            DurableExecutionArn=execution_arn
        )
    except lambda_client.exceptions.ClientError:
        return None


def get_function_policy(function_name: str):
    """Get function resource policy."""
    lambda_client = get_lambda_client()

    try:
        response = lambda_client.get_policy(FunctionName=function_name)
        return json.loads(response["Policy"])
    except lambda_client.exceptions.ResourceNotFoundException:
        return None
    except (lambda_client.exceptions.ClientError, json.JSONDecodeError):
        return None


def list_examples():
    """List available examples."""
    catalog = load_catalog()
    logger.info("Available examples:")
    for example in catalog["examples"]:
        logger.info("  - %s: %s", example["name"], example["description"])


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="Durable Functions Examples CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Bootstrap command
    subparsers.add_parser("bootstrap", help="Bootstrap account with necessary IAM role")

    # Build command
    build_parser = subparsers.add_parser("build", help="Build examples with SAM")
    build_parser.add_argument("--example-name", help="Build one example")
    build_parser.add_argument(
        "--function-name", help="Set FunctionName in generated template"
    )
    build_parser.add_argument(
        "--no-use-container",
        action="store_true",
        help="Run SAM build without a Lambda-compatible build container",
    )

    # List command
    subparsers.add_parser("list", help="List available examples")

    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy an example")
    deploy_parser.add_argument("example_name", help="Name of example to deploy")
    deploy_parser.add_argument("--function-name", help="Custom function name")
    deploy_parser.add_argument(
        "--stack-name", help="Custom SAM/CloudFormation stack name"
    )
    deploy_parser.add_argument(
        "--no-use-container",
        action="store_true",
        help="Run SAM build without a Lambda-compatible build container",
    )

    # Invoke command
    invoke_parser = subparsers.add_parser("invoke", help="Invoke a deployed function")
    invoke_parser.add_argument("function_name", help="Name of function to invoke")
    invoke_parser.add_argument("--payload", default="{}", help="JSON payload to send")

    # Get command
    get_parser = subparsers.add_parser("get", help="Get execution details")
    get_parser.add_argument("execution_arn", help="Execution ARN")

    # Policy command
    policy_parser = subparsers.add_parser("policy", help="Get function resource policy")
    policy_parser.add_argument("function_name", help="Function name")

    # History command
    history_parser = subparsers.add_parser("history", help="Get execution history")
    history_parser.add_argument("execution_arn", help="Execution ARN")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "bootstrap":
            bootstrap_account()
        elif args.command == "build":
            sam_build(
                example_name=args.example_name,
                function_name=args.function_name,
                use_container=not args.no_use_container,
            )
        elif args.command == "list":
            list_examples()
        elif args.command == "deploy":
            deploy_function(
                args.example_name,
                args.function_name,
                args.stack_name,
                use_container=not args.no_use_container,
            )
        elif args.command == "invoke":
            invoke_function(args.function_name, args.payload)
        elif args.command == "policy":
            get_function_policy(args.function_name)
        elif args.command == "get":
            get_execution(args.execution_arn)
        elif args.command == "history":
            get_execution_history(args.execution_arn)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(1)


if __name__ == "__main__":
    main()
