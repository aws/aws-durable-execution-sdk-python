#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_FUNCTION_NAME_PREFIX = "DurablePythonExample-"
DEFAULT_DURABLE_LOGGING_CONFIG: dict[str, str] = {"LogFormat": "JSON"}
LOG_RETENTION_DAYS = 7
MICROVM_CONDITION = "HasMicrovmImage"


def uses_microvm(example: dict[str, Any]) -> bool:
    """Return true when the catalog example launches a MicroVM."""
    return example.get("usesMicrovm") is True


def load_catalog() -> dict[str, Any]:
    """Load the examples catalog."""
    catalog_path = Path(__file__).resolve().parent.parent / "examples-catalog.json"
    with catalog_path.open() as file:
        return json.load(file)


def to_logical_id(handler_name: str) -> str:
    """Convert a handler module name to a CloudFormation logical id."""
    handler_base = handler_name.replace(".handler", "")
    return "".join(word.capitalize() for word in handler_base.split("_"))


def build_template(examples: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a SAM template for all catalog examples."""
    template: dict[str, Any] = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Globals": {
            "Function": {
                "Runtime": {"Ref": "PythonRuntime"},
                "Timeout": 60,
                "MemorySize": 128,
                "Environment": {
                    "Variables": {"AWS_ENDPOINT_URL_LAMBDA": {"Ref": "LambdaEndpoint"}}
                },
            }
        },
        "Parameters": {
            "PythonRuntime": {
                "Type": "String",
                "Default": "python3.13",
                "AllowedValues": [
                    "python3.11",
                    "python3.12",
                    "python3.13",
                    "python3.14",
                ],
                "Description": "Python runtime to use for all example Lambda functions.",
            },
            "LambdaEndpoint": {
                "Type": "String",
                "Default": "https://lambda.us-west-2.amazonaws.com",
            },
            "FunctionNamePrefix": {
                "Type": "String",
                "Default": DEFAULT_FUNCTION_NAME_PREFIX,
            },
            "LambdaExecutionRoleArn": {
                "Type": "String",
                "Description": "ARN of an existing IAM role for all example Lambda functions.",
            },
            "AdotLayerArn": {
                "Type": "String",
                "Description": "ARN of the ADOT Python Lambda layer.",
            },
            # A deploy can pass both parameters while no example uses them.
            # CloudFormation rejects an override of an undeclared parameter.
            # So the template always declares both.
            "MicrovmImageArn": {
                "Type": "String",
                "Default": "",
                "Description": (
                    "ARN of the MicroVM image for the MicroVM examples. "
                    "Empty leaves the MicroVM examples out of the stack."
                ),
            },
            "MicrovmExecutionRoleArn": {
                "Type": "String",
                "Default": "",
                "Description": (
                    "ARN of the IAM role that the MicroVMs of the MicroVM "
                    "examples run with."
                ),
            },
        },
        "Resources": {},
    }

    # A MicroVM example launches the image named by MicrovmImageArn. Without an
    # image and a role, every invocation of its function would fail. So the
    # function and its log group exist only when both parameters are set.
    # CloudFormation linters flag an unused condition. So the template declares
    # the condition only when an example uses it.
    if any(uses_microvm(example) for example in examples):
        template["Conditions"] = {
            MICROVM_CONDITION: {
                "Fn::And": [
                    {"Fn::Not": [{"Fn::Equals": [{"Ref": "MicrovmImageArn"}, ""]}]},
                    {
                        "Fn::Not": [
                            {"Fn::Equals": [{"Ref": "MicrovmExecutionRoleArn"}, ""]}
                        ]
                    },
                ]
            }
        }

    for example in examples:
        logical_id = to_logical_id(example["handler"])
        properties: dict[str, Any] = {
            "CodeUri": "build/",
            "Handler": example["handler"],
            "Description": example["description"],
            "Role": {"Ref": "LambdaExecutionRoleArn"},
            "FunctionName": {"Fn::Sub": f"${{FunctionNamePrefix}}{logical_id}"},
        }

        if "durableConfig" in example:
            properties["DurableConfig"] = example["durableConfig"]
            logging_config: dict[str, Any] = dict(DEFAULT_DURABLE_LOGGING_CONFIG)
        else:
            logging_config = {}

        if "loggingConfig" in example:
            logging_config.update(example["loggingConfig"])

        logging_config["LogGroup"] = {"Ref": f"{logical_id}LogGroup"}
        properties["LoggingConfig"] = logging_config

        if "layers" in example:
            properties["Layers"] = example["layers"]

        if "tracing" in example:
            properties["Tracing"] = example["tracing"]

        if "timeout" in example:
            properties["Timeout"] = example["timeout"]

        variables: dict[str, Any] = dict(example.get("environment", {}))
        if uses_microvm(example):
            # SAM merges these variables with the Globals variables.
            variables["MICROVM_IMAGE_ARN"] = {"Ref": "MicrovmImageArn"}
            variables["MICROVM_EXECUTION_ROLE_ARN"] = {"Ref": "MicrovmExecutionRoleArn"}
        if variables:
            properties["Environment"] = {"Variables": variables}

        function_resource: dict[str, Any] = {
            "Type": "AWS::Serverless::Function",
            "DependsOn": [f"{logical_id}LogGroup"],
            "Properties": properties,
        }
        log_group_resource: dict[str, Any] = {
            "Type": "AWS::Logs::LogGroup",
            "Properties": {
                "LogGroupName": {
                    "Fn::Sub": f"/aws/lambda/${{FunctionNamePrefix}}{logical_id}"
                },
                "RetentionInDays": LOG_RETENTION_DAYS,
            },
        }
        if uses_microvm(example):
            # SAM copies the condition of a function to the resources that it
            # generates for the function.
            function_resource["Condition"] = MICROVM_CONDITION
            log_group_resource["Condition"] = MICROVM_CONDITION

        template["Resources"][logical_id] = function_resource
        template["Resources"][f"{logical_id}LogGroup"] = log_group_resource

    return template


def generate_sam_template(output_path: Path | None = None) -> Path:
    """Generate SAM template for all examples."""
    catalog = load_catalog()
    template = build_template(catalog["examples"])

    template_path = output_path or (
        Path(__file__).resolve().parent.parent / "template.generated.json"
    )
    template_path.parent.mkdir(parents=True, exist_ok=True)
    with template_path.open("w") as file:
        json.dump(template, file, sort_keys=False, indent=2)
        file.write("\n")

    print(f"Generated SAM template at {template_path}")
    return template_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a SAM template for examples")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the generated template to this path",
    )
    args = parser.parse_args()

    generate_sam_template(output_path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
