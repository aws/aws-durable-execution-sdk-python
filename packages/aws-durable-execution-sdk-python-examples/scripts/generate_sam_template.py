#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Any


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
                "Default": "",
            },
            "LambdaExecutionRoleArn": {
                "Type": "String",
                "Description": "ARN of an existing IAM role for all example Lambda functions.",
            },
        },
        "Resources": {},
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

        if "loggingConfig" in example:
            properties["LoggingConfig"] = example["loggingConfig"]

        if "layers" in example:
            properties["Layers"] = example["layers"]

        if "tracing" in example:
            properties["Tracing"] = example["tracing"]

        if "environment" in example:
            properties["Environment"] = {"Variables": example["environment"]}

        template["Resources"][logical_id] = {
            "Type": "AWS::Serverless::Function",
            "Properties": properties,
        }

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
