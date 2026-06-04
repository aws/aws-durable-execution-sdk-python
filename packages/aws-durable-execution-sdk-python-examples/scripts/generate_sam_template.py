#!/usr/bin/env python3

import argparse
import json
import re
from pathlib import Path


def load_catalog() -> dict:
    """Load examples catalog."""
    catalog_path = Path(__file__).parent.parent / "examples-catalog.json"
    with open(catalog_path) as f:
        return json.load(f)


def logical_id_for_example(example: dict) -> str:
    """Create a stable CloudFormation logical ID for an example."""
    handler_base = example["handler"].replace(".handler", "")
    words = re.split(r"[^A-Za-z0-9]+", handler_base)
    return "".join(word[:1].upper() + word[1:] for word in words if word)


def get_example(catalog: dict, example_name: str) -> dict:
    for example in catalog["examples"]:
        if example["name"] == example_name:
            return example

    msg = f"Example not found: {example_name}"
    raise ValueError(msg)


def generate_sam_template(
    *,
    example_name: str | None = None,
    function_name: str | None = None,
    code_uri: str = ".aws-sam/source",
) -> dict:
    """Generate a SAM template for all examples, or for one selected example."""
    catalog = load_catalog()
    examples = (
        [get_example(catalog, example_name)] if example_name else catalog["examples"]
    )

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Globals": {
            "Function": {
                "Runtime": "python3.13",
                "Timeout": 60,
                "MemorySize": 128,
                "Environment": {
                    "Variables": {"AWS_ENDPOINT_URL_LAMBDA": {"Ref": "LambdaEndpoint"}}
                },
            }
        },
        "Parameters": {
            "LambdaEndpoint": {
                "Type": "String",
                "Default": "https://lambda.us-west-2.amazonaws.com",
            },
            "KmsKeyArn": {
                "Type": "String",
                "Default": "",
            },
        },
        "Conditions": {
            "HasKmsKeyArn": {"Fn::Not": [{"Fn::Equals": [{"Ref": "KmsKeyArn"}, ""]}]}
        },
        "Resources": {
            "DurableFunctionRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy"
                    ],
                    "Policies": [
                        {
                            "PolicyName": "DurableExecutionExamplesKmsPolicy",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Action": [
                                            "kms:CreateGrant",
                                            "kms:Decrypt",
                                            "kms:Encrypt",
                                        ],
                                        "Resource": "*",
                                    }
                                ],
                            },
                        }
                    ],
                },
            }
        },
        "Outputs": {},
    }

    for example in examples:
        logical_id = logical_id_for_example(example)
        properties = {
            "CodeUri": code_uri,
            "Handler": example["handler"],
            "Description": example["description"],
            "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
            "KmsKeyArn": {
                "Fn::If": [
                    "HasKmsKeyArn",
                    {"Ref": "KmsKeyArn"},
                    {"Ref": "AWS::NoValue"},
                ]
            },
        }

        if function_name:
            properties["FunctionName"] = function_name

        if "durableConfig" in example:
            properties["DurableConfig"] = example["durableConfig"]

        if "loggingConfig" in example:
            properties["LoggingConfig"] = example["loggingConfig"]

        template["Resources"][logical_id] = {
            "Type": "AWS::Serverless::Function",
            "Properties": properties,
        }
        template["Outputs"][f"{logical_id}FunctionName"] = {
            "Value": {"Ref": logical_id}
        }
        template["Outputs"][f"{logical_id}QualifiedFunctionName"] = {
            "Value": {
                "Fn::Sub": [
                    "${FunctionName}:$LATEST",
                    {"FunctionName": {"Ref": logical_id}},
                ]
            }
        }

    return template


def write_sam_template(
    *,
    output_path: Path,
    example_name: str | None = None,
    function_name: str | None = None,
    code_uri: str = ".aws-sam/source",
) -> None:
    template = generate_sam_template(
        example_name=example_name,
        function_name=function_name,
        code_uri=code_uri,
    )
    with open(output_path, "w") as f:
        json.dump(template, f, sort_keys=False, indent=2)
        f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SAM template for examples")
    parser.add_argument("--example-name", help="Generate a template for one example")
    parser.add_argument(
        "--function-name", help="Set FunctionName on the generated function"
    )
    parser.add_argument(
        "--code-uri",
        default=".aws-sam/source",
        help="CodeUri to use for generated functions",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "template.yaml",
    )
    args = parser.parse_args()

    write_sam_template(
        output_path=args.output,
        example_name=args.example_name,
        function_name=args.function_name,
        code_uri=args.code_uri,
    )

    print(f"Generated SAM template at {args.output}")


if __name__ == "__main__":
    main()
