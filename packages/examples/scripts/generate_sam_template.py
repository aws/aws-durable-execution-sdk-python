#!/usr/bin/env python3

import json
from pathlib import Path

import json


def load_catalog():
    """Load examples catalog."""
    catalog_path = Path(__file__).parent.parent / "examples-catalog.json"
    with open(catalog_path) as f:
        return json.load(f)


def generate_sam_template():
    """Generate SAM template for all examples."""
    catalog = load_catalog()

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
            }
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
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                    "Policies": [
                        {
                            "PolicyName": "DurableExecutionPolicy",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Action": [
                                            "lambda:CheckpointDurableExecution",
                                            "lambda:GetDurableExecutionState",
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
    }

    for example in catalog["examples"]:
        # Convert handler name to PascalCase (e.g., hello_world -> HelloWorld)
        handler_base = example["handler"].replace(".handler", "")
        function_name = "".join(word.capitalize() for word in handler_base.split("_"))
        template["Resources"][function_name] = {
            "Type": "AWS::Serverless::Function",
            "Properties": {
                "CodeUri": "build/",
                "Handler": example["handler"],
                "Description": example["description"],
                "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
            },
        }

        if "durableConfig" in example:
            template["Resources"][function_name]["Properties"]["DurableConfig"] = (
                example["durableConfig"]
            )

    template_path = Path(__file__).parent.parent / "template.yaml"
    with open(template_path, "w") as f:
        json.dump(template, f, sort_keys=False, indent=2)

    print(f"Generated SAM template at {template_path}")


if __name__ == "__main__":
    generate_sam_template()
