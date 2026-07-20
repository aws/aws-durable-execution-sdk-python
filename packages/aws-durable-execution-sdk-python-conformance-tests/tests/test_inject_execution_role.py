"""Unit tests for scripts/inject_execution_role.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import inject_execution_role  # noqa: E402


ROLE_ARN = "arn:aws:iam::123456789012:role/my-execution-role"


def _write(tmp_path: Path, doc: dict) -> str:
    path = tmp_path / "template.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, sort_keys=False)
    return str(path)


def test_inject_rewrites_functions_and_drops_role(tmp_path: Path) -> None:
    template = _write(
        tmp_path,
        {
            "Resources": {
                "DurableFunctionRole": {"Type": "AWS::IAM::Role"},
                "StepBasic": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {
                        "Handler": "step.step_basic.handler",
                        "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
                    },
                },
                "WaitBasic": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {"Handler": "wait.wait_basic.handler"},
                },
            }
        },
    )

    updated = inject_execution_role.inject(template, ROLE_ARN)

    assert updated == 2
    with open(template, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    resources = doc["Resources"]
    assert "DurableFunctionRole" not in resources
    assert resources["StepBasic"]["Properties"]["Role"] == ROLE_ARN
    assert resources["WaitBasic"]["Properties"]["Role"] == ROLE_ARN


def test_inject_sets_role_when_properties_missing(tmp_path: Path) -> None:
    template = _write(
        tmp_path,
        {
            "Resources": {
                "StepBasic": {"Type": "AWS::Serverless::Function"},
            }
        },
    )

    updated = inject_execution_role.inject(template, ROLE_ARN)

    assert updated == 1
    with open(template, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    assert doc["Resources"]["StepBasic"]["Properties"]["Role"] == ROLE_ARN


def test_inject_ignores_non_function_resources(tmp_path: Path) -> None:
    template = _write(
        tmp_path,
        {
            "Resources": {
                "SomeTable": {"Type": "AWS::DynamoDB::Table"},
                "StepBasic": {"Type": "AWS::Serverless::Function"},
            }
        },
    )

    updated = inject_execution_role.inject(template, ROLE_ARN)

    assert updated == 1
    with open(template, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    assert "Role" not in doc["Resources"]["SomeTable"]


def test_inject_raises_when_no_resources(tmp_path: Path) -> None:
    template = _write(tmp_path, {"AWSTemplateFormatVersion": "2010-09-09"})

    with pytest.raises(SystemExit):
        inject_execution_role.inject(template, ROLE_ARN)


def test_inject_raises_when_no_functions(tmp_path: Path) -> None:
    template = _write(
        tmp_path,
        {"Resources": {"DurableFunctionRole": {"Type": "AWS::IAM::Role"}}},
    )

    with pytest.raises(SystemExit):
        inject_execution_role.inject(template, ROLE_ARN)
