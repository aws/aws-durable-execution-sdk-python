# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the checks that run before any durable operation."""

from __future__ import annotations

import math
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import ValidationError

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmConfig,
    MicrovmRequestConfig,
    microvm,
)
from aws_durable_execution_sdk_python_extras.microvm import operation


class NoOperations:
    """A context that fails the test when the operation starts any durable
    operation."""

    def __init__(self, arn: str = "arn:aws:lambda:eu-west-1:1:function:f") -> None:
        self.execution_context = type("ExecutionContext", (), {})()
        self.execution_context.durable_execution_arn = arn

    def run_in_child_context(self, *args: Any, **kwargs: Any) -> Any:
        msg = "no durable operation may start"
        raise AssertionError(msg)


def config(**fields: Any) -> MicrovmConfig:
    values: dict[str, Any] = {
        "image_identifier": "img",
        "execution_role_arn": "role",
        "timeout": Duration.from_minutes(5),
        **fields,
    }
    return MicrovmConfig(**values)


@pytest.mark.parametrize(
    ("name", "fields", "message"),
    [
        ("", {}, "requires a non-empty name"),
        ("build", {"image_identifier": ""}, "image_identifier is required"),
        ("build", {"execution_role_arn": ""}, "execution_role_arn is required"),
        ("build", {"timeout": Duration(0)}, "timeout must be between 1 second"),
        (
            "build",
            {"timeout": Duration.from_seconds(28_801)},
            "Got 28801 seconds",
        ),
        (
            "build",
            {"heartbeat_timeout": Duration(0)},
            "heartbeat_timeout must be at least 1 second",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="job")},
            "the request path must be an RFC 3986 path",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path=7)},  # type: ignore[arg-type]
            "the request path must be an RFC 3986 path",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/clone build")},
            'starts with "/". Percent-encode any other character, for example a '
            "space as %20.",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/café")},
            "the request path must be an RFC 3986 path",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/job?x=1")},
            "the request path must be an RFC 3986 path",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/bad%zz")},
            "the request path must be an RFC 3986 path",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/aws/lambda-microvms/run")},
            "must not use the lifecycle hook prefix /aws/lambda-microvms/",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/jobs/../build")},
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/jobs/./build")},
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/jobs/..")},
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/jobs/%2e/build")},
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(path="/jobs/%2E%2e/build")},
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {
                "request": MicrovmRequestConfig(
                    path="/jobs/../aws/lambda-microvms/runtime/v1/run"
                )
            },
            'must not have a "." or ".." segment',
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(port=0)},
            "the request port must be an integer between 1 and 65535",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(port=65_536)},
            "the request port must be an integer between 1 and 65535",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(port=True)},
            "the request port must be an integer between 1 and 65535",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(port=8080.0)},  # type: ignore[arg-type]
            "the request port must be an integer between 1 and 65535",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(retry_window=Duration(0))},
            "the request retry_window must be between 1 second and 10 minutes",
        ),
        (
            "build",
            {"request": MicrovmRequestConfig(retry_window=Duration.from_seconds(601))},
            "the request retry_window must be between 1 second and 10 minutes",
        ),
    ],
)
def test_rejects_an_invalid_call_before_any_durable_operation(
    name: str, fields: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        microvm(NoOperations(), name, job_input={}, config=config(**fields))  # type: ignore[arg-type]


def test_raises_the_class_of_duration_for_every_invalid_timeout() -> None:
    # Duration rejects a negative value itself. The operation rejects zero.
    # Both raise the SDK's ValidationError.
    with pytest.raises(ValidationError):
        config(timeout=Duration.from_seconds(-1))
    with pytest.raises(ValidationError, match="timeout must be between"):
        microvm(
            NoOperations(),  # type: ignore[arg-type]
            "build",
            job_input={},
            config=config(timeout=Duration(0)),
        )


@pytest.mark.parametrize("job_input", [{1, 2}, math.nan, {"x": math.inf}])
def test_rejects_an_input_that_is_not_json(job_input: Any) -> None:
    with pytest.raises(TypeError, match="must be JSON-serializable"):
        microvm(NoOperations(), "build", job_input=job_input, config=config())  # type: ignore[arg-type]


def test_rejects_a_call_without_a_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION")

    with pytest.raises(ValidationError, match="cannot determine the AWS Region"):
        microvm(NoOperations("not-an-arn"), "build", job_input={}, config=config())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("arn", "expected"),
    [
        ("arn:aws:lambda:eu-west-1:1:function:f:$LATEST/x", ("aws", "eu-west-1")),
        ("arn:aws-cn:lambda:cn-north-1:1:function:f", ("aws-cn", "cn-north-1")),
        ("not-an-arn", ("aws", "us-west-2")),
        ("arn:aws:lambda::1:function:f", ("aws", "us-west-2")),
    ],
)
def test_reads_the_partition_and_region(arn: str, expected: tuple[str, str]) -> None:
    assert operation._partition_and_region(arn) == expected  # noqa: SLF001


def test_creates_one_default_client_per_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[tuple[str, str]] = []
    configs: list[Any] = []

    class Session:
        def client(self, service: str, region_name: str, config: Any) -> object:
            created.append((service, region_name))
            configs.append(config)
            return object()

    monkeypatch.setattr(operation.boto3.session, "Session", Session)
    clients = operation._DefaultClients()  # noqa: SLF001

    first = clients.get("us-east-1")

    assert clients.get("us-east-1") is first
    assert clients.get("eu-west-1") is not first
    assert created == [
        ("lambda-microvms", "us-east-1"),
        ("lambda-microvms", "eu-west-1"),
    ]
    # botocore's defaults are 60 seconds each, with legacy retries.
    assert (configs[0].connect_timeout, configs[0].read_timeout) == (5, 30)
    assert configs[0].retries == {"mode": "standard", "total_max_attempts": 3}


def test_takes_the_input_and_the_config_by_keyword_only() -> None:
    with pytest.raises(TypeError, match="positional argument"):
        microvm(NoOperations(), "build", {}, config())  # type: ignore[arg-type, misc]


class LambdaContext:
    def get_remaining_time_in_millis(self) -> int:
        return 12_500


@pytest.mark.parametrize(
    ("lambda_context", "expected"), [(LambdaContext(), 12.5), (None, None)]
)
def test_reads_the_remaining_time_or_reports_no_deadline(
    lambda_context: Any, expected: float | None
) -> None:
    context: Any = type("Context", (), {"lambda_context": lambda_context})()

    assert operation._remaining_seconds(context)() == expected  # noqa: SLF001


@pytest.mark.parametrize("path", ["/.well-known/job", "/.../job", "/a.b/c..d", "/"])
def test_accepts_dots_that_are_not_a_dot_segment(path: str) -> None:
    # Dots next to other characters, and three dots, are not dot segments.
    # fetch and urllib leave such a path unchanged.
    assert (
        operation._validate_request("build", MicrovmRequestConfig(path=path))  # noqa: SLF001
        == 60
    )
