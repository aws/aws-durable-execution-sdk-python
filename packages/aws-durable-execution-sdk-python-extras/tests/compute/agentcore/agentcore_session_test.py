# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the validation of the agentcore_session config."""

from __future__ import annotations

from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import ValidationError

from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    AgentcoreSessionConfig,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.session import (
    _validate_session_config,
)


ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"


def test_accepts_a_valid_config() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    assert _validate_session_config("review", config) == 1800


def test_config_has_no_default_runtime_arn() -> None:
    fields: dict[str, Any] = {"timeout": Duration.from_minutes(30)}

    with pytest.raises(TypeError, match="agent_runtime_arn"):
        AgentcoreSessionConfig(**fields)


def test_rejects_an_empty_name() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    with pytest.raises(
        ValidationError, match="an AgentCore operation requires a non-empty name"
    ):
        _validate_session_config("", config)


def test_rejects_a_non_string_name() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    with pytest.raises(
        ValidationError, match="an AgentCore operation requires a non-empty name"
    ):
        _validate_session_config(1, config)  # type: ignore[arg-type]


def test_rejects_a_timeout_under_1_second() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_seconds(0)
    )

    with pytest.raises(
        ValidationError, match='AgentCore "review": timeout must be at least 1 second'
    ):
        _validate_session_config("review", config)
