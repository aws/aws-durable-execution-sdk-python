# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durable operations that ask a decision model or a large language model.

This module is experimental. Every export can change or be removed in any
release.

The exports match ``@aws/durable-execution-sdk-js-extras/ai``:

- :func:`decider` returns ``check``, ``classify``, ``label``, and ``score``.
  Each one asks a Strands Decider one typed question in one step.
- :func:`llm` returns ``ask``, which asks a model on Amazon Bedrock one
  question in one step and returns its reply as text.
- :func:`agentcore_transport`, :func:`sagemaker_transport`, and
  :func:`lambda_transport` reach a Strands Decider on AWS.
"""

from aws_durable_execution_sdk_python_extras.ai._shared import (
    MAX_CLASSIFY_OPTIONS,
    MAX_SCORE_LEVELS,
)
from aws_durable_execution_sdk_python_extras.ai.decider import Decider, decider
from aws_durable_execution_sdk_python_extras.ai.errors import (
    AiError,
    AiTimeoutError,
    DeciderError,
    DeciderInvalidResponseError,
    LlmMaxTokensError,
    LlmStoppedError,
)
from aws_durable_execution_sdk_python_extras.ai.llm import (
    DEFAULT_LLM_MODEL_ID,
    BedrockCallOptions,
    BedrockConfig,
    BedrockGuardrail,
    Llm,
    llm,
)
from aws_durable_execution_sdk_python_extras.ai.retry import default_ai_retry_strategy
from aws_durable_execution_sdk_python_extras.ai.subtypes import AiOperationSubType
from aws_durable_execution_sdk_python_extras.ai.transports import (
    DeciderRequest,
    DeciderTransport,
    agentcore_transport,
    lambda_transport,
    sagemaker_transport,
)


__all__ = [
    "DEFAULT_LLM_MODEL_ID",
    "MAX_CLASSIFY_OPTIONS",
    "MAX_SCORE_LEVELS",
    "AiError",
    "AiOperationSubType",
    "AiTimeoutError",
    "BedrockCallOptions",
    "BedrockConfig",
    "BedrockGuardrail",
    "Decider",
    "DeciderError",
    "DeciderInvalidResponseError",
    "DeciderRequest",
    "DeciderTransport",
    "Llm",
    "LlmMaxTokensError",
    "LlmStoppedError",
    "agentcore_transport",
    "decider",
    "default_ai_retry_strategy",
    "lambda_transport",
    "llm",
    "sagemaker_transport",
]
