# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The subtypes of the ``decider`` and ``llm`` steps."""

from __future__ import annotations

from enum import Enum


class AiOperationSubType(str, Enum):
    """Subtypes that the ``decider`` and ``llm`` operations record on their
    steps.

    Experimental. It can change or be removed in any release.

    Execution history and plugins can filter on them without parsing step
    names. The values match the JavaScript SDK. Replay compares each subtype
    with the checkpoint. So a changed value is a breaking change for
    executions in flight, and these values must stay fixed.
    """

    DECIDER_CHECK = "DeciderCheck"
    """The step of ``decider().check``."""
    DECIDER_CLASSIFY = "DeciderClassify"
    """The step of ``decider().classify``."""
    DECIDER_LABEL = "DeciderLabel"
    """The step of ``decider().label``."""
    DECIDER_SCORE = "DeciderScore"
    """The step of ``decider().score``."""
    LLM_ASK = "LlmAsk"
    """The step of ``llm().ask``."""
