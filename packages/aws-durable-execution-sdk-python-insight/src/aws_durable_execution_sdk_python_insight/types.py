# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Configuration types for the Workflow Insight plugin.

Mirrors the JS ``WorkflowInsightConfig`` / ``ContentConfig`` / ``OperationOverride``
(``aws-durable-execution-sdk-js-insight/src/types.ts``). Python uses snake_case
config field names; the *emitted wire record* keeps the JS camelCase field names
(see ``plugin.py``) so records read identically across SDKs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


# on-complete (default): emit once at terminal SUCCEEDED/FAILED.
# on-failure: emit once only at terminal FAILED.
# on-change: emit on every operation change and at end (nondeterministic count).
EmitMode = str  # "on-complete" | "on-failure" | "on-change"

# top-level (default): drop any operation with a parentId.
# full-tree: include children of contexts too.
OperationDetail = str  # "top-level" | "full-tree"


class InsightExporter(Protocol):
    """A destination that receives one curated Workflow Insight record.

    ``max_record_size_bytes`` bounds the serialized record body (the plugin's
    size limiter measures ``render(record)``); ``None`` disables truncation.
    ``render`` maps the canonical record dict to the exact shape the exporter
    serializes (identity for array exporters, the ``operationsByName`` expansion
    for point-access exporters).
    """

    max_record_size_bytes: int | None

    def render(self, record: dict[str, Any]) -> Any: ...  # pragma: no cover

    def export(self, record: dict[str, Any]) -> None: ...  # pragma: no cover

    def flush(self) -> None: ...  # pragma: no cover


@dataclass(frozen=True)
class OperationOverride:
    """Per-operation override matched by ``operation_name``.

    ``result`` opts the operation's result into the record via a transform that
    receives the checkpointed, JSON-parsed result (the SDK's own serialized form
    — the plugin never runs custom Serdes). Mirrors JS ``OperationOverride``.
    """

    operation_name: str
    exclude: bool = False
    result: Callable[[Any], Any] | None = None


@dataclass(frozen=True)
class ContentOperations:
    overrides: list[OperationOverride] = field(default_factory=list)
    include_errors: bool | None = None


@dataclass(frozen=True)
class ContentConfig:
    """Controls what data is included in emitted records.

    ``input`` / ``output``: ``False`` omits the field, a callable transforms it,
    ``True``/``None`` includes it as-is. Mirrors JS ``ContentConfig``.
    """

    input: bool | Callable[[Any], Any] | None = None
    output: bool | Callable[[Any], Any] | None = None
    operations: ContentOperations | None = None


@dataclass(frozen=True)
class WorkflowInsightConfig:
    """Configuration for the Workflow Insight plugin. Mirrors JS ``WorkflowInsightConfig``."""

    exporters: list[InsightExporter] = field(default_factory=list)
    sampling_rate: float | None = None
    emit_mode: EmitMode | None = None
    operation_detail: OperationDetail | None = None
    content: ContentConfig | None = None
