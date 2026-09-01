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

import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Literal, Protocol


class EmitMode(StrEnum):
    """When the plugin emits a record. The values match the JS plugin's modes."""

    # emit once at terminal SUCCEEDED/FAILED (default)
    ON_COMPLETE = "on-complete"
    # emit once only at terminal FAILED
    ON_FAILURE = "on-failure"
    # emit on every operation change and at end (nondeterministic count)
    ON_CHANGE = "on-change"


class OperationDetail(StrEnum):
    """How much of the operation tree a record carries. Values match the JS plugin."""

    # drop any operation with a parentId (default)
    TOP_LEVEL = "top-level"
    # include children of contexts too
    FULL_TREE = "full-tree"


# Accepted string inputs, kept in lockstep with the enum values above. Config
# fields are typed as these ``Literal`` unions (never bare ``str``) so a typoed
# mode fails a static type check, while ``WorkflowInsightConfig.__post_init__``
# normalizes any accepted value to the matching enum member and raises
# ``ValueError`` for an invalid dynamic string.
EmitModeInput = Literal["on-complete", "on-failure", "on-change"]
OperationDetailInput = Literal["top-level", "full-tree"]


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
    emit_mode: EmitMode | EmitModeInput | None = None
    operation_detail: OperationDetail | OperationDetailInput | None = None
    content: ContentConfig | None = None
    # Single shared deadline (seconds) for the invocation-end drain + flush of
    # every touched exporter lane. Mirrors the JS plugin's best-effort bound: on
    # timeout the workflow response is returned and delivery degrades to
    # best-effort. Must be a finite number greater than zero.
    export_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        # Normalize accepted string inputs to enum members so the plugin always
        # compares against ``EmitMode`` / ``OperationDetail`` members. ``EmitMode(x)``
        # is idempotent for members, accepts the exact JS-style strings, and raises
        # ``ValueError`` for an unrecognized dynamic string. Frozen dataclass, so use
        # ``object.__setattr__`` to rebind the normalized value.
        if self.emit_mode is not None:
            object.__setattr__(self, "emit_mode", EmitMode(self.emit_mode))
        if self.operation_detail is not None:
            object.__setattr__(
                self, "operation_detail", OperationDetail(self.operation_detail)
            )
        self._validate_export_timeout()

    def _validate_export_timeout(self) -> None:
        # A finite, strictly-positive number. Reject ``bool`` (a subtype of
        # ``int`` that would silently mean 1s / disallowed 0s), NaN, +/-inf, zero
        # and negatives -- an invalid timeout must fail loudly at construction,
        # not silently disable or unbound the invocation-end drain.
        timeout = self.export_timeout_seconds
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError(
                f"export_timeout_seconds must be a number, got {type(timeout).__name__}"
            )
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(
                "export_timeout_seconds must be a finite number greater than "
                f"zero, got {timeout!r}"
            )
