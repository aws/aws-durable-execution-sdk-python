# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Configuration types for the Durable Execution Telemetry plugin.

Mirrors the JS ``DurableExecutionTelemetryConfig`` / ``ContentConfig`` /
``OperationOverride``
(``aws-durable-execution-sdk-js-telemetry-exporter/src/types.ts``). Python uses
snake_case config field names; the *emitted wire record* keeps the JS camelCase
field names (see ``plugin.py``) so records read identically across SDKs.
"""

from __future__ import annotations

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
# mode fails a static type check, while
# ``DurableExecutionTelemetryConfig.__post_init__`` normalizes any accepted value to the matching enum member and raises
# ``ValueError`` for an invalid dynamic string.
EmitModeInput = Literal["on-complete", "on-failure", "on-change"]
OperationDetailInput = Literal["top-level", "full-tree"]


class TelemetryExporter(Protocol):
    """A destination that receives one curated Durable Execution Telemetry record.

    ``max_record_size_bytes`` bounds the serialized record body (the plugin's
    size limiter measures ``render(record)``). First-party exporters default it
    to their destination's practical limit; an exporter whose value is ``None``
    is never truncated.
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
class DurableExecutionTelemetryConfig:
    """Configuration for the Durable Execution Telemetry plugin.

    Mirrors JS ``DurableExecutionTelemetryConfig``.
    """

    exporters: list[TelemetryExporter] = field(default_factory=list)
    sampling_rate: float | None = None
    emit_mode: EmitMode | EmitModeInput | None = None
    operation_detail: OperationDetail | OperationDetailInput | None = None
    content: ContentConfig | None = None

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
