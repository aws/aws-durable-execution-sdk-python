"""Preview generation utilities for durable function checkpoints.

This module provides a standalone utility for building compact preview objects
from values. Previews can be stored inline in checkpoint envelopes to make key
fields visible in the console and API without reading the full stored data.

The preview system is designed to be used with any SerDes implementation — not
just the filesystem serdes. For example, you could use build_preview with a
custom DynamoDB-backed serdes or any other external storage serdes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class PreviewMode(StrEnum):
    """Controls which fields are included in the preview by default.

    - INCLUDE_ALL: Include all fields, then apply exclude and mask rules.
    - EXCLUDE_ALL: Exclude all fields, then apply include and mask rules.
    """

    INCLUDE_ALL = "INCLUDE_ALL"
    EXCLUDE_ALL = "EXCLUDE_ALL"


class FieldMatchMode(StrEnum):
    """Controls whether a preview field is matched by name anywhere in the
    object tree, or by exact dot-notation path from the root.

    - ANYWHERE: Match the field name at any depth in the object tree (default).
    - PATH: Match by exact dot-notation path from root. A single segment
      (e.g. "email") matches only the root-level field.
    """

    ANYWHERE = "ANYWHERE"
    PATH = "PATH"


@dataclass(frozen=True)
class PreviewField:
    """A field selector used in preview include/exclude/mask lists.

    Attributes:
        name: Field name or dot-notation path.
        match: How to match the field. Defaults to FieldMatchMode.ANYWHERE.
    """

    name: str
    match: FieldMatchMode = FieldMatchMode.ANYWHERE


@dataclass(frozen=True)
class PreviewConfig:
    """Configuration for build_preview.

    Attributes:
        mode: Whether to start with all fields included or all excluded.
        include: Fields to include (used with EXCLUDE_ALL mode).
        exclude: Fields to exclude (used with INCLUDE_ALL mode).
        mask: Fields to mask — if visible, their value is replaced with mask_string.
        mask_string: String used to replace masked field values. Default: "***"
        max_preview_bytes: Maximum size in bytes for the preview object
            (JSON-serialized). Fields are added until this limit is reached.
            Default: 4096.
    """

    mode: PreviewMode
    include: list[PreviewField] = field(default_factory=list)
    exclude: list[PreviewField] = field(default_factory=list)
    mask: list[PreviewField] = field(default_factory=list)
    mask_string: str = "***"
    max_preview_bytes: int = 4096


def _field_matches(path: str, preview_field: PreviewField) -> bool:
    """Check if a field at the given path matches a PreviewField rule."""
    if preview_field.match == FieldMatchMode.PATH:
        return path == preview_field.name
    # ANYWHERE: match if any segment of the path matches the field name
    return preview_field.name in path.split(".")


def _is_matched(path: str, fields: list[PreviewField]) -> bool:
    """Check if a path matches any field in the list."""
    return any(_field_matches(path, f) for f in fields)


def build_preview(
    value: Any,
    config: PreviewConfig,
) -> dict[str, Any] | None:
    """Build a preview object from value according to config.

    Traverses the object tree and collects fields based on the include/exclude/mask
    rules in config. The result is a nested object mirroring the original structure,
    capped at config.max_preview_bytes (default 4096 bytes).

    Priority rules:
    - exclude always wins — excluded fields are never shown, even if in mask
    - mask implies visibility — masked fields are shown (with mask_string) unless
      excluded

    Limitations:
    - Field names containing dots are not supported (indistinguishable from path
      separators)
    - Array structure is not preserved — fields from array elements are merged
      into a plain object at the array's path
    - When array elements have heterogeneous shapes at the same field path,
      later elements overwrite earlier primitives in the preview

    Args:
        value: The value to build a preview from.
        config: Preview configuration.

    Returns:
        A nested dict representing the preview, or None if no fields are visible
        or value is not a dict/object.
    """
    if not isinstance(value, dict):
        return None

    dangerous_keys = {"__proto__", "constructor", "prototype"}
    pairs: list[tuple[str, Any]] = []

    def collect(obj: Any, path_prefix: str) -> None:
        if obj is None or not isinstance(obj, dict | list):
            return

        if isinstance(obj, list):
            for item in obj:
                collect(item, path_prefix)
            return

        for key in obj:
            if key in dangerous_keys:
                continue
            if "." in str(key):
                continue

            path = f"{path_prefix}.{key}" if path_prefix else str(key)
            masked = _is_matched(path, config.mask)
            excluded = _is_matched(path, config.exclude)
            visible = not excluded and (
                masked
                or (
                    config.mode == PreviewMode.INCLUDE_ALL
                    if not _is_matched(path, config.include)
                    else True
                )
            )

            if not visible:
                if not excluded:
                    collect(obj[key], path)
                continue

            if masked:
                pairs.append((path, config.mask_string))
                continue

            if isinstance(obj[key], dict | list):
                collect(obj[key], path)
            else:
                pairs.append((path, obj[key]))

    collect(value, "")
    if not pairs:
        return None

    # Apply byte budget
    accepted: list[tuple[str, Any]] = []
    estimated_size = 2  # "{}"
    for path, val in pairs:
        entry_size = len(f'"{path}":{json.dumps(val)},'.encode())
        if estimated_size + entry_size > config.max_preview_bytes:
            break
        accepted.append((path, val))
        estimated_size += entry_size

    if not accepted:
        return None

    # Build nested result dict
    result: dict[str, Any] = {}
    for path, val in accepted:
        parts = path.split(".")
        node = result
        for i in range(len(parts) - 1):
            if not isinstance(node.get(parts[i]), dict):
                node[parts[i]] = {}
            node = node[parts[i]]
        node[parts[-1]] = val

    return result if result else None
