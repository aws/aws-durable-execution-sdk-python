"""Filesystem-based serialization for durable functions.

This module provides a SerDes implementation that stores serialized values on a
durable filesystem (Amazon S3 Files or Amazon EFS mounted to Lambda).

WARNING: Do NOT use with Lambda's ephemeral /tmp storage. Lambda's /tmp is local
to a single execution environment and is not shared across invocations. On replay,
a different environment may be used and the file will not be found.

Use only with a durable, shared filesystem such as:
- Amazon S3 Files — mount an S3 bucket as a filesystem via the Lambda console or IaC
- Amazon EFS — mount an EFS file system to your Lambda function

Key Features:
- Two storage modes: ALWAYS (write every value to file) and OVERFLOW (inline if
  small, overflow to file if exceeds checkpoint size limit)
- Two path encoding modes: URI (human-readable) and HASH (fixed-length, safe)
- Optional preview support for storing compact summaries inline in checkpoints
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from aws_durable_execution_sdk_python.preview import (
    FieldMatchMode,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    build_preview,
)
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


# Re-export preview types so users can import everything from filesystem_serdes
__all__ = [
    "FieldMatchMode",
    "FileSystemPathEncoding",
    "FileSystemSerdesConfig",
    "FileSystemSerdesMode",
    "PreviewConfig",
    "PreviewField",
    "PreviewMode",
    "build_preview",
    "create_filesystem_serdes",
]

logger = logging.getLogger(__name__)

# Checkpoint size limit in bytes (256KB)
_CHECKPOINT_SIZE_LIMIT_BYTES = 256 * 1024

# Subtract 1KB headroom for the envelope wrapper and other checkpoint metadata
_OVERFLOW_THRESHOLD_BYTES = _CHECKPOINT_SIZE_LIMIT_BYTES - 1024


class FileSystemSerdesMode(StrEnum):
    """Controls when data is written to the filesystem.

    - ALWAYS: Every value is written to a file; the checkpoint stores only a
      file pointer. Best for consistently large payloads or when you want
      predictable checkpoint sizes.

    - OVERFLOW: Data is written inline (as JSON) unless it exceeds the durable
      function checkpoint size limit (~256KB), in which case it overflows to a
      file. Best for mixed workloads where most payloads are small.
    """

    ALWAYS = "ALWAYS"
    OVERFLOW = "OVERFLOW"


class FileSystemPathEncoding(StrEnum):
    """Controls how the durable execution ARN and operation ID are turned into
    on-disk directory and file names.

    - URI: The per-execution directory is a compact, human-navigable path built
      from the ARN's function name, execution name and invocation id
      (<functionName>/<executionName>/<invocationId>); the file name is the
      operation ID percent-encoded. Names stay readable, but a very long
      operation ID may exceed the filesystem's per-name length limit (commonly
      255 bytes). If the ARN does not match the expected durable-execution shape,
      the whole ARN is percent-encoded into a single directory segment instead.

    - HASH: The ARN (directory) and operation ID (file name) are each replaced
      by their SHA-256 hex digest. Names are a fixed length (64 chars) and always
      filesystem-safe regardless of the characters or length of the original
      value, at the cost of no longer being human-readable.
    """

    URI = "URI"
    HASH = "HASH"


class GeneratePreview(Protocol):
    """Protocol for preview generation functions."""

    def __call__(self, value: Any) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class FileSystemSerdesConfig:
    """Configuration options for create_filesystem_serdes.

    Attributes:
        storage_mode: Controls when data is written to the filesystem.
            Default: FileSystemSerdesMode.ALWAYS
        path_encoding: Controls how the ARN (directory) and operation ID (file
            name) are encoded into path segments. Default: FileSystemPathEncoding.URI
        generate_preview: Optional function that generates a preview object from
            the value. When provided, the preview is stored inline in the
            checkpoint envelope alongside the file pointer, making data visible
            in the console and API without reading the full file.
    """

    storage_mode: FileSystemSerdesMode = FileSystemSerdesMode.ALWAYS
    path_encoding: FileSystemPathEncoding = FileSystemPathEncoding.URI
    generate_preview: GeneratePreview | None = None


# Matches a durable execution ARN of the form:
# arn:<partition>:lambda:<region>:<account>:function:<functionName>:<version>/durable-execution/<executionName>/<invocationId>
_DURABLE_EXECUTION_ARN_PATTERN = re.compile(
    r"^arn:[^:]*:lambda:[^:]*:[^:]*:function:([^:/]+):[^:/]+/durable-execution/([^/]+)/([^/]+)$"
)


def _encode_segment(value: str, encoding: FileSystemPathEncoding) -> str:
    """Encode a path segment (ARN or operation ID) into a filesystem-safe name.

    For URI encoding, uses percent-encoding of all characters except unreserved
    (RFC 3986). For HASH encoding, uses SHA-256 hex digest.
    """
    if encoding == FileSystemPathEncoding.HASH:
        return hashlib.sha256(value.encode()).hexdigest()
    from urllib.parse import quote

    return quote(value, safe="")


def _parse_durable_execution_arn(
    arn: str,
) -> tuple[str, str, str] | None:
    """Parse a durable execution ARN into (functionName, executionName, invocationId).

    Returns None if the ARN doesn't match the expected pattern.
    """
    match = _DURABLE_EXECUTION_ARN_PATTERN.match(arn)
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def _resolve_execution_dir(
    base_path: str,
    arn: str,
    path_encoding: FileSystemPathEncoding,
) -> str:
    """Resolve the per-execution directory under base_path.

    In URI mode, derives a compact human-navigable path from the execution's
    function name, execution name and invocation id. If the ARN doesn't match
    the expected shape, the whole ARN is URI-encoded into a single segment.

    In HASH mode, the whole ARN is hashed into a single fixed-length segment.
    """
    if path_encoding == FileSystemPathEncoding.URI:
        parts = _parse_durable_execution_arn(arn)
        if parts:
            function_name, execution_name, invocation_id = parts
            return os.path.join(base_path, function_name, execution_name, invocation_id)
    return os.path.join(base_path, _encode_segment(arn, path_encoding))


def _write_to_file(
    base_path: str,
    value: Any,
    context: SerDesContext,
    path_encoding: FileSystemPathEncoding,
) -> str:
    """Write value as JSON to a file and return the file path."""
    dir_path = _resolve_execution_dir(
        base_path, context.durable_execution_arn, path_encoding
    )
    os.makedirs(dir_path, exist_ok=True)
    file_name = f"{_encode_segment(context.operation_id, path_encoding)}.json"
    file_path = os.path.join(dir_path, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(value, f)
    return file_path


class _FileSystemSerDes(SerDes[Any]):
    """SerDes that stores values on a durable filesystem.

    The checkpoint stores a JSON envelope that is either:
    - {"data": "<inline JSON>"} — value stored inline (OVERFLOW mode, under threshold)
    - {"file": "<path>"} — value stored in a file
    - {"file": "<path>", "preview": {...}} — file pointer with inline preview
    """

    def __init__(self, base_path: str, config: FileSystemSerdesConfig) -> None:
        self._base_path = base_path
        self._storage_mode = config.storage_mode
        self._path_encoding = config.path_encoding
        self._generate_preview = config.generate_preview

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        """Serialize value to a JSON envelope string.

        In ALWAYS mode, writes to file and returns a file pointer envelope.
        In OVERFLOW mode, stores inline if small, overflows to file if large.
        """
        if self._storage_mode == FileSystemSerdesMode.ALWAYS:
            file_path = _write_to_file(
                self._base_path, value, serdes_context, self._path_encoding
            )
            envelope: dict[str, Any] = {"file": file_path}
            if self._generate_preview:
                preview = self._generate_preview(value)
                if preview:
                    envelope["preview"] = preview
            return json.dumps(envelope)

        # OVERFLOW mode: serialize inline first, overflow to file if too large
        inline_json = json.dumps(value)
        envelope_str = json.dumps({"data": inline_json})
        if len(envelope_str.encode("utf-8")) > _OVERFLOW_THRESHOLD_BYTES:
            file_path = _write_to_file(
                self._base_path, value, serdes_context, self._path_encoding
            )
            envelope = {"file": file_path}
            if self._generate_preview:
                preview = self._generate_preview(value)
                if preview:
                    envelope["preview"] = preview
            return json.dumps(envelope)

        return envelope_str

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:  # noqa: ARG002
        """Deserialize from a JSON envelope string.

        Reads from file if envelope contains a file pointer, otherwise parses
        inline data.
        """
        envelope = json.loads(data)

        if "file" in envelope:
            with open(envelope["file"], encoding="utf-8") as f:
                return json.load(f)

        return json.loads(envelope["data"])


def create_filesystem_serdes(
    base_path: str,
    config: FileSystemSerdesConfig | None = None,
) -> SerDes[Any]:
    """Create a SerDes that stores serialized values on a durable filesystem.

    WARNING: Do NOT use with Lambda's ephemeral /tmp storage.
    Lambda's /tmp filesystem is local to a single execution environment and is
    not shared across invocations or function instances. On replay, a different
    execution environment may be used and the file will not be found, causing
    deserialization to fail.

    Use only with a durable, shared filesystem such as:
    - Amazon S3 Files — mount an S3 bucket as a filesystem via the Lambda
      console or IaC
    - Amazon EFS — mount an EFS file system to your Lambda function

    Both options provide persistence across invocations and are accessible from
    multiple concurrent function instances, which is required for correct replay
    behavior.

    The checkpoint stores a JSON envelope that is either:
    - {"data": "<inline JSON>"} — value stored inline (OVERFLOW mode, under
      threshold)
    - {"file": "<path>"} — value stored in a file
    - {"file": "<path>", "preview": {...}} — file pointer with inline preview
      (when preview is configured)

    Args:
        base_path: Directory path where data files will be stored (e.g. "/mnt/s3"
            for S3 Files, "/mnt/efs" for EFS).
        config: Optional configuration options. Defaults to ALWAYS mode with
            URI path encoding.

    Returns:
        A SerDes instance that reads/writes JSON files under base_path.

    Example:
        # Always write to S3 Files mount (default)
        fs_serdes = create_filesystem_serdes("/mnt/s3")

        # Only overflow to filesystem when payload exceeds ~256KB
        fs_serdes = create_filesystem_serdes("/mnt/s3", FileSystemSerdesConfig(
            storage_mode=FileSystemSerdesMode.OVERFLOW,
        ))

        # With preview: show id and masked email in checkpoint
        fs_serdes = create_filesystem_serdes("/mnt/s3", FileSystemSerdesConfig(
            generate_preview=lambda value: build_preview(value, PreviewConfig(
                mode=PreviewMode.EXCLUDE_ALL,
                include=[PreviewField(name="id"), PreviewField(name="status")],
                mask=[PreviewField(name="email")],
            )),
        ))
    """
    effective_config = config or FileSystemSerdesConfig()
    return _FileSystemSerDes(base_path, effective_config)
