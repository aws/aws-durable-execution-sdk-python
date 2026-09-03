# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared serialization helpers for the Workflow Insight exporters.

Kept private to the ``exporters`` package: every backend needs the same
JS-compatible compact JSON encoding and the same key/file-name sanitizer, so
they live here rather than being duplicated per exporter module.
"""

from __future__ import annotations

import json
import re
from typing import Any


def compact_dumps(value: Any) -> str:
    """Serialize ``value`` as compact JSON (no whitespace, non-ASCII preserved).

    Matches the JS exporters' ``JSON.stringify`` output so the wire bytes are
    identical across SDKs.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def sanitize(value: str) -> str:
    """Replace characters unsafe for object keys / file names with ``_``."""
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)
