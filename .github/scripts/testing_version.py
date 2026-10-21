#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re

from packaging.version import InvalidVersion, Version

_IMAGE_VERSION = re.compile(r"[0-9][0-9A-Za-z._-]*")


def parse_image_version(version: str) -> Version:
    """Validate a testing version usable by every image tag in the workflow.

    Require three release components and a valid PEP 440 suffix. Restrict the
    spelling to Docker tag characters, excluding epochs and local versions.
    Callers retain the original spelling when constructing or returning tags.
    """
    if not _IMAGE_VERSION.fullmatch(version):
        raise InvalidVersion(f"Version is not image-compatible: {version!r}")
    if len(f"v{version}-x86_64") > 128:
        raise InvalidVersion(
            f"Version exceeds the Docker tag length limit: {version!r}"
        )

    parsed = Version(version)
    if len(parsed.release) != 3:
        raise InvalidVersion(
            f"Version must have exactly three release components: {version!r}"
        )
    return parsed
