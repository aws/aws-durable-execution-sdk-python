#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import tomllib
from pathlib import Path


def _read_pinned_sdk_version(metadata_path: Path) -> str:
    with metadata_path.open("rb") as file:
        metadata = tomllib.load(file)

    try:
        pinned_version = metadata["layer"]["sdk-version"]
    except KeyError as error:
        raise ValueError(f"{metadata_path}: missing layer.sdk-version") from error

    if not isinstance(pinned_version, str) or not pinned_version:
        raise ValueError(f"{metadata_path}: layer.sdk-version must be a string")
    return pinned_version


def resolve_layer_sdk_version(
    event_name: str,
    release_tag: str,
    source_sdk_version: str,
    metadata_path: Path,
) -> str:
    """Resolve the SDK version bundled in the OTel plugin Lambda layer."""

    pinned_version = _read_pinned_sdk_version(metadata_path)
    sdk_release = any(part.startswith("sdk-v") for part in release_tag.split(","))

    if event_name == "release" and not sdk_release:
        return pinned_version

    if event_name == "release" and pinned_version != source_sdk_version:
        raise ValueError(
            f"New SDK releases must update layer.sdk-version to {source_sdk_version}"
        )

    return source_sdk_version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve the SDK version for the OTel plugin Lambda layer."
    )
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--release-tag", default="")
    parser.add_argument("--source-sdk-version", required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args(argv)

    print(
        resolve_layer_sdk_version(
            event_name=args.event_name,
            release_tag=args.release_tag,
            source_sdk_version=args.source_sdk_version,
            metadata_path=args.metadata,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
