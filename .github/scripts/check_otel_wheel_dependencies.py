#!/usr/bin/env python3
from __future__ import annotations

import argparse
import email
import zipfile
from pathlib import Path

from packaging.requirements import Requirement


EXPECTED_STANDALONE_DEPENDENCIES = {
    "opentelemetry-api",
    "opentelemetry-propagator-aws-xray",
    "opentelemetry-sdk",
}


def check_otel_wheel_dependencies(wheel: Path) -> None:
    """Validate that OpenTelemetry dependencies are layer-provided by default."""
    requirements = _read_requirements(wheel)
    base_otel_dependencies = {
        requirement.name
        for requirement in requirements
        if requirement.name.startswith("opentelemetry-")
        and (requirement.marker is None or requirement.marker.evaluate({"extra": ""}))
    }
    if base_otel_dependencies:
        names = ", ".join(sorted(base_otel_dependencies))
        raise ValueError(
            f"{wheel.name} installs OpenTelemetry dependencies by default: {names}"
        )

    standalone_dependencies = {
        requirement.name
        for requirement in requirements
        if requirement.name.startswith("opentelemetry-")
        and requirement.marker is not None
        and requirement.marker.evaluate({"extra": "standalone"})
    }
    if standalone_dependencies != EXPECTED_STANDALONE_DEPENDENCIES:
        expected = ", ".join(sorted(EXPECTED_STANDALONE_DEPENDENCIES))
        actual = ", ".join(sorted(standalone_dependencies)) or "none"
        raise ValueError(
            f"{wheel.name} standalone OpenTelemetry dependencies must be "
            f"{expected}; found {actual}"
        )


def _read_requirements(wheel: Path) -> list[Requirement]:
    if not wheel.is_file():
        raise FileNotFoundError(wheel)

    with zipfile.ZipFile(wheel) as archive:
        metadata_files = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise ValueError(
                f"{wheel.name} must contain exactly one dist-info/METADATA file"
            )
        metadata = email.message_from_bytes(archive.read(metadata_files[0]))

    return [
        Requirement(value) for value in metadata.get_all("Requires-Dist", failobj=[])
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the OTel plugin wheel dependency contract."
    )
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args(argv)

    check_otel_wheel_dependencies(args.wheel)
    print(args.wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
