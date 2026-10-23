#!/usr/bin/env python3

import argparse
import json
from typing import Any

from generate_sam_template import load_catalog, to_logical_id, uses_microvm


def build_function_name_map(
    examples: list[dict[str, Any]],
    function_name_prefix: str,
    include_microvm: bool = False,
) -> dict[str, str]:
    """Map each example name to its qualified Lambda function name.

    The stack deploys a MicroVM example only when CI passes an image. So the
    map leaves a MicroVM example out unless ``include_microvm`` is true. The
    conftest skips a test whose example is missing from the map.
    """
    return {
        example["name"]: (
            f"{function_name_prefix}{to_logical_id(example['handler'])}:$LATEST"
        )
        for example in examples
        if include_microvm or not uses_microvm(example)
    }


def generate_function_name_map(
    function_name_prefix: str, include_microvm: bool = False
) -> dict[str, str]:
    """Generate a test-name to qualified Lambda function-name map."""
    return build_function_name_map(
        load_catalog()["examples"], function_name_prefix, include_microvm
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate pytest cloud function-name mapping"
    )
    parser.add_argument(
        "--function-name-prefix",
        required=True,
        help="Prefix used by the deployed SAM stack for Lambda function names",
    )
    parser.add_argument(
        "--include-microvm",
        action="store_true",
        help=(
            'Include the examples with "usesMicrovm": true. Pass it only when '
            "the stack was deployed with MicrovmImageArn."
        ),
    )
    args = parser.parse_args()

    print(
        json.dumps(
            generate_function_name_map(
                args.function_name_prefix, include_microvm=args.include_microvm
            ),
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
