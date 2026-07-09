#!/usr/bin/env python3

import argparse
import json

from generate_sam_template import load_catalog, to_logical_id


def generate_function_name_map(function_name_prefix: str) -> dict[str, str]:
    """Generate a test-name to qualified Lambda function-name map."""
    catalog = load_catalog()
    return {
        example["name"]: (
            f"{function_name_prefix}{to_logical_id(example['handler'])}:$LATEST"
        )
        for example in catalog["examples"]
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate pytest cloud function-name mapping"
    )
    parser.add_argument(
        "--function-name-prefix",
        required=True,
        help="Prefix used by the deployed SAM stack for Lambda function names",
    )
    args = parser.parse_args()

    print(
        json.dumps(
            generate_function_name_map(args.function_name_prefix),
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
