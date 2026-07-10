#!/usr/bin/env python3

import argparse
import subprocess

from generate_sam_template import load_catalog, to_logical_id


def log_group_resources(function_name_prefix: str) -> list[tuple[str, str]]:
    """Return generated LogGroup logical ids and physical names."""
    logical_ids = {
        to_logical_id(example["handler"]) for example in load_catalog()["examples"]
    }
    return [
        (f"{logical_id}LogGroup", f"/aws/lambda/{function_name_prefix}{logical_id}")
        for logical_id in sorted(logical_ids)
    ]


def run_aws(args: list[str], region: str) -> subprocess.CompletedProcess[str]:
    """Run an AWS CLI command and capture its output."""
    return subprocess.run(
        ["aws", *args, "--region", region],
        capture_output=True,
        check=False,
        text=True,
    )


def stack_owns_resource(stack_name: str, logical_resource_id: str, region: str) -> bool:
    """Return true when the stack already owns the generated log group resource."""
    result = run_aws(
        [
            "cloudformation",
            "describe-stack-resource",
            "--stack-name",
            stack_name,
            "--logical-resource-id",
            logical_resource_id,
        ],
        region,
    )
    if result.returncode == 0:
        return True

    if "does not exist" in result.stderr:
        return False

    msg = result.stderr.strip() or result.stdout.strip()
    raise RuntimeError(msg)


def delete_log_group(log_group_name: str, region: str) -> bool:
    """Delete a log group, ignoring log groups that do not exist."""
    result = run_aws(
        ["logs", "delete-log-group", "--log-group-name", log_group_name],
        region,
    )
    if result.returncode == 0:
        print(f"Deleted unmanaged log group {log_group_name}")
        return True

    if "ResourceNotFoundException" in result.stderr:
        print(f"No unmanaged log group found for {log_group_name}")
        return True

    print(result.stderr, end="")
    return False


def cleanup_unmanaged_log_groups(
    stack_name: str,
    function_name_prefix: str,
    region: str,
) -> bool:
    """Remove log groups that would block CloudFormation from owning them."""
    success = True
    resources = log_group_resources(function_name_prefix)
    for logical_resource_id, log_group_name in resources:
        if stack_owns_resource(stack_name, logical_resource_id, region):
            print(f"Stack already owns {logical_resource_id}; leaving it in place")
            continue

        success = delete_log_group(log_group_name, region) and success

    return success


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete unmanaged Lambda log groups before SAM owns them"
    )
    parser.add_argument("--stack-name", required=True)
    parser.add_argument("--function-name-prefix", required=True)
    parser.add_argument("--region", required=True)
    args = parser.parse_args()

    if not cleanup_unmanaged_log_groups(
        stack_name=args.stack_name,
        function_name_prefix=args.function_name_prefix,
        region=args.region,
    ):
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
