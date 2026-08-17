from __future__ import annotations

import argparse
import json
from dataclasses import dataclass


DEFAULT_AUDIENCE = "sts.amazonaws.com"
CHINA_AUDIENCE = "sts.amazonaws.com.cn"


@dataclass(frozen=True)
class CredentialGroup:
    name: str
    regions: tuple[str, ...]
    role_secret: str
    aws_region: str
    audience: str = DEFAULT_AUDIENCE


@dataclass(frozen=True)
class PublishTarget:
    name: str
    regions: tuple[str, ...]
    role_secret: str
    aws_region: str
    audience: str

    def to_workflow_matrix_entry(self) -> dict[str, str]:
        return {
            "name": self.name,
            "regions": ",".join(self.regions),
            "role_secret": self.role_secret,
            "aws_region": self.aws_region,
            "audience": self.audience,
        }


DEFAULT_COMMERCIAL_REGIONS = (
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ca-central-1",
    "eu-central-1",
    "eu-north-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
)

OPT_IN_REGIONS = (
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-south-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-west-1",
    "eu-central-2",
    "eu-south-1",
    "eu-south-2",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
)

CHINA_REGIONS = ("cn-north-1", "cn-northwest-1")
US_GOV_REGIONS = ("us-gov-east-1", "us-gov-west-1")


def _region_role_secret(region: str) -> str:
    return f"LAYER_PUBLISH_ROLE_ARN_{region.upper().replace('-', '_')}"


CREDENTIAL_GROUPS = (
    CredentialGroup(
        name="commercial-default",
        regions=DEFAULT_COMMERCIAL_REGIONS,
        role_secret="LAYER_PUBLISH_ROLE_ARN",
        aws_region="us-east-1",
    ),
    *(
        CredentialGroup(
            name=region,
            regions=(region,),
            role_secret=_region_role_secret(region),
            aws_region=region,
        )
        for region in OPT_IN_REGIONS
    ),
    CredentialGroup(
        name="china",
        regions=CHINA_REGIONS,
        role_secret="LAYER_PUBLISH_ROLE_ARN_CHINA",
        aws_region="cn-north-1",
        audience=CHINA_AUDIENCE,
    ),
    CredentialGroup(
        name="us-gov",
        regions=US_GOV_REGIONS,
        role_secret="LAYER_PUBLISH_ROLE_ARN_US_GOV",
        aws_region="us-gov-east-1",
    ),
)

SUPPORTED_REGIONS = frozenset(
    region for group in CREDENTIAL_GROUPS for region in group.regions
)


def resolve_publish_targets(regions: str | None) -> tuple[PublishTarget, ...]:
    requested_regions = {
        region.strip() for region in (regions or "").split(",") if region.strip()
    }
    unknown_regions = requested_regions - SUPPORTED_REGIONS
    if unknown_regions:
        unknown_list = ", ".join(sorted(unknown_regions))
        raise ValueError(f"Unsupported layer publish regions: {unknown_list}")

    targets: list[PublishTarget] = []
    for group in CREDENTIAL_GROUPS:
        selected_regions = (
            tuple(region for region in group.regions if region in requested_regions)
            if requested_regions
            else group.regions
        )
        if selected_regions:
            targets.append(
                PublishTarget(
                    name=group.name,
                    regions=selected_regions,
                    role_secret=group.role_secret,
                    aws_region=group.aws_region,
                    audience=group.audience,
                )
            )
    return tuple(targets)


def serialize_publish_targets(targets: tuple[PublishTarget, ...]) -> str:
    entries = [target.to_workflow_matrix_entry() for target in targets]
    return json.dumps(entries, separators=(",", ":"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve credential-scoped Lambda layer publish targets."
    )
    parser.add_argument(
        "--regions",
        default="",
        help="Optional comma-separated AWS Regions; defaults to every supported Region.",
    )
    args = parser.parse_args()

    try:
        targets = resolve_publish_targets(args.regions)
    except ValueError as error:
        parser.error(str(error))
    print(serialize_publish_targets(targets))


if __name__ == "__main__":
    main()
