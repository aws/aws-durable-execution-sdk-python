from __future__ import annotations

import json
import os
import sys

import pytest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from resolve_layer_publish_targets import (
    CHINA_AUDIENCE,
    CHINA_REGIONS,
    CREDENTIAL_GROUPS,
    DEFAULT_AUDIENCE,
    DEFAULT_COMMERCIAL_REGIONS,
    OPT_IN_REGIONS,
    SUPPORTED_REGIONS,
    US_GOV_REGIONS,
    resolve_publish_targets,
    serialize_publish_targets,
)


EXPECTED_DEFAULT_COMMERCIAL_REGIONS = {
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
}
EXPECTED_OPT_IN_REGIONS = {
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
}


def test_region_sets_match_the_configured_aws_partitions() -> None:
    assert set(DEFAULT_COMMERCIAL_REGIONS) == EXPECTED_DEFAULT_COMMERCIAL_REGIONS
    assert set(OPT_IN_REGIONS) == EXPECTED_OPT_IN_REGIONS
    assert CHINA_REGIONS == ("cn-north-1", "cn-northwest-1")
    assert US_GOV_REGIONS == ("us-gov-east-1", "us-gov-west-1")


def test_default_targets_use_separate_credentials_for_each_region_scope() -> None:
    targets = resolve_publish_targets(None)

    assert len(targets) == len(OPT_IN_REGIONS) + 3
    assert {
        region for target in targets for region in target.regions
    } == SUPPORTED_REGIONS

    default_target = next(
        target for target in targets if target.name == "commercial-default"
    )
    assert default_target.regions == DEFAULT_COMMERCIAL_REGIONS
    assert default_target.role_secret == "LAYER_PUBLISH_ROLE_ARN"
    assert default_target.audience == DEFAULT_AUDIENCE

    for region in OPT_IN_REGIONS:
        target = next(target for target in targets if target.name == region)
        assert target.regions == (region,)
        assert target.role_secret == (
            f"LAYER_PUBLISH_ROLE_ARN_{region.upper().replace('-', '_')}"
        )
        assert target.aws_region == region
        assert target.audience == DEFAULT_AUDIENCE

    china_target = next(target for target in targets if target.name == "china")
    assert china_target.regions == CHINA_REGIONS
    assert china_target.role_secret == "LAYER_PUBLISH_ROLE_ARN_CHINA"
    assert china_target.audience == CHINA_AUDIENCE

    us_gov_target = next(target for target in targets if target.name == "us-gov")
    assert us_gov_target.regions == US_GOV_REGIONS
    assert us_gov_target.role_secret == "LAYER_PUBLISH_ROLE_ARN_US_GOV"
    assert us_gov_target.audience == DEFAULT_AUDIENCE


def test_requested_regions_only_include_required_credential_groups() -> None:
    targets = resolve_publish_targets(" cn-northwest-1,us-east-1,ap-east-1,cn-north-1 ")

    assert [(target.name, target.regions) for target in targets] == [
        ("commercial-default", ("us-east-1",)),
        ("ap-east-1", ("ap-east-1",)),
        ("china", CHINA_REGIONS),
    ]


def test_requested_regions_reject_unsupported_regions() -> None:
    with pytest.raises(
        ValueError,
        match="Unsupported layer publish regions: moon-west-1, test-east-1",
    ):
        resolve_publish_targets("test-east-1,moon-west-1")


def test_serialized_targets_are_valid_workflow_matrix_entries() -> None:
    targets = resolve_publish_targets("us-gov-west-1")

    assert json.loads(serialize_publish_targets(targets)) == [
        {
            "name": "us-gov",
            "regions": "us-gov-west-1",
            "role_secret": "LAYER_PUBLISH_ROLE_ARN_US_GOV",
            "aws_region": "us-gov-east-1",
            "audience": DEFAULT_AUDIENCE,
        }
    ]


def test_credential_groups_do_not_assign_a_region_more_than_once() -> None:
    configured_regions = [
        region for group in CREDENTIAL_GROUPS for region in group.regions
    ]

    assert len(configured_regions) == len(set(configured_regions))
    assert len(CREDENTIAL_GROUPS) <= 256
