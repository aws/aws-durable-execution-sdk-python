from pathlib import Path

import yaml


COMMERCIAL_REGIONS = {
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-central-1",
    "ca-west-1",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
}
CONFLICT_AFFECTED_REGIONS = {"me-central-1", "me-south-1"}


def test_default_layer_regions_exclude_conflict_affected_regions() -> None:
    workflow_path = Path(__file__).parents[2] / "workflows" / "lambda-layer-publish.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    configured_regions = {
        region.strip()
        for region in workflow["env"]["DEFAULT_LAYER_REGIONS"].split(",")
        if region.strip()
    }

    assert configured_regions == COMMERCIAL_REGIONS - CONFLICT_AFFECTED_REGIONS
