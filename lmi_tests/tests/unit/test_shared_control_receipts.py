# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Control intent and acknowledgement are separate, retained observations."""

import json
from threading import Event

import pytest

from lmi_tests.cloud import Cloud


def test_release_intent_is_saved_before_transport_and_ack_only_after_success(tmp_path):
    cloud = object.__new__(Cloud)
    cloud.manifest = {"bucket": "private"}
    cloud.case_context = object()
    cloud.cancel_event = Event()
    cloud.gates = {"gate", "second"}
    cloud.released_gates = set()
    cloud.control_prefix = "runs/run/cases/case/control/"
    cloud.control_records = []
    cloud.artifacts_dir = tmp_path
    cloud.source_uris = set()

    class S3:
        fail = False

        def put_object(self, **kwargs):
            recorded = json.loads((tmp_path / "control-records.json").read_text())
            assert recorded[-1]["state"] == "PENDING"
            assert "time" not in recorded[-1]
            if self.fail:
                raise OSError("ambiguous transport failure")

    cloud.s3 = S3()
    cloud.release("gate")
    successful = cloud.control_records[0]
    assert successful["state"] == "ACKNOWLEDGED"
    assert successful["started"] <= successful["time"]
    cloud.s3.fail = True
    cloud.release("gate")
    assert len(cloud.control_records) == 1
    with pytest.raises(OSError):
        cloud.release("second")
    assert cloud.control_records[-1]["state"] == "PENDING"
    assert "time" not in cloud.control_records[-1]
