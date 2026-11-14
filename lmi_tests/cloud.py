# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Python fixture binding to the shared AWS driver."""

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud as SharedCloud
from lmi_tests.deploy import ARTIFACTS, manager


class Cloud(SharedCloud):
    def __init__(
        self, manifest, configuration=None, *, artifacts_dir=None, case_context=None
    ):
        super().__init__(
            manifest,
            configuration,
            artifacts_dir=artifacts_dir or ARTIFACTS,
            case_context=case_context,
            verify_profile=manager().verify,
        )
