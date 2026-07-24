"""T8: trigger-rule truth table over upstream statuses."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.dag import TaskStatus, TriggerRule
from aws_durable_execution_sdk_python.operation.dag_executor import _trigger_passes

S = TaskStatus.SUCCEEDED
F = TaskStatus.FAILED
K = TaskStatus.SKIPPED

ALL_SUCC = [S, S]
ALL_FAIL = [F, F]
MIXED = [S, F]
INCLUDES_SKIP = [S, K]
EMPTY: list[TaskStatus] = []


@pytest.mark.parametrize(
    ("rule", "statuses", "expected"),
    [
        # ALL_SUCCESS
        (TriggerRule.ALL_SUCCESS, ALL_SUCC, True),
        (TriggerRule.ALL_SUCCESS, ALL_FAIL, False),
        (TriggerRule.ALL_SUCCESS, MIXED, False),
        (TriggerRule.ALL_SUCCESS, INCLUDES_SKIP, False),
        (TriggerRule.ALL_SUCCESS, EMPTY, True),  # root
        # ALL_FAILED (len>0 guard)
        (TriggerRule.ALL_FAILED, ALL_FAIL, True),
        (TriggerRule.ALL_FAILED, ALL_SUCC, False),
        (TriggerRule.ALL_FAILED, MIXED, False),
        (TriggerRule.ALL_FAILED, EMPTY, False),
        # ALL_DONE
        (TriggerRule.ALL_DONE, ALL_SUCC, True),
        (TriggerRule.ALL_DONE, ALL_FAIL, True),
        (TriggerRule.ALL_DONE, MIXED, True),
        (TriggerRule.ALL_DONE, INCLUDES_SKIP, True),
        (TriggerRule.ALL_DONE, EMPTY, True),
        # ONE_SUCCESS
        (TriggerRule.ONE_SUCCESS, MIXED, True),
        (TriggerRule.ONE_SUCCESS, ALL_FAIL, False),
        (TriggerRule.ONE_SUCCESS, EMPTY, False),
        # ONE_FAILED
        (TriggerRule.ONE_FAILED, MIXED, True),
        (TriggerRule.ONE_FAILED, ALL_SUCC, False),
        (TriggerRule.ONE_FAILED, EMPTY, False),
        # NONE_FAILED
        (TriggerRule.NONE_FAILED, ALL_SUCC, True),
        (TriggerRule.NONE_FAILED, INCLUDES_SKIP, True),
        (TriggerRule.NONE_FAILED, MIXED, False),
        (TriggerRule.NONE_FAILED, EMPTY, True),
    ],
)
def test_trigger_truth_table(rule, statuses, expected):
    assert _trigger_passes(rule, statuses) is expected
