"""A stopped parent must reject new work created in a residual branch's finally."""

from lmi_tests import evidence
from lmi_tests.tests.unit.test_java_parity import run_inflight_case


def test_abandoned_child_cannot_start_another_step():
    events, history = run_inflight_case("late-operation", "SUCCEEDED")
    evidence.late_operation(events, history)
