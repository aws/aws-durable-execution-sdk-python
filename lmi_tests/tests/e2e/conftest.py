"""Explicit cloud opt-in; infrastructure/collection exceptions stay JUnit errors."""

import json

import pytest

from lmi_tests.cloud import Cloud
from lmi_tests.deploy import ARTIFACTS, CONCURRENCIES
from lmi_tests.evidence import CollectionError, PlacementError, ProvisioningError


def pytest_addoption(parser):
    parser.addoption(
        "--lmi-cloud", action="store_true", help="Use a verified real LMI deployment"
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if call.excinfo:
        category = type(call.excinfo.value).__name__
        if isinstance(
            call.excinfo.value, (CollectionError, PlacementError, ProvisioningError)
        ):
            report.user_properties.append(("lmi_outcome", category))
            item.user_properties.append(("lmi_outcome", category))
        elif isinstance(call.excinfo.value, AssertionError):
            report.user_properties.append(("lmi_outcome", "RegressionAssertion"))
            item.user_properties.append(("lmi_outcome", "RegressionAssertion"))


@pytest.fixture(scope="session")
def deployment(request):
    if not request.config.getoption("--lmi-cloud"):
        pytest.skip("Real LMI deployment required; run hatch run lmi:cloud")
    manifest = json.loads((ARTIFACTS / "manifest.json").read_text())
    cloud = Cloud(manifest)
    cloud.verify()
    return manifest


@pytest.fixture(scope="session", params=CONCURRENCIES)
def configuration(request, deployment):
    return {"key": request.param, "pending": None}


@pytest.fixture
def cloud(deployment, configuration):
    # A failed teardown cannot silently hand a still-busy function to another
    # case. Retry only retirement; never retry the regression assertions.
    if configuration["pending"] is not None:
        configuration["pending"].settle_case()
    driver = Cloud(deployment, configuration["key"])
    configuration["pending"] = driver
    try:
        yield driver
    finally:
        try:
            driver.settle_case()
            configuration["pending"] = None
        finally:
            driver.collect(full_run=False)
