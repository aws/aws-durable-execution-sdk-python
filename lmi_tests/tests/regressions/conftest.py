import pytest


def pytest_addoption(parser):
    parser.addoption("--run-lmi-regressions", action="store_true")


@pytest.fixture(autouse=True)
def opt_in(request):
    if not request.config.getoption("--run-lmi-regressions"):
        pytest.skip("#741 regressions are opt-in and expected red until the SDK fix")
