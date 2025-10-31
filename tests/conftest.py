import pytest

from aws_durable_execution_sdk_python.logger import RequestInfo


@pytest.fixture(autouse=True)
def setup_request_info():
    RequestInfo.set(request_id="test-request-id")
    yield
    RequestInfo.set(request_id="test-request-id")
