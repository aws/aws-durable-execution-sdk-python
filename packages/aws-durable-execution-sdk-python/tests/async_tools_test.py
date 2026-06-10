import asyncio

from aws_durable_execution_sdk_python.async_tools import invoke_callable


def test_invoke_callable_runs_async_callable():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "async-result"

    assert invoke_callable(async_callable) == "async-result"


def test_invoke_callable_runs_async_callable_from_running_loop():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "nested-async-result"

    async def main() -> str:
        return invoke_callable(async_callable)

    assert asyncio.run(main()) == "nested-async-result"
