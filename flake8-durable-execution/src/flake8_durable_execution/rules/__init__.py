from flake8_durable_execution.rules.random_call import RandomCallRule
from flake8_durable_execution.rules.datetime_now import DatetimeNowRule
from flake8_durable_execution.rules.uuid_generation import UuidGenerationRule
from flake8_durable_execution.rules.time_time import TimeTimeRule
from flake8_durable_execution.rules.os_environ import OsEnvironRule
from flake8_durable_execution.rules.network_call import NetworkCallRule
from flake8_durable_execution.rules.nested_durable_op import NestedDurableOpRule
from flake8_durable_execution.rules.closure_mutation import ClosureMutationRule

ALL_RULES = [
    RandomCallRule,
    DatetimeNowRule,
    UuidGenerationRule,
    TimeTimeRule,
    OsEnvironRule,
    NetworkCallRule,
    NestedDurableOpRule,
    ClosureMutationRule,
]

__all__ = [
    "RandomCallRule",
    "DatetimeNowRule",
    "UuidGenerationRule",
    "TimeTimeRule",
    "OsEnvironRule",
    "NetworkCallRule",
    "NestedDurableOpRule",
    "ClosureMutationRule",
    "ALL_RULES",
]
