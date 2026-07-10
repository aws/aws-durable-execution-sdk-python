import importlib.util
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_script(name: str):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


generate_sam_template = load_script("generate_sam_template")
cleanup_unmanaged_log_groups = load_script("cleanup_unmanaged_log_groups")


def test_build_template_creates_lambda_log_group_with_seven_day_retention():
    template = generate_sam_template.build_template(
        [
            {
                "handler": "hello_world.handler",
                "description": "Example handler",
                "durableConfig": {
                    "RetentionPeriodInDays": 7,
                    "ExecutionTimeout": 300,
                },
            }
        ]
    )

    assert template["Resources"]["HelloWorldLogGroup"] == {
        "Type": "AWS::Logs::LogGroup",
        "Properties": {
            "LogGroupName": {"Fn::Sub": "/aws/lambda/${FunctionNamePrefix}HelloWorld"},
            "RetentionInDays": 7,
        },
    }
    assert template["Resources"]["HelloWorld"]["DependsOn"] == ["HelloWorldLogGroup"]
    assert template["Resources"]["HelloWorld"]["Properties"]["LoggingConfig"] == {
        "LogGroup": {"Ref": "HelloWorldLogGroup"},
    }


def test_build_template_preserves_existing_logging_config():
    template = generate_sam_template.build_template(
        [
            {
                "handler": "otel_logger_example.handler",
                "description": "Example handler",
                "loggingConfig": {
                    "ApplicationLogLevel": "INFO",
                    "LogFormat": "JSON",
                },
            }
        ]
    )

    assert template["Resources"]["OtelLoggerExample"]["Properties"][
        "LoggingConfig"
    ] == {
        "ApplicationLogLevel": "INFO",
        "LogFormat": "JSON",
        "LogGroup": {"Ref": "OtelLoggerExampleLogGroup"},
    }


def test_log_group_resources_match_generated_template_names(monkeypatch):
    monkeypatch.setattr(
        cleanup_unmanaged_log_groups,
        "load_catalog",
        lambda: {
            "examples": [
                {"handler": "hello_world.handler"},
                {"handler": "step_with_name.handler"},
            ]
        },
    )

    assert cleanup_unmanaged_log_groups.log_group_resources("Py313-") == [
        ("HelloWorldLogGroup", "/aws/lambda/Py313-HelloWorld"),
        ("StepWithNameLogGroup", "/aws/lambda/Py313-StepWithName"),
    ]


def test_cleanup_skips_stack_owned_log_groups_and_deletes_unmanaged(monkeypatch):
    deleted_log_groups = []

    monkeypatch.setattr(
        cleanup_unmanaged_log_groups,
        "log_group_resources",
        lambda _prefix: [
            ("OwnedLogGroup", "/aws/lambda/Py313-Owned"),
            ("UnmanagedLogGroup", "/aws/lambda/Py313-Unmanaged"),
        ],
    )
    monkeypatch.setattr(
        cleanup_unmanaged_log_groups,
        "stack_owns_resource",
        lambda _stack_name, logical_id, _region: logical_id == "OwnedLogGroup",
    )

    def fake_delete_log_group(log_group_name, _region):
        deleted_log_groups.append(log_group_name)
        return True

    monkeypatch.setattr(
        cleanup_unmanaged_log_groups,
        "delete_log_group",
        fake_delete_log_group,
    )

    assert cleanup_unmanaged_log_groups.cleanup_unmanaged_log_groups(
        stack_name="Py313-python-examples",
        function_name_prefix="Py313-",
        region="us-west-2",
    )
    assert deleted_log_groups == ["/aws/lambda/Py313-Unmanaged"]
