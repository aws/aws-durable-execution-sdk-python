"""Tests for the cloud test function-name map."""

import importlib.util
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
# The script imports generate_sam_template as a top-level module.
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SPEC = importlib.util.spec_from_file_location(
    "generate_function_name_map", SCRIPTS_DIR / "generate_function_name_map.py"
)
assert SPEC is not None
assert SPEC.loader is not None
generate_function_name_map = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generate_function_name_map)


EXAMPLES = [
    {"name": "Hello World", "handler": "hello_world.handler"},
    {
        "name": "MicroVM Run Job",
        "handler": "microvm_run_job.handler",
        "usesMicrovm": True,
    },
]


def test_map_leaves_out_microvm_examples_by_default():
    assert generate_function_name_map.build_function_name_map(EXAMPLES, "Py313-") == {
        "Hello World": "Py313-HelloWorld:$LATEST"
    }


def test_map_includes_microvm_examples_on_request():
    assert generate_function_name_map.build_function_name_map(
        EXAMPLES, "Py313-", include_microvm=True
    ) == {
        "Hello World": "Py313-HelloWorld:$LATEST",
        "MicroVM Run Job": "Py313-MicrovmRunJob:$LATEST",
    }


def test_map_covers_every_catalog_example_without_microvm():
    catalog = generate_function_name_map.load_catalog()["examples"]
    expected = {
        example["name"]
        for example in catalog
        if not generate_function_name_map.uses_microvm(example)
    }

    assert set(generate_function_name_map.generate_function_name_map("P-")) == (
        expected
    )
