"""Install and verify built artifacts without silently using editable sources."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import json
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CORE = "aws-durable-execution-sdk-python"
OTEL = CORE + "-otel"


def built_wheel(package: str) -> Path:
    directory = ROOT / "packages" / package
    module = package.replace("-", "_")
    about = ast.parse((directory / "src" / module / "__about__.py").read_text())
    version = next(
        ast.literal_eval(node.value)
        for node in about.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        )
    )
    wheels = list((directory / "dist").glob(f"{module}-{version}-*.whl"))
    if len(wheels) != 1:
        raise ValueError(f"Build exactly one {package} {version} wheel first: {wheels}")
    return wheels[0]


def verify(wheel: Path, package: str) -> None:
    installed = importlib.metadata.distribution(package)
    direct = json.loads(installed.read_text("direct_url.json") or "{}")
    assert not direct.get("dir_info", {}).get("editable"), direct
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert direct["archive_info"]["hashes"]["sha256"] == digest, direct
    module = package.replace("-", "_")
    with zipfile.ZipFile(wheel) as archive:
        sources = [
            name
            for name in archive.namelist()
            if name.startswith(module + "/") and name.endswith(".py")
        ]
        assert sources
        for name in sources:
            path = Path(installed.locate_file(name)).resolve()
            assert "site-packages" in path.parts, path
            assert path.read_bytes() == archive.read(name), path
    print(
        json.dumps(
            {
                "package": package,
                "version": installed.version,
                "wheel": str(wheel),
                "sha256": digest,
                "verified_sources": len(sources),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-plugin", action="store_true")
    args = parser.parse_args()
    packages = [CORE] if args.legacy_plugin else [CORE, OTEL]
    wheels = [built_wheel(package) for package in packages]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--force-reinstall",
            *map(str, wheels),
        ],
        check=True,
    )
    for wheel, package in zip(wheels, packages, strict=True):
        verify(wheel, package)
    if args.legacy_plugin:
        assert importlib.metadata.version(OTEL) == "1.0.0"
        import aws_durable_execution_sdk_python_otel as otel

        assert "site-packages" in Path(otel.__file__).resolve().parts
    subprocess.run([sys.executable, "-m", "pip", "check"], check=True)


if __name__ == "__main__":
    main()
