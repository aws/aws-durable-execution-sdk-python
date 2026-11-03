"""Small, dependency-free Actions summary; no executed scenario means no pass."""

import json
from pathlib import Path
import xml.etree.ElementTree as ET


def summarize(directory):
    directory = Path(directory)
    manifest = directory / "manifest.json"
    lines = ["LMI cloud regressions for Python SDK #741.", ""]
    if manifest.exists():
        config = json.loads(manifest.read_text())
        lines.append(
            f"Commit `{config['commit']}`; {config['runtime']}; one deployment, environment concurrency {list(config['concurrencies'].values())}."
        )
        lines.append(
            f"Timeouts: invocation {config['invocationTimeout']}s (all scenarios), durable execution {config['executionTimeout']}s, driver {config['driverTimeout']}s; cleanup grace {config['cleanupGrace']}s."
        )
    report = directory / "cloud.xml"
    if report.exists():
        for case in ET.parse(report).iter("testcase"):
            category = next(
                (
                    p.get("value")
                    for p in case.findall("properties/property")
                    if p.get("name") == "lmi_outcome"
                ),
                None,
            )
            problem = next(
                (
                    case.find(k)
                    for k in ("error", "failure", "skipped")
                    if case.find(k) is not None
                ),
                None,
            )
            state = category or (problem.tag.upper() if problem is not None else "PASS")
            lines.append(f"- {case.get('name')}: {state}")
    else:
        lines.append(
            "Cloud scenarios did not run. Inspect provisioning/build diagnostics."
        )
    for path in sorted(directory.glob("*-error.json")):
        data = json.loads(path.read_text())
        lines.append(f"- {path.stem}: {data['type']}")
    cleanup = directory / "cleanup.json"
    lines.append(
        "Run work released; persistent stack, functions, bucket, and code retained."
        if cleanup.exists()
        else "Run cleanup not confirmed; persistent resources are retained. Inspect cleanup diagnostics before updating."
    )
    return "\n\n".join(lines)


if __name__ == "__main__":
    print(summarize(Path(__file__).parent / "artifacts"))
