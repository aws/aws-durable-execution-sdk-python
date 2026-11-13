"""Summarize the shared lifecycle report without inventing cloud acceptance."""

import json
from pathlib import Path


def summarize(directory):
    directory = Path(directory)
    manifest = directory / "manifest.json"
    config = json.loads(manifest.read_text()) if manifest.exists() else None
    lines = ["Python shared LMI lifecycle conformance."]
    if config:
        lines.append(
            f"Commit `{config['commit']}`; {config['runtime']}; native concurrency {list(config['concurrencies'].values())}."
        )
    shared = directory / "shared"
    path = shared / "standard/report.json"
    report = json.loads(path.read_text()) if path.exists() else None
    if report and config is None:
        lines.append(
            "Deployment identity is missing; current cloud acceptance is unavailable."
        )
    elif report and config and report.get("run", {}).get("name") != config["run"]:
        lines.append(
            "Report belongs to another run; current cloud acceptance is unavailable."
        )
    elif report and report.get("results"):
        lines.extend(f"- {case['id']}: {case['status']}" for case in report["results"])
        lines.append(
            f"Shared runner exit code: {report.get('ci', {}).get('exit_code', 'unknown')}."
        )
    else:
        lines.append(
            "Cloud scenarios did not run. Inspect provisioning/build diagnostics."
        )
    for path in sorted(directory.glob("*-error.json")):
        data = json.loads(path.read_text())
        lines.append(f"- {path.stem}: {data['type']}")
    path = shared / "cleanup.json"
    cleanup = json.loads(path.read_text()) if path.exists() else {}
    if config is None or cleanup.get("runId") != config["run"]:
        lines.append("Run cleanup not confirmed; persistent resources are retained.")
    elif cleanup.get("status") == "NO_CASES":
        lines.append("No scenario work was recorded for cleanup.")
    elif cleanup.get("status") == "CLEANUP_REQUESTED" and cleanup.get("errors") == []:
        lines.append(
            "Run-scoped cleanup requests completed; see case reports for task retirement."
        )
    else:
        lines.append(
            "Run cleanup failed or is incomplete; inspect cleanup diagnostics."
        )
    return "\n\n".join(lines)


if __name__ == "__main__":
    print(summarize(Path(__file__).parent / "artifacts"))
