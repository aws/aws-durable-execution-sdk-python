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
    receipt = shared / "ownership-released.json"
    released = (
        config is not None
        and receipt.exists()
        and json.loads(receipt.read_text())
        == {"runId": config["run"], "lease": config.get("sharedLease")}
    )
    lines.append(
        "Run ownership released after retirement; persistent resources retained."
        if released
        else "Run cleanup not confirmed; ownership and persistent resources are retained."
    )
    return "\n\n".join(lines)


if __name__ == "__main__":
    print(summarize(Path(__file__).parent / "artifacts"))
