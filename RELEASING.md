# Releasing

This document describes how to cut a release for packages in this monorepo and how the automated PyPI publishing workflow is triggered.

## Packages

This monorepo contains the following packages:

| Package | Path | Tag Prefix |
|---------|------|------------|
| `aws-durable-execution-sdk-python` | `packages/aws-durable-execution-sdk-python` | `sdk` |
| `aws-durable-execution-sdk-python-otel` | `packages/aws-durable-execution-sdk-python-otel` | `otel` |
| `aws-durable-execution-sdk-python-testing` | `packages/aws-durable-execution-sdk-python-testing` | `testing` |
| `aws-durable-execution-sdk-python-telemetry-exporter` | `packages/aws-durable-execution-sdk-python-telemetry-exporter` | `telemetry-exporter` |

## Versioning

Each package maintains its own version in its respective `__about__.py` file:

- SDK: `packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/__about__.py`
- OTel: `packages/aws-durable-execution-sdk-python-otel/src/aws_durable_execution_sdk_python_otel/__about__.py`
- Testing: `packages/aws-durable-execution-sdk-python-testing/src/aws_durable_execution_sdk_python_testing/__about__.py`
- Telemetry Exporter: `packages/aws-durable-execution-sdk-python-telemetry-exporter/src/aws_durable_execution_sdk_python_telemetry_exporter/__about__.py`

Set the release version in the appropriate `__about__.py` file(s) and merge to
`main` before creating a release. Use a new version for an existing distribution,
or the initial version for a new distribution.

## Publishing a Package for the First Time

Each distribution name needs its own PyPI project and trusted publisher. This
also applies when renaming a package: the old project's publisher does not
carry over to the new name. Trusted publishing uses GitHub Actions OIDC, so this
process does not require a PyPI API token on your machine or in GitHub secrets.

### 1. Prepare the package

Confirm the distribution name in the package's `pyproject.toml` (`[project].name`)
and its initial version in `__about__.py`. The distribution name is the name used
by `pip install`; it can differ from the Python import name.

Add the package to **both** matrices in
[`pypi-publish.yml`](.github/workflows/pypi-publish.yml): its name and path in
`release-build`, and its name in `pypi-publish`. Add its path and tag prefix to
the [Packages](#packages) table above. Merge the package and confirm CI passes
before publishing. Follow [CONTRIBUTING.md](CONTRIBUTING.md) for tests and run
`hatch build` from the package directory. From the repository root, run
[`check_dist_legal_files.py`](.github/scripts/check_dist_legal_files.py) with
`packages/<package-name>` as its argument to verify the wheel and source archive
include `LICENSE` and `NOTICE`.

### 2. Configure the PyPI publisher

Sign in to the PyPI account that should own the new project. Open
[Publishing settings](https://pypi.org/manage/account/publishing/), find **Add a
new pending publisher**, select **GitHub**, and fill in:

| Field | Value |
|-------|-------|
| PyPI Project Name | The exact `[project].name` from the new package's `pyproject.toml` |
| Owner | `aws` |
| Repository name | `aws-durable-execution-sdk-python` |
| Workflow name | `pypi-publish.yml` |
| Environment name | `pypi` |

**Owner** is the GitHub organization, not the PyPI username. **Workflow name**
is the filename, without `.github/workflows/`; it is not the display name
`Upload PyPI Package`. Enter `pypi` for the environment even though the form
marks it optional, because the publishing job uses that GitHub environment.

Click **Add**. The publisher remains pending until the first successful upload,
which creates the project and converts it to a regular trusted publisher. The
PyPI account that registered it becomes a project owner. A pending publisher
**does not reserve the name**. See PyPI's
[first-project publishing guide](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).

If you need to claim a name without uploading a release and have access to a
PyPI organization, use the organization's **Manage → Projects → Create and add
a new project** action instead ([instructions](https://docs.pypi.org/organization-accounts/actions/project-actions/#create-a-project)).
Then add the trusted publisher through that project's **Publishing** settings.
A public PyPI 404 alone does not prove a name is free; a project can exist
without any releases ([name availability](https://pypi.org/help/#project-name)).

### 3. Publish the first version

For a coordinated release, follow [Cutting a Release](#cutting-a-release) below.
The initial version only needs to be unused under the new distribution name;
there is no need to bump it just to create the project.

Check the entire release scope first. The current workflow publishes **all
packages in both matrices**, regardless of the release's tag prefix. A release
also triggers [`ecr-release.yml`](.github/workflows/ecr-release.yml), which
publishes the testing emulator image. Do not create a GitHub release solely to
claim one name if those other publications are not intended.

To publish only the new package, a maintainer can use a temporary publishing
branch without changing `main`:

1. Create the branch from the merged commit selected for the first release.
2. On that branch, edit the existing `.github/workflows/pypi-publish.yml`.
   Replace its `release` trigger with `workflow_dispatch`, and keep only the
   new package in **both** matrices. Keep the workflow filename and `pypi`
   environment so they match the trusted publisher.
3. Pin the checkout step's `ref` to the selected source commit. Replace the
   environment URL's `github.event.release.name` expression with the initial
   version, since a manual dispatch has no release event. Preserve the build,
   legal-file verification, artifact upload/download, and OIDC publishing
   steps. The branch must satisfy the `pypi` environment's deployment rules.
4. Review the diff, push the temporary branch, then dispatch that branch's
   workflow. For example, for a branch named `first-pypi-publish`:

   ```bash
   gh workflow run pypi-publish.yml \
     --repo aws/aws-durable-execution-sdk-python \
     --ref first-pypi-publish
   ```

The workflow on `main` currently has no manual-dispatch trigger. The command
above uses the edited workflow on the temporary branch; omitting `--ref` does
not select that workflow. A manual dispatch does not trigger the release-event
workflows.

### 4. Verify the project

Check that the workflow ran on the intended branch and commit, that its checkout
step used the selected merged source commit, and that both the build and publish
jobs succeeded. On PyPI, verify the project name, version,
wheel, and source archive. Compare their SHA-256 hashes with the workflow's
distribution artifact. Confirm the publisher now appears in the project's
**Publishing** settings and arrange the team's project ownership as needed.

If publishing reports `invalid-pending-publisher` or `invalid-publisher`, check
the repository owner, repository name, workflow filename, and environment
against the form values above. Also check that the built distribution's name
matches **PyPI Project Name**. Correct the configuration before retrying; adding
the pending publisher by itself does not finish publication.

After a successful isolated publication, remove the temporary remote branch.
Do not merge its reduced matrices or manual-only trigger into `main`.

## Cutting a Release

### 1. Set the version

For an existing distribution, update `__version__` in the relevant
`__about__.py` file(s) to a new version. For a new distribution's first release,
use the initial version prepared above. Commit and merge to `main`.

### 2. Create a GitHub Release

1. Go to the [Releases page](https://github.com/aws/aws-durable-execution-sdk-python/releases) on GitHub.
2. Click **Draft a new release**.
3. Create a new tag following the tagging convention below.
4. Set the release title (typically the same as the tag).
5. Write release notes following the format described in [Release Notes Format](#release-notes-format).
6. Click **Publish release**.

### Tagging Convention

The tag should be the version of the package being bumped, prepended with a descriptive prefix:

- **SDK only:** `sdk-v<version>` (e.g., `sdk-v1.6.0`)
- **OTel only:** `otel-v<version>` (e.g., `otel-v0.3.0`)
- **Testing only:** `testing-v<version>` (e.g., `testing-v1.2.1`)
- **Telemetry Exporter only:** `telemetry-exporter-v<version>` (e.g., `telemetry-exporter-v0.0.1`)
- **Multiple packages in a single release:** comma-separate the sub-tags (e.g., `sdk-v1.6.0,otel-v0.3.0`)

Examples:

```
sdk-v1.6.0
otel-v0.3.0
testing-v1.2.1
telemetry-exporter-v0.0.1
sdk-v1.6.0,otel-v0.3.0
```

If additional packages are added to the monorepo in the future, follow the same pattern: choose a short descriptive prefix for the package and use `<prefix>-<version>`.

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** all packages using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** each package to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event, so it fires whenever a release is published on GitHub — no manual intervention is needed beyond creating the release.

> **Note:** The tag prefix does not select packages. The workflow builds and
> publishes every package in its matrices, including any unrelated version
> that has not been published yet. Check every package's version before
> creating a release. PyPI rejects an already published filename; with
> `fail-fast: false`, that job can fail while other packages still publish.

Releases containing an `otel-v` tag also trigger the
[`lambda-layer-publish.yml`](.github/workflows/lambda-layer-publish.yml)
workflow. It builds the SDK and OTel plugin into Lambda layers for each
supported Python runtime and architecture, then publishes public versions of the
`aws-durable-execution-sdk-python-otel-plugin` layer.
For OTel-only releases, the workflow downloads the exact SDK version pinned by
`layer.sdk-version` in `.github/lambda-layer-publish.toml`; that version must
already be published to PyPI. Combined SDK and OTel releases require the pin to
match the new SDK version and build both distributions from the tagged source.

The publishing job uses the `lambda-layer-publish` GitHub environment and its
`LAYER_PUBLISH_ROLE_ARN` secret. Set the optional `LAYER_PUBLISH_REGIONS`
environment variable to a comma-separated list of AWS Regions. When unset, the
workflow publishes to every commercial AWS Region supported by Lambda.
The workflow can also be run manually from the Actions tab on `main`; its
optional `regions` input overrides `LAYER_PUBLISH_REGIONS` for that run.
Each runtime and architecture layer archive is built once and retained as a
workflow artifact so retries publish the exact same resolved dependencies. Its
SHA-256 is included in the layer description and verified before reuse.
The publishing role must allow `lambda:PublishLayerVersion` and
`lambda:AddLayerVersionPermission`, as well as `lambda:ListLayerVersions` and
`lambda:GetLayerVersion` for identity-checked, idempotent release retries.

## Release Notes Format

Release notes should maintain separate timelines for each package. Use the following structure:

```markdown
## aws-durable-execution-sdk-python v1.6.0

### Features
- Added support for X
- New `context.foo()` API

### Bug Fixes
- Fixed issue with Y under Z conditions

### Breaking Changes
- Removed deprecated `bar()` method

---

## aws-durable-execution-sdk-python-otel v0.3.0

### Features
- Added tracing for `map` operations

### Bug Fixes
- Fixed span context propagation in child contexts

---

## aws-durable-execution-sdk-python-testing v1.2.1

### Bug Fixes
- Fixed issue with test runner under condition Z
```

If only one package is being released, include only that package's section. Each package's changelog should be self-contained so users can follow the history of the package they depend on independently.

## Checklist

Before publishing a release:

- [ ] Release version set in the relevant `__about__.py` file(s)
- [ ] For a first release of a new distribution name (for example
      `aws-durable-execution-sdk-python-telemetry-exporter`), follow
      [Publishing a Package for the First Time](#publishing-a-package-for-the-first-time)
      and configure its PyPI publisher before uploading
- [ ] Every unpublished version in the PyPI matrices is intended for this
      release, along with the testing emulator image publication
- [ ] OTel layer SDK pin identifies a compatible published SDK, or matches the
      SDK version included in a combined release
- [ ] Changes merged to `main`
- [ ] CI checks pass on `main`
- [ ] Release notes written with separate sections per package
- [ ] Tag follows the naming convention (`sdk-vX.Y.Z`, `otel-vX.Y.Z`, `testing-vX.Y.Z`,
      `telemetry-exporter-vX.Y.Z`, or comma-separated)
