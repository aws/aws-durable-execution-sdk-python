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

## Release Permissions and Protection

The GitHub `pypi` environment permits deployments only from **tags** matching:

- `sdk-v*`
- `otel-v*`
- `testing-v*`
- `telemetry-exporter-v*`

Branches, including `main` and temporary publishing branches, are not allowed.
GitHub checks the workflow run's ref; checking out an allowed tag or a merged
commit inside a branch-triggered workflow does not make that run eligible.

The [protected tags ruleset](https://github.com/aws/aws-durable-execution-sdk-python/rules/25440902)
restricts creation, updates, deletion, and force pushes for the same tag
patterns. Have an actor permitted by that ruleset create the release tag at the
intended merged commit after CI passes. Treat published tags as immutable: use
a new version and tag for corrections instead of moving, deleting, or
recreating a published tag, even if your role permits those operations.

When adding a package with a new tag prefix, configure its pattern in **both**
the [`pypi` environment's deployment rules](https://github.com/aws/aws-durable-execution-sdk-python/settings/environments)
and the protected tags ruleset before its first release. Updating only the
workflow matrix or this document is not enough. Keep these protections in
place for first-time publications as well as later releases.

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
the [Packages](#packages) table above, and configure any new tag prefix as
described in [Release Permissions and Protection](#release-permissions-and-protection).
Merge the package and confirm CI passes before publishing. Follow
[CONTRIBUTING.md](CONTRIBUTING.md) for tests and run
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

Follow [Cutting a Release](#cutting-a-release) below using an allowed release tag.
The initial version only needs to be unused under the new distribution name;
there is no need to bump it just to create the project.

Check the entire release scope first. The current workflow publishes **all
packages in both matrices**, regardless of the release's tag prefix. After
PyPI publishing succeeds, it calls
[`ecr-release.yml`](.github/workflows/ecr-release.yml), which publishes the
testing emulator image only when the tag includes `testing-v<version>`. Do not
create a GitHub release solely to claim one name if those other publications
are not intended.

The current PyPI workflow has no package selector or manual-dispatch trigger,
and the environment rules prevent publishing from a temporary branch. If only
the new package should be uploaded, first review and merge workflow changes
that limit publication to the intended package and avoid the unrelated emulator
image release, while retaining the tag protections. Include those changes in
the release's tagged commit. For name reservation without an upload, use the
PyPI organization project-creation option above.

### 4. Verify the project

Check that the workflow ran on the intended release tag, that the tag and the
checkout step resolve to the selected merged source commit, and that both the
build and publish jobs succeeded. On PyPI, verify the project name, version,
wheel, and source archive. Compare their SHA-256 hashes with the workflow's
distribution artifact. Confirm the publisher now appears in the project's
**Publishing** settings and arrange the team's project ownership as needed.

If publishing reports `invalid-pending-publisher` or `invalid-publisher`, check
the repository owner, repository name, workflow filename, and environment
against the form values above. Also check that the built distribution's name
matches **PyPI Project Name**. Correct the configuration before retrying; adding
the pending publisher by itself does not finish publication.

If GitHub rejects access to the `pypi` environment, check the run's tag ref
against the allowed patterns. Retrying a branch run or changing its checkout
does not satisfy a tag-only deployment rule. If tag creation is rejected, have
an actor permitted by the protected tags ruleset create the intended release
tag. Do not relax the environment or tag protections to retry a publication.

## Cutting a Release

### 1. Set the version

For an existing distribution, update `__version__` in the relevant
`__about__.py` file(s) to a new version. For a new distribution's first release,
use the initial version prepared above. Commit and merge to `main`.

### 2. Create a GitHub Release

If the release creates a new tag, the actor creating it must be permitted by
the [protected tags ruleset](#release-permissions-and-protection). The tag must
point to the merged commit whose CI passed, including any publishing workflow
changes needed for this release.

1. Go to the [Releases page](https://github.com/aws/aws-durable-execution-sdk-python/releases) on GitHub.
2. Click **Draft a new release**.
3. Select the intended merged commit as the target and create a new tag
   following the tagging convention below. If the tag already exists, verify
   its target commit before selecting it; do not move it to another commit.
4. Set the release title (typically the same as the tag).
5. Write release notes following the format described in [Release Notes Format](#release-notes-format).
6. Click **Publish release**.

### Tagging Convention

The tag should be the version of the package being bumped, prepended with a descriptive prefix:

- **SDK only:** `sdk-v<version>` (e.g., `sdk-v1.6.0`)
- **OTel only:** `otel-v<version>` (e.g., `otel-v0.3.0`)
- **Testing only:** `testing-v<version>` (e.g., `testing-v1.2.1`)
- **Telemetry Exporter only:** `telemetry-exporter-v<version>` (e.g., `telemetry-exporter-v1.0.0`)
- **Multiple packages in a single release:** comma-separate the sub-tags (e.g., `sdk-v1.6.0,otel-v0.3.0`)

Examples:

```
sdk-v1.6.0
otel-v0.3.0
testing-v1.2.1
telemetry-exporter-v1.0.0
sdk-v1.6.0,otel-v0.3.0
```

If additional packages are added to the monorepo in the future, choose a short
descriptive prefix and use `<prefix>-v<version>`. Configure that prefix in both
sets of [protection rules](#release-permissions-and-protection) before releasing.
Combined tags such as `sdk-v1.6.0,otel-v0.3.0` match the `sdk-v*` pattern because
the full tag name starts with that prefix; the comma-separated parts are not
separate Git tags.

### 3. Update the shared documentation

The release engineer owns this step for every new version release. Complete it
after the published packages are available on PyPI.

The shared documentation lives in
[`aws/aws-durable-execution-docs`](https://github.com/aws/aws-durable-execution-docs).
Checking version strings alone is insufficient. Review the changes in every
package released, including the OTel and testing packages when applicable, and
update the documentation to match:

- [ ] API references and guides that describe new or changed behavior
- [ ] `examples/python/` snippets that use new or changed APIs
- [ ] Dependency versions, compatibility guidance, and migration instructions

Search the entire documentation repository so references are not missed:

```bash
rg -n 'aws-durable-execution-sdk-python' docs examples/python
```

Open or update the documentation pull request, link this GitHub release in its
description, and follow through on review and merge. If the release needs no
documentation changes, add a note to the GitHub release stating that the
documentation review was completed and found no changes needed.

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** all packages using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** each package to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event. Its publishing jobs use
the protected `pypi` environment, so the release tag must satisfy that
environment's deployment rules before they can upload to PyPI. The workflow
and package source come from the tagged revision; later changes on `main` do
not change an existing release's source.

> **Note:** The tag prefix does not select packages. The workflow builds and
> publishes every package in its matrices, including any unrelated version
> that has not been published yet. Check every package's version before
> creating a release. PyPI rejects an already published filename; with
> `fail-fast: false`, that job can fail while other packages still publish.

The [`ecr-release.yml`](.github/workflows/ecr-release.yml) workflow publishes the
testing emulator image only when the release tag names `testing-v<version>`,
either alone or in a comma-separated release tag. That version must match the
testing package's `__about__.py`. An SDK-only tag does not publish an image, even
if the testing package also has an unpublished version bump. Image versions need
three numeric release components and may have PEP 440 suffixes such as `rc1`,
`.post1`, or `.dev1`; epochs, local versions containing `+`, and duplicate testing
components are rejected before publication.

### Emulator images

Every testing release publishes its explicit version tag. `latest` is reserved
for stable testing package versions because SAM CLI uses it by default.
Stability is determined from the testing package's PEP 440 version: alpha
(`a1`), beta (`b1`), release candidate (`rc1`), and development (`.dev1`) versions
are prereleases; final versions and their post releases (`.post1`) are stable.

| Testing release tag | Multi-platform image tags |
| --- | --- |
| `testing-v2.1.0` | `v2.1.0`, and `latest` when newer than the current stable version |
| `testing-v2.1.0.post1` | `v2.1.0.post1`, and `latest` when newer than the current stable version |
| `testing-v2.2.0rc1` | `v2.2.0rc1`; `latest` is unchanged |
| `testing-v2.2.0.dev1` | `v2.2.0.dev1`; `latest` is unchanged |

Published version manifests are never overwritten. Prereleases still build and
publish their versioned images, and the workflow verifies those tags even though
the `latest` job is skipped. To test a prerelease with SAM CLI, select it explicitly:

```bash
DURABLE_EXECUTIONS_EMULATOR_IMAGE_TAG=v2.2.0rc1 sam local invoke MyFunction
```

The workflow requires the `ECR_UPLOAD_IAM_ROLE_ARN` secret and a role that trusts
this repository's release OIDC identity and permits ECR Public image reads and
publishing. Merge the workflow changes before creating a new testing release;
rerunning an older release uses its original workflow and tagged source.

Release runs queue through preflight, publication, and verification so retries
cannot overlap another release's writes. An existing version manifest is kept
unchanged. For stable releases, recovery copies that manifest by digest to
`latest` when it is newer than the version already there, then verifies the
digest and stable version in ECR. Older stable releases and all prereleases leave
`latest` unchanged. Registry read errors fail the workflow
instead of being treated as a missing image. ECR visibility is polled for up to
150 seconds; retry a failed run after resolving the reported error. An existing
`latest` without a recognized version tag, or pointing to a prerelease ahead of
the stable release being published, requires manual inspection.

## Release Notes Format

For the coordinated Core 3.0 / OTel 2.0 release, include the
[2.x to 3.0 migration guide](docs/migration-2.x-to-3.0.md) in the release notes.
It covers dependency and layer upgrades, custom providers, and validation.

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
      release, along with the testing emulator image publication when the tag
      includes `testing-v<version>`
- [ ] Changes merged to `main`
- [ ] CI checks pass on `main`
- [ ] Release tag points to the selected merged, CI-passing commit, including
      the publishing workflow used for the release
- [ ] Release tag matches an allowed `pypi` deployment pattern and is covered
      by the protected tags ruleset; any new prefix is configured in both
- [ ] The actor creating the release tag is permitted by the protected tags
      ruleset
- [ ] Release notes written with separate sections per package
- [ ] Tag follows the naming convention (`sdk-vX.Y.Z`, `otel-vX.Y.Z`, `testing-vX.Y.Z`,
      `telemetry-exporter-vX.Y.Z`, or comma-separated)

After publishing a release:

- [ ] Shared documentation reviewed per [Update the shared documentation](#3-update-the-shared-documentation), and the docs pull request opened or "no documentation changes needed" recorded on the release
