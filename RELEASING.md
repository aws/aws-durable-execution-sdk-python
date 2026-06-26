# Releasing

This document describes how to cut a release for packages in this monorepo and how the automated PyPI publishing workflow is triggered.

## Packages

This monorepo contains the following packages:

| Package | Path | Tag Prefix |
|---------|------|------------|
| `aws-durable-execution-sdk-python` | `packages/aws-durable-execution-sdk-python` | `sdk` |
| `aws-durable-execution-sdk-python-otel` | `packages/aws-durable-execution-sdk-python-otel` | `otel` |

## Versioning

Each package maintains its own version in its respective `__about__.py` file:

- SDK: `packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/__about__.py`
- OTel: `packages/aws-durable-execution-sdk-python-otel/src/aws_durable_execution_sdk_python_otel/__about__.py`

Bump the version in the appropriate `__about__.py` file(s) and merge to `main` before creating a release.

## Cutting a Release

### 1. Bump the version

Update the `__version__` string in the relevant `__about__.py` file(s). Commit and merge to `main`.

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
- **Both packages in a single release:** comma-separate the sub-tags (e.g., `sdk-v1.6.0,otel-v0.3.0`)

Examples:

```
sdk-v1.6.0
otel-v0.3.0
sdk-v1.6.0,otel-v0.3.0
```

If additional packages are added to the monorepo in the future, follow the same pattern: choose a short descriptive prefix for the package and use `<prefix>-<version>`.

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** both packages using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** each package to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event, so it fires whenever a release is published on GitHub — no manual intervention is needed beyond creating the release.

> **Note:** The workflow builds and publishes all packages in the matrix. Ensure the version in each package's `__about__.py` is correct before publishing. If only one package has a version bump, PyPI will reject the re-upload of the unchanged package (which is expected and harmless since `fail-fast: false` is set).

Creating a GitHub Release also triggers the [`lambda-layer-publish.yml`](.github/workflows/lambda-layer-publish.yml) workflow. The workflow builds the SDK and OTel wheels from the release tag, packages them into Lambda layer zips for each supported Python runtime and architecture, and publishes public layer versions to the regions configured by the `LAMBDA_LAYER_REGIONS` repository variable. If no regions are configured, it publishes to `us-east-1`.

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
```

If only one package is being released, include only that package's section. Each package's changelog should be self-contained so users can follow the history of the package they depend on independently.

## Checklist

Before publishing a release:

- [ ] Version bumped in the relevant `__about__.py` file(s)
- [ ] Changes merged to `main`
- [ ] CI checks pass on `main`
- [ ] Release notes written with separate sections per package
- [ ] Tag follows the naming convention (`sdk-X.Y.Z`, `otel-X.Y.Z`, or comma-separated)
