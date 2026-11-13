# Python-owned LMI regression suite

## Problem

Python-specific lifecycle requirements were mixed into the language-neutral
conformance catalog. That made Python exception, thread, cleanup and plugin
rules look like shared SDK requirements and coupled their evolution to the shared
package release.

## Cause

The implementation combined two separate responsibilities: reusable execution,
coordination and evidence handling, and the SDK-specific rules defining what a
Python fixture must do.

## Options considered

1. Keep a Python-only profile in the shared catalog. This leaves the ownership
   problem in place.
2. Copy the whole coordinator and reporter into the SDK. This duplicates the
   lease, journal, cancellation and retirement machinery.
3. Keep Python definitions and validators in this repository, using the shared
   runner through its programmatic suite interface.

## Chosen design

Use option 3. The Python SDK owns `python_catalog.py`, `python_lifecycle.py` and
`python_otel.py`, their regression tests, and the corresponding evidence rules.
The local `python-lifecycle` suite has 28 baseline lifecycle cells and 48 OTel
cells. It reports `python-lmi-*` IDs under the `python-lmi` report namespace.
These results do not claim acceptance of shared conformance requirements.

The shared repository owns only the language-neutral `standard` requirements
1–8 and `export-lifecycle` requirement 16. Its exporter profile runs 24 cells
across both modes, both views and native c1/c2. The Python workflow continues to
run those 24 shared cells alongside the 76 SDK-owned cells, using the same two
persistent functions. The separate shared standard profile is not implicitly
accepted by this combination.

`SuiteDefinition` supplies local requirement metadata, the exact matrix and the
local validator to the common programmatic runner. It does not register a new
profile in the shared CLI or package Python-specific requirement resources in
shared wheels. The common runner still owns bounded scheduling, immutable
manifest snapshots, exact launch journals, common evidence checks, resource
leases, independent retirement auditing and strict JSON/JUnit reporting.

Python-specific applicability, recovery policies and nonnegative resource-growth
budgets are declared in the local suite. Both OTel views and native c1/c2 remain
independent required cells. Missing capabilities, missing evidence and SDK bugs
stay failures; migration does not remove assertions or make the SDK implement a
capability it lacks.

## Evidence and cleanup

The adapter uses real public SDK hooks and real service history. It preserves
actual service operation timestamps and checkpoint results, immutable invocation
identities, captured SDK boundaries, exporter ownership and raw observations.
The validator selects the fixture's declared work gate separately from its
winner-admission handshake. Execution-view historical spans require matching
service-operation evidence. Contradictory span records remain failures.

A persisted deadline measurement records the original cohort and bounds before
normal fault release. The full event ledger remains available; new user work and
SDK dispatch after the cutoff cannot be excused as cleanup. Completion of already
dispatched I/O is accounted for separately.

Failed phases can retain partial observations before release. A separate final
snapshot may prove physical retirement without changing the original failure.
Every actual launch and owned resource must be accounted for; unknown requests,
missing completion or unpublished observations retain quarantine. Ownership uses
conditional S3 writes outside the expiring run prefix, with private storage and
native IAM/SigV4 authentication.

## Integration and removed coupling

CI automatically runs on trusted same-repository PRs through CodeBuild and keeps
manual dispatch. Fork and Dependabot PRs only run the unprivileged harness. The
conformance dependency is pinned to an exact reviewed revision in
`shared_contract.py` and the workflow. Setup can install that dependency before
importing any SDK-owned suite that uses it.

Python-only shared requirement files, catalog branches, validators and their
self-tests are moved out of the shared repository. Their behavior is retained
here under local IDs. Existing SDK-local regression entry points remain available.
The driver-only catalogs and validators are not included in the Lambda bundle.
No production SDK fix or real-LMI acceptance is implied by local harness tests.
