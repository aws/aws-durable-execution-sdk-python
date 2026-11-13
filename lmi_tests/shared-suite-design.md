# Shared lifecycle suite ownership

The Python adapter consumes the seven standard lifecycle requirements pinned by
`shared_contract.py`. Each YAML file in the shared repository has one `scenario`.
Normal return is requirement 2; normal failure is requirement 7. There is no
Python-owned replacement catalog, OTel profile or exporter matrix in this PR.

The shared repository owns requirement descriptions, roles, minimum native
concurrency, scheduling, behavior validators and strict result aggregation. The
SDK repository maps each scenario to its real public API fixture and retains
source observations. `make_manifest` reads the shared matrix, uses c1 for
single-worker cases and c2 for overlap/recovery, and preserves every required role.

The same shared runner handles admission, original launch identity, immutable
case observations and independent retirement auditing. The adapter never replaces
a failed original with a retry, translates a runtime deadline into invented
service evidence, or clears a resource merely because cleanup was attempted.

The deployed instrumentation uses the public SDK observation hooks plus a
transparent service-client wrapper. It records operations and user-code entries;
it does not change production SDK behavior. Admission probes expose actual SDK
rejections, acceptance and pending work. Local harness tests validate that these
outcomes reach the shared validator accurately; they do not claim real-LMI
acceptance or fix the underlying SDK lifecycle behavior.

The core-only Lambda bundle excludes driver code and conformance packages. CI
retains the dedicated test-account boundary, trusted-PR condition, exact dependency
pin, conditional ownership lease and final reconciliation path.
