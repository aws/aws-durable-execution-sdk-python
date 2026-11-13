# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""SDK-owned bridge to the pinned shared LMI scenario coordinator."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from concurrent.futures import TimeoutError as FutureTimeout
from copy import deepcopy
import hashlib
from pathlib import Path
import time
from typing import Any, Protocol

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    ScenarioContext,
)
from lmi_tests.cloud import Cloud
from lmi_tests.evidence import CollectionError, ProvisioningError
from lmi_tests.shared_scenarios import ScenarioDriver
from lmi_tests.shared_contract import configuration_identity


class RunOwnership(Protocol):
    def assert_owned(self, resource_id: str) -> None: ...
    def finish_profile(self, resource_id: str, quiescent: bool) -> None: ...


class EvidenceBuilder(Protocol):
    def __call__(
        self, context: ScenarioContext, cloud: Cloud, *, phase: str
    ) -> dict[str, Any]: ...


class _Case:
    def __init__(self, driver: ScenarioDriver) -> None:
        self.driver = driver
        self.worker: asyncio.Task[Any] | None = None

    async def call(self, operation: Callable[[], Any]) -> Any:
        if self.worker is not None and not self.worker.done():
            raise CollectionError(
                "A previous case worker still owns blocking service calls"
            )
        self.worker = asyncio.create_task(asyncio.to_thread(operation))
        try:
            # Keep the worker task alive on outer cancellation so retirement can
            # join the actual thread before issuing cleanup writes.
            return await asyncio.shield(self.worker)
        except asyncio.CancelledError:
            self.driver.cloud.cancel()
            raise

    async def prepare_retirement(self) -> None:
        if self.worker is not None:
            try:
                await asyncio.shield(self.worker)
            except Exception:
                # The coordinator retains the original phase error; this only
                # confirms its service-call worker has really stopped.
                pass
            finally:
                if self.worker.done():
                    self.worker = None
        self.driver.cloud.begin_retirement()


class SharedAdapter:
    """Each shared case has one Cloud, worker, control namespace and evidence sink."""

    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        ownership: RunOwnership,
        evidence_builder: EvidenceBuilder,
        cloud_factory: Callable[..., Cloud] = Cloud,
    ) -> None:
        self.manifest = deepcopy(dict(manifest))
        self.sdk = self.manifest["sdk"]
        self.deployment = self.sdk["deployment"]
        self.artifacts_dir = Path(self.sdk["artifacts_dir"])
        self.ownership = ownership
        self.evidence_builder = evidence_builder
        self.cloud_factory = cloud_factory
        self.cases: dict[str, _Case] = {}

    async def prepare(self, profile: Profile) -> None:
        def verify() -> None:
            self.ownership.assert_owned(profile.resource_id)
            if profile.resource_id not in self.deployment["functions"].values():
                raise ProvisioningError(
                    "Shared profile is not part of the owned SDK deployment"
                )
            key = next(
                key
                for key, arn in self.deployment["functions"].items()
                if arn == profile.resource_id
            )
            if self.deployment["concurrencies"][key] != profile.native_concurrency:
                raise ProvisioningError(
                    "Native concurrency differs from the shared profile"
                )
            if profile.configuration_id != configuration_identity(self.deployment, key):
                raise ProvisioningError(
                    "Shared configuration identity differs from the deployed fixture"
                )
            cloud = self.cloud_factory(
                self.deployment, key, artifacts_dir=self.artifacts_dir / "configuration"
            )
            cloud.verify()

        await asyncio.to_thread(verify)

    def _case(self, context: ScenarioContext) -> _Case:
        try:
            return self.cases[context.scenario.case_id]
        except KeyError as error:
            raise CollectionError(
                "Case was not initialized by the shared adapter"
            ) from error

    async def start(self, context: ScenarioContext) -> None:
        if context.scenario.case_id in self.cases:
            raise CollectionError("A later scenario cannot replace an admitted case")
        await asyncio.to_thread(
            self.ownership.assert_owned, context.profile.resource_id
        )
        key = next(
            (
                key
                for key, arn in self.deployment["functions"].items()
                if arn == context.profile.resource_id
            ),
            None,
        )
        if key is None:
            raise ProvisioningError("Case target is outside the owned deployment")
        owner = hashlib.sha256(
            f"{context.scenario.case_id}:{context.scenario.cohort_id}".encode()
        ).hexdigest()[:20]
        cloud = await asyncio.to_thread(
            self.cloud_factory,
            self.deployment,
            key,
            artifacts_dir=self.artifacts_dir / "cases" / owner,
            case_context=context,
        )
        loop = asyncio.get_running_loop()

        def capacity(operation: Callable[[], Any]) -> None:
            cloud.check_active()
            pending = asyncio.run_coroutine_threadsafe(operation(), loop)
            try:
                while True:
                    try:
                        pending.result(timeout=0.2)
                        return
                    except FutureTimeout:
                        cloud.check_active()
            finally:
                if not pending.done():
                    pending.cancel()

        driver = ScenarioDriver(
            context,
            cloud,
            release_capacity=lambda: capacity(context.release_capacity),
            reserve_capacity=lambda: capacity(context.reserve_capacity),
        )
        case = self.cases[context.scenario.case_id] = _Case(driver)
        await case.call(driver.start)

    async def place(self, context: ScenarioContext) -> None:
        case = self._case(context)
        await case.call(case.driver.place)

    async def execute(self, context: ScenarioContext) -> None:
        case = self._case(context)
        await case.call(case.driver.execute)

    async def collect(self, context: ScenarioContext) -> dict[str, Any]:
        case = self._case(context)

        def collect() -> dict[str, Any]:
            case.driver.collect()
            return self.evidence_builder(
                context, case.driver.cloud, phase="observations"
            )

        return await case.call(collect)

    async def collect_partial(self, context: ScenarioContext) -> dict[str, Any]:
        """Retain a failed phase's facts before release, without awaiting cleanup."""
        case = self._case(context)

        def capture() -> dict[str, Any]:
            cloud = case.driver.cloud
            read_error = None
            try:
                cloud.collect(full_run=False)
            except Exception as error:
                read_error = (
                    f"Partial source collection: {type(error).__name__}: {error}"
                )
            cloud.observation_ended_at = time.time()
            partial = self.evidence_builder(context, cloud, phase="observations")
            if read_error is not None:
                partial.setdefault("collectionErrors", []).append(read_error)
                partial.setdefault("collection", {})["watermarks"] = {}
            cloud._save("shared-partial-evidence.json", partial)
            return partial

        return await case.call(capture)

    async def release(self, context: ScenarioContext) -> None:
        case = self.cases.get(context.scenario.case_id)
        if case is None:
            return
        await case.prepare_retirement()
        await case.call(case.driver.cloud.release_all)

    async def retire(self, context: ScenarioContext) -> bool:
        case = self.cases.get(context.scenario.case_id)
        if case is None:
            return True
        await case.prepare_retirement()

        def retire() -> bool:
            complete = case.driver.retire()
            final = self.evidence_builder(context, case.driver.cloud, phase="final")
            # The coordinator freezes observation evidence before fault release.
            # Only actual retirement facts may be appended by this phase.
            context.evidence["retirement"] = final.get("retirement", {})
            # Fresh completion facts prove physical retirement separately; they
            # must never replace the case's pre-release behavior observations.
            context.retirement_evidence = final
            return complete

        return await case.call(retire)

    async def finish(self, profile: Profile, *, quiescent: bool) -> None:
        await asyncio.to_thread(
            self.ownership.finish_profile, profile.resource_id, quiescent
        )


def create_adapter(manifest: Mapping[str, Any]) -> SharedAdapter:
    """Factory used by the shared CLI; persistent ownership is mandatory."""
    from lmi_tests.shared_evidence import build_evidence
    from lmi_tests.shared_lock import RunLease

    ownership = RunLease.from_manifest(manifest["sdk"])
    return SharedAdapter(manifest, ownership=ownership, evidence_builder=build_evidence)
