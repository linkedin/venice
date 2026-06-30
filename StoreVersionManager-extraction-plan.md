# Plan: complete the `StoreVersionManager` extraction

Branch: `eldernewborn/version-orchestrator`

Each PR below is its own reviewable increment. The through-line: every increment moves _fields/behavior_ into
`StoreVersionManager` and adds _explicitly-injected_ dependencies — never a `VeniceHelixAdmin` back-reference.
`StoreVersionManager` is a **per-cluster** instance, constructed and owned by `HelixVeniceClusterResources`, reached via
`getHelixVeniceClusterResources(cluster).getStoreVersionManager()`.

Baseline already landed (context, not a step): the version _reads_ — `getCurrentVersion`, `getFutureVersion`,
`getBackupVersion`, `getFutureVersionWithStatus`, `versionsForStore`, `getLargestUsedVersion` — plus the per-cluster
wiring and the isolated test harness. Numbering below continues from that baseline.

> Line numbers reflect `VeniceHelixAdmin.java` at the current baseline and will drift as PRs land.

## PR 2 — lifecycle setters (the real design win)

**Move:** `setStoreCurrentVersion` (both overloads, `:4825`/`:4833`), `setStoreLargestUsedVersion` (`:5025`),
`setStoreLargestUsedRTVersion` (`:5036`), `updateStoreVersionStatus` (`:5669`).

**The win:** today the first three route through `storeMetadataUpdate` (`:6076`), whose lambda is handed a live
`HelixVeniceClusterResources` (`(store, resources) -> …`, used for `resources.getVeniceVersionLifecycleEventManager()`
and `resources::isSourceCluster`). `StoreVersionManager` instead does its own
`lock → getStore → mutate → repository.updateStore`, deleting that leak for the version paths. `storeMetadataUpdate`
itself **stays** in `VeniceHelixAdmin` (still used by `setStoreOwner` and many non-version updaters).

**New constructor deps to inject** (per-cluster, all available in `HelixVeniceClusterResources`):

- `RealTimeTopicSwitcher` — for `transmitVersionSwapMessage`.
- `VeniceVersionLifecycleEventManager` — for `onCurrentVersionChanged`.
- `BiPredicate<String,String> sourceClusterChecker` — bind `resources::isSourceCluster` (narrow functional dep, not a
  god-class handle).
- `boolean isParent` (← `config.isParent()`) and `String regionName` — for the parent guard in `setStoreCurrentVersion`.

**Lock-type fidelity (must preserve exactly):** `storeMetadataUpdate` uses `createStoreWriteLock`;
`updateStoreVersionStatus` uses `createStoreWriteLockOnly` and keeps its `PARTIALLY_ONLINE`-only guard. Do not unify
them.

**Forwarders:** `checkControllerLeadershipFor` + store-exists precondition stay in `VeniceHelixAdmin` (mirroring
`checkPreConditionForUpdateStore`), then delegate.

**Tests:** add `StoreVersionManagerTest` cases — current-version validation / `enableWrites` guards, parent-skip path,
largest-used setters, the `PARTIALLY_ONLINE` transition guard. No more lambda interception needed.

## PR 3 — rollForward / rollback

**Move:** `rollForwardToFutureVersion` (`:4879`), `rollbackToBackupVersion` (`:4973`).

**New injected dep:** `HelixCustomizedViewOfflinePushRepository` — `rollForward`'s deferred-swap readiness check calls
`getReadyToServeInstances(topic, partition)`. (`rollback` reuses PR 2's deps +
`VersionLifecyclePolicy.getBackupVersionNumber`.)

**Stays in `VeniceHelixAdmin`:** the region-filter gate (`getRegionName` / `parseRegionsFilterList` /
`isRegionPartOfRegionsFilterList`) — controller-wide concern; gate in the forwarder, then delegate the metadata
mutation.

**Depends on PR 2** (shares the setter machinery).

**Tests:** the brittle `TestVeniceHelixAdmin.java:1244-1330` rollForward tests (which `mock(VeniceHelixAdmin.class)`,
`doCallRealMethod`, stub `getHelixVeniceClusterResources`, intercept the `storeMetadataUpdate` lambda, and use
`MockedStatic<RegionUtils>`) migrate to direct `StoreVersionManager` construction — most of that scaffolding disappears.

## PR 4 — deletion metadata

**Move (metadata only):** `deleteVersionFromStoreRepository` (`:4465`).

**Stays in `VeniceHelixAdmin`:** all orchestration in `deleteOneStoreVersion` (`:4112`/`:4128`),
`deleteAllVersionsInStore` (`:4053`), `deleteOldVersionInStore` (`:4081`) — Helix resource deletion, kill-push, topic
truncation, DaVinci push-status cleanup, view cleanup, pre/post hooks. These call
`StoreVersionManager.deleteVersionFromStoreRepository(...)` for the repo step.

**Independent** of PR 3 (could be parallelized).

**Tests:** unit-test the metadata deletion in isolation; verify `deleteOneStoreVersion` still fires its hooks
(`TestVeniceHelixAdminStoreLifecycleHooks`).

## Explicitly out of scope (separate project)

- `addVersion` (the ~450-line core) and the deletion _orchestration_ — too entangled with Helix / topic / push-monitor
  provisioning for a clean field+method move (scoping seams S1 / S2).
- `getLargestUsedVersionFromStoreGraveyard` stays in `VeniceHelixAdmin` (controller-wide graveyard, no per-cluster
  state, no leadership requirement).

## Decisions to confirm

1. **`isSourceCluster` as injected `BiPredicate`** (PR 2) — acceptable, or prefer a named narrow interface instead?
2. **Parent reachability** — keep `VeniceHelixAdmin` forwarders so `ParentVersionOrchestrator`'s
   `getVeniceHelixAdmin().X(...)` calls are untouched (recommended; keeps this project independent of the in-flight
   parent PR), vs. repointing the parent shim at `getStoreVersionManager()`.
3. **PR sequencing** — PR 3 depends on PR 2 (shared setter machinery); PR 4 is independent and could be parallelized.
