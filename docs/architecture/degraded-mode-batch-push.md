# Degraded-Mode Batch Push: Resilience to Datacenter Failures

## Problem Statement

Currently, a multi-datacenter batch push fails entirely if **any single DC** fails. This is enforced at three layers:

1. **Parent controller** (`VeniceParentHelixAdmin.getFinalReturnStatus()`): ERROR has higher priority than COMPLETED in
   status aggregation; an unreachable DC with terminal status forces ERROR.
2. **VPJ polling** (`VenicePushJob.pollStatusUntilComplete()`): Success requires all DCs to report completion —
   `completedDatacenters.size() != regionSpecificInfo.size()` causes an exception.
3. **Deferred swap** (`DeferredVersionSwapService`): A failed target region results in the version being KILLED.

The goal is to allow pushes to succeed in a "degraded mode" when a known-down DC fails but all healthy DCs complete
successfully.

---

## Current Multi-DC Push Architecture (Native Replication)

With native replication (NR), VPJ writes data directly to the Version Topic (VT) in the NR source fabric. Other DCs
replicate from that fabric's VT. The parent controller is control-plane only — it handles version creation, admin
messages, and status aggregation, but does NOT host a "parent VT" for data.

### Status Aggregation Flow

```
┌─────────────────────────────────────────────────┐
│  Child Controller (per DC)                       │
│  Replicas → Partition Status (PushStatusDecider) │
│  Partitions → Overall DC Status                  │
│  Strategy: WaitAll or WaitNMinusOne              │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│  Parent Controller (control plane only)          │
│  1. Query all DCs                                │
│  2. Sort statuses by STATUS_PRIORITIES           │
│  3. Majority check (>50% DCs reachable)          │
│  4. Terminal + unreachable DC → ERROR            │
│  Result: Aggregated status + per-region details  │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│  VenicePushJob Polling                           │
│  1. Poll parent controller periodically          │
│  2. Success: terminal + ALL DCs completed        │
│  3. Failure: terminal + ANY DC not completed     │
│  4. Timeout: UNKNOWN > 30 minutes                │
└─────────────────────────────────────────────────┘
```

### Key Enforcement Points

| Location             | File                                      | Behavior                                                    |
| -------------------- | ----------------------------------------- | ----------------------------------------------------------- |
| Status priority sort | `VeniceHelixAdmin.java:348-364`           | ERROR dominates COMPLETED in aggregation                    |
| Unreachable DC check | `VeniceParentHelixAdmin.java:4411-4421`   | Unreachable DC + terminal → forces ERROR                    |
| VPJ completion check | `VenicePushJob.java:2674`                 | All DCs must complete for success                           |
| VPJ PARTIALLY_ONLINE | `VenicePushJob.java:2727-2731`            | Throws exception on PARTIALLY_ONLINE                        |
| Deferred swap        | `DeferredVersionSwapService.java:502-504` | PARTIALLY_ONLINE is terminal — no further recovery          |
| Version retirement   | `VeniceHelixAdmin.java:4490`              | Child controller retires old versions after new goes ONLINE |

---

## Existing Building Blocks

| Component               | File                                      | What It Does                                                                                                                      |
| ----------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Targeted region push    | `AdminExecutionTask.java:769-773`         | `skipConsumption` for non-targeted regions (but NOT when deferred swap is enabled — see Critical Fix 1)                           |
| Deferred version swap   | `DeferredVersionSwapService.java:706-797` | Partial region rollforward, produces PARTIALLY_ONLINE                                                                             |
| Emergency source region | `VeniceHelixAdmin.java:7808-7847`         | Overrides NR source fabric selection at cluster level (highest priority in NR source selection chain)                             |
| Data recovery           | `DataRecoveryManager.java:79-117`         | Replays missed version by having recovering DC replicate from NR source fabric's VT (always available for current/backup version) |
| Per-DC version tracking | `StoreInfo.coloToCurrentVersions`         | Tracks current version independently per DC                                                                                       |
| PARTIALLY_ONLINE status | `VersionStatus.java:47-50`                | Represents "serving in some DCs but not all"                                                                                      |
| WaitNMinusOne strategy  | `WaitNMinusOnePushStatusDecider.java`     | Tolerates 1 replica failure per partition (per-replica, not per-DC)                                                               |

---

## Critical Issues Any Approach Must Handle

| #   | Issue                                                                                                                   | Impact                                                                                                                                                                                                                                                                                  | Resolution                                                                                         |
| --- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| 1   | **VPJ throws on `PARTIALLY_ONLINE`**                                                                                    | Any approach producing PARTIALLY_ONLINE causes VPJ to fail                                                                                                                                                                                                                              | Step 3: VPJ accepts PARTIALLY_ONLINE when degraded mode info returned in `VersionCreationResponse` |
| 2   | **VT availability during recovery** — recovery needs the VT in the NR source fabric to replay data to the recovering DC | Not an issue: the current version's VT is never cleaned up, and Venice's backup version retention guarantees the VT survives one version transition. If a normal push targets the recovering DC, it naturally heals it — no explicit recovery needed. Parent VT truncation is harmless. | No special handling needed — existing backup version retention is sufficient                       |
| 3   | **NR source fabric = degraded DC** — if degraded DC is the NR source, VPJ cannot write data and no DC can replicate     | Must redirect NR source to a healthy DC so VPJ writes there and other DCs replicate from it                                                                                                                                                                                             | Step 1: Set cluster-level `emergencySourceRegion` to a healthy DC                                  |
| 4   | **Admin message queue creates ghost versions** — `skipConsumption` is bypassed when `versionSwapDeferred=true`          | Degraded DC creates a version it cannot ingest                                                                                                                                                                                                                                          | **Critical Fix 1**: Modify `AdminExecutionTask` to skip for degraded DCs                           |
| 5   | **Incremental push on stale base diverges data** — non-AA stores get inconsistent state                                 | Data divergence                                                                                                                                                                                                                                                                         | Step 6: Block incremental push for non-AA stores with degraded DC                                  |

---

## Design Options Evaluated

### Option A: Modify Core Aggregation Logic ("N-1 DC Strategy")

Add `PUSH_JOB_TOLERATED_DC_FAILURES` config. Change `getFinalReturnStatus()` to count completed vs errored DCs instead
of taking highest-priority status. Relax VPJ success condition.

- **Pros**: Simple concept, backwards-compatible
- **Cons**: No intelligence about which DC is down; requires changes to aggregation logic (~8+ files); ghost version
  problem in recovering DCs

### Option B: Known-Degraded DC Allow-List with Custom Aggregation

Operator marks DC as degraded. Filter degraded DCs from `getFinalReturnStatus()` aggregation. VPJ excludes degraded DCs
from completion check.

- **Pros**: Explicit, operator-controlled
- **Cons**: Requires new aggregation logic; ghost version problem; ~8+ files changed

### Option C: Extend Targeted Region Push (Recommended)

Auto-convert regular pushes to targeted region pushes that exclude degraded DCs. Leverage existing deferred swap
infrastructure.

- **Pros**: Minimal new code; reuses proven infrastructure; ~6-8 files changed
- **Cons**: Requires operator intervention to mark/unmark degraded DCs; requires fix to `skipConsumption` logic

### Option D: Automatic DC Health Detection

Parent controller auto-detects unhealthy DCs via cross-store failure correlation.

- **Pros**: Fully automated
- **Cons**: High complexity; false positive risk; new subsystem required

### Option E: Two-Phase Push with Automatic Retry

Push to all DCs; on failure, swap in healthy DCs and queue retry for failed DC.

- **Pros**: Best user experience
- **Cons**: Very complex; version divergence risk; ~8+ files; race conditions with subsequent pushes

---

## Recommended Approach: Extend Targeted Region Push (Option C)

### Architecture

```
Operator marks DC as degraded (with optional auto-unmark timeout)
  - If degraded DC is NR source, set cluster-level
    emergencySourceRegion to redirect VPJ writes + replication
        ↓
Push submitted (normal VPJ)
        ↓
Controller auto-converts to targeted region push
  - targetedRegions = all DCs except degraded
  - versionSwapDeferred = true
  - Degraded DC: skipConsumption enforced (Critical Fix 1)
  - Controller returns degraded mode info + NR source Kafka URL
    in VersionCreationResponse (Critical Fix 2)
        ↓
VPJ writes data directly to NR source fabric's VT
  (possibly redirected if degraded DC was the NR source)
  - Other healthy DCs replicate from NR source fabric's VT
  - Degraded DC skips consumption entirely
        ↓
Push completes in healthy DCs
  - DeferredVersionSwapService swaps in healthy DCs
  - Version status = PARTIALLY_ONLINE
  - Current version VT intact in NR source fabric (no special retention needed)
        ↓
VPJ detects degraded mode from VersionCreationResponse
  - Enters deferred swap monitoring path
  - Accepts PARTIALLY_ONLINE as success
  - Logs warning with degraded DC details
        ↓
Operator unmarks DC when recovered (or auto-unmark after timeout)
  - Bulk recovery orchestrator triggers DataRecoveryManager per store
  - Recovering DC replicates from NR source fabric's VT (current version, always available)
  - OR: next normal push naturally targets recovering DC (no explicit recovery needed)
  - Cluster-level `emergencySourceRegion` cleared
  - Version transitions PARTIALLY_ONLINE → ONLINE after all stores recover
```

### Implementation Steps

#### Step 1: Controller API to Mark/Unmark DC as Degraded

- New admin APIs: `markDatacenterDegraded(dcName, timeoutMinutes)` / `unmarkDatacenterDegraded(dcName)` /
  `getDegradedDatacenters()`
- Store degraded DC set in ZooKeeper (cluster-level ZNode, e.g., `/venice-cluster/degraded-dcs`)
  - Use ZK versioned writes (compare-and-swap) to handle concurrent operator updates safely
  - Persists across controller leader failovers
  - Each entry stores: `{dcName, degradedTimestamp, timeoutMinutes, operatorId}`
- **Pre-condition**: If degraded DC is (or could be) the NR source fabric, set the existing cluster-level
  `emergencySourceRegion` config to a healthy DC. This is the highest priority in the NR source selection chain
  (`getNativeReplicationSourceFabric()`), so it immediately redirects all stores. For stores whose NR source was already
  a healthy DC, this is a harmless no-op.
- **Feature flag**: Gated behind cluster-level config `degradedModeEnabled` (default: `false`). Must be explicitly
  enabled before `markDatacenterDegraded` is accepted.

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/Admin.java` — new API methods
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceParentHelixAdmin.java` —
  implementation + cluster-level `emergencySourceRegion` management
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/server/AdminSparkServer.java` — REST
  endpoints
- `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/ControllerRoute.java` — new route entries
- `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/ControllerClient.java` — client methods for
  VPJ and admin tooling

#### Step 2: Auto-Convert Push to Targeted Region Push

At version creation time in `CreateVersion.java` (~line 398-415) and
`VeniceParentHelixAdmin.incrementVersionIdempotent()` (~line 1780):

- If degraded DCs exist AND `degradedModeEnabled=true` AND store is NOT hybrid:
  - Set `targetedRegions` to exclude degraded DCs
  - Enable `versionSwapDeferred = true`
  - Guard: if ALL DCs are degraded (empty healthy set), throw an error
  - Hybrid stores are excluded from auto-conversion (they have separate RT data flow)
- **Return degraded mode info in `VersionCreationResponse`** — add a new field `degradedDatacenters` (list of excluded
  DC names) so the VPJ can detect the auto-conversion and enter the correct polling code path. This fixes **Critical
  Flaw 2**.

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/server/CreateVersion.java`
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceParentHelixAdmin.java` —
  `addVersionAndStartIngestion()`
- `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/VersionCreationResponse.java` — add
  `degradedDatacenters` field

#### Step 3: Fix skipConsumption for Degraded DCs (Critical Fix 1)

The current `AdminExecutionTask.java:769-773` bypasses `skipConsumption` when `versionSwapDeferred=true`:

```java
if (skipConsumption && !isTargetRegionPushWithDeferredSwap) {
    // SKIP
} else {
    admin.addVersionAndStartIngestion(...); // Executes even for excluded DCs!
}
```

**Fix**: Add a degraded-DC-aware condition. When a DC is in the degraded set, it must skip consumption regardless of
`versionSwapDeferred`:

```java
boolean isDegradedDC = degradedDatacenters.contains(regionName);
if (skipConsumption && (!isTargetRegionPushWithDeferredSwap || isDegradedDC)) {
    // SKIP — degraded DCs always skip
} else {
    admin.addVersionAndStartIngestion(...);
}
```

The degraded DC set must be available to `AdminExecutionTask`. Options:

- Pass it in the admin message (new field in the ADD_VERSION message)
- Or read it from ZK at consumption time

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/kafka/consumer/AdminExecutionTask.java` — fix
  `skipConsumption` logic

#### Step 4: Handle PARTIALLY_ONLINE in VPJ (Critical Fix 2)

Modify VPJ to detect and handle auto-converted degraded pushes:

1. After receiving `VersionCreationResponse`, check the new `degradedDatacenters` field
2. If non-empty, set internal flag `isDegradedModePush = true` and set `isTargetRegionPushWithDeferredSwap = true`
3. In `pollStatusUntilComplete()`, when `isDegradedModePush=true`:
   - Accept `PARTIALLY_ONLINE` as a successful terminal state (guard exception at line 2727-2731)
   - Skip the `targetRegionSwapWaitTime` timeout (line 2742-2750) since the degraded DC will never complete during this
     push
   - Log a WARNING with the list of degraded DCs
   - Emit a metric `push.completed.degraded.count`

**Files:**

- `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java` — `pollStatusUntilComplete()`

#### Step 5: VT Retention — No Special Handling Needed

**No code changes required for VT retention.** The existing backup version retention policy is sufficient.

**Why**: Venice retains one backup version in addition to the current version. A PARTIALLY_ONLINE version produced by a
degraded-mode push is the current version in healthy DCs — its VT in the NR source fabric is never cleaned up. Recovery
targets the current version, whose VT is guaranteed to exist.

**Scenario analysis during recovery**:

| Scenario                                            | VT State                                             | Recovery Action                                              |
| --------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| No new push since degraded-mode push                | VT of current version exists in NR source fabric     | DataRecoveryManager replays current version to recovering DC |
| New push (v7) in progress                           | v6 is still current, VT intact                       | Recovery continues on v6                                     |
| New push (v7) completes mid-recovery                | v6 becomes backup, VT still intact (backup retained) | Recovery continues uninterrupted on v6                       |
| v7 is a normal push (DC unmarked, all DCs targeted) | v7 naturally targets recovering DC                   | **No explicit recovery needed** — v7 IS the recovery         |
| v7 is another degraded push (DC still excluded)     | v6 backup VT intact; v7 becomes current              | Abandon v6 recovery if incomplete; re-run for v7             |

**Parent VT note**: Parent VT truncation (by `handleTerminalJobStatus()` and `DeferredVersionSwapService`) is harmless —
it only affects the parent's local copy used for concurrent push detection, not the NR source fabric's data VT.

**Files:** None — no code changes needed for this step.

#### Step 6: Block ALL Incremental Pushes During Degraded Mode

In the incremental push entry point (`VeniceParentHelixAdmin.incrementVersionIdempotent()`):

- If `isDegradedModeEnabled` AND any DC is degraded AND the push is incremental:
  - Throw: "Incremental push blocked: DC(s) {names} are degraded."
- **All** incremental pushes are blocked, regardless of AA status. The original design allowed AA-enabled stores to
  proceed, but during implementation we decided to block all incremental pushes for simplicity and safety — even
  AA-enabled stores could have consistency issues if the degraded DC's RT is stale.

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceParentHelixAdmin.java`

#### Step 7: Version Retention — No Special Handling Needed

**No code changes required.** The existing backup version retention policy already provides the safety margin needed for
recovery:

- Venice retains one backup version in addition to the current version
- The current version's VT is never cleaned up
- If a new push completes during recovery, the previous version becomes backup — its VT is still intact
- A normal push targeting the recovering DC naturally heals it, making explicit recovery unnecessary

**Files:** None — no code changes needed for this step.

#### Step 8: Recovery When DC Comes Back

On `unmarkDatacenterDegraded(dcName)`:

1. **Enumerate affected stores**: Scan all stores for the highest PARTIALLY_ONLINE version per store.
2. **Bulk recovery orchestrator** (`DegradedModeRecoveryService`):
   - **Phase 1 — Initiation** (parallel, bounded thread pool, configurable size):
     - Pre-check: verify store exists and version still PARTIALLY_ONLINE
     - Resolve NR source fabric (respects emergencySourceRegion override)
     - `admin.prepareDataRecovery()` → `pollUntilReady()` → `admin.initiateDataRecovery()`
     - Retry up to 3x with exponential backoff on failure
   - **Phase 2 — Confirmation** (parallel, bounded monitor pool):
     - Poll child DC via `getCurrentVersionInRegion()` until version is current
     - Handle version supersession: if a newer version becomes current, treat as success (recovery moot)
     - On confirmation: `updateStoreVersionStatus()` transitions PARTIALLY_ONLINE → ONLINE
     - Re-verifies version is still PARTIALLY_ONLINE before transitioning (prevents stale updates)
   - Tracks progress: `{totalStores, recoveredStores, failedStores, versionsTransitioned}`
   - Exposes progress via REST API: `GET /get_recovery_progress?cluster=X&datacenter_name=Y`
   - Recovery keyed by `cluster/datacenter` to prevent multi-cluster collisions
   - Uses atomic `compute()` to prevent concurrent recovery races
3. **emergencySourceRegion reminder**: Logs ACTION REQUIRED if emergencySourceRegion is still set after all stores
   recover. Clearing is a manual operator action (static config).
4. **Leader failover safety**: Periodic DC monitor (60s) scans for orphaned PARTIALLY_ONLINE versions with no active
   recovery. Re-triggers recovery idempotently (prepare cleans up previous state).

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/DegradedModeRecoveryService.java`
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceParentHelixAdmin.java`
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/Admin.java`

#### Step 9: Degraded DC Duration Monitoring (Alert-Only)

A periodic background task (every 60 seconds) in `DegradedModeRecoveryService`:

- Emits `degraded_mode.dc.duration_minutes` gauge per cluster + region (alertable metric)
- If a DC has been degraded longer than `timeoutMinutes`: logs a WARN-level ALERT
- **No automatic unmark** — operator must decide when to unmark. The `timeoutMinutes` from `markDatacenterDegraded` is
  advisory only, used for alerting thresholds.

**Files:**

- `services/venice-controller/src/main/java/com/linkedin/venice/controller/DegradedModeRecoveryService.java`
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/stats/DegradedModeStats.java`

---

## Metrics and Observability

| Metric                                                | Type          | Description      |
| ----------------------------------------------------- | ------------- | ---------------- | ---------------------------------------------------------- |
| OTel Metric Name                                      | Type          | Dimensions       | Description                                                |
| ----------------------------------------------------- | ------------- | ---------------- | ---------------------------------------------------------- |
| `degraded_mode.dc.active_count`                       | Gauge         | -                | Number of currently degraded DCs                           |
| `degraded_mode.dc.duration_minutes`                   | Gauge (ALERT) | cluster, region  | How long each DC has been degraded. Set alerts on this.    |
| `degraded_mode.push.auto_converted_count`             | Counter       | cluster, store   | Pushes auto-converted to targeted region push              |
| `degraded_mode.push.blocked_incremental_count`        | Counter       | cluster, store   | Incremental pushes blocked due to degraded DC              |
| `degraded_mode.recovery.store_success_count`          | Counter       | cluster, store   | Individual store recoveries initiated successfully         |
| `degraded_mode.recovery.store_failure_count`          | Counter       | cluster, store   | Individual store recoveries failed after all retries       |
| `degraded_mode.recovery.version_transitioned_count`   | Counter       | cluster, store   | Versions transitioned PARTIALLY_ONLINE -> ONLINE           |
| `degraded_mode.recovery.progress`                     | Gauge         | -                | Recovery progress (0.0 to 1.0)                             |
| `degraded_mode.recovery.store_duration_ms`            | Gauge avg/max | cluster, store   | Per-store recovery latency in milliseconds                 |

---

## Edge Cases & Mitigations

| Edge Case                                | Mitigation                                                                                                                                                                              |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Degraded DC is NR source fabric          | Step 1: Set cluster-level `emergencySourceRegion` to a healthy DC. Redirects VPJ writes and cross-DC replication for all stores. Harmless no-op for stores already using the target DC. |
| Incremental push during degraded mode    | Step 6: Block ALL incremental pushes when any DC is degraded (regardless of AA status)                                                                                                  |
| Hybrid/AA stores with RT writes          | Self-healing via conflict resolution once DC catches up; acceptable                                                                                                                     |
| Repush during degraded mode              | No special handling — repush also auto-excludes degraded DC; current version VT always available                                                                                        |
| Next push while DC catching up           | New push also auto-excludes still-recovering DC (same mechanism)                                                                                                                        |
| Multiple DCs fail simultaneously         | Existing majority-reachable check prevents push if >50% DCs down                                                                                                                        |
| Ghost versions in degraded DC            | Step 3: Fix `AdminExecutionTask` to enforce `skipConsumption` for degraded DCs                                                                                                          |
| VPJ doesn't know about auto-conversion   | Step 2: Return `degradedDatacenters` in `VersionCreationResponse`; Step 4: VPJ detects and adapts                                                                                       |
| `emergencySourceRegion` is cluster-level | Step 1: Use the existing cluster-level config — it's the simplest approach and harmless for unaffected stores                                                                           |
| Source version retired before recovery   | Not a concern — current version VT always exists; backup version retention provides one version of safety margin; normal push naturally heals recovering DC                             |
| Operator forgets to unmark DC            | Step 9: Auto-unmark timeout with alerts                                                                                                                                                 |
| No way to disable degraded mode quickly  | Step 1: Gated behind `degradedModeEnabled` feature flag                                                                                                                                 |
| Compression dictionary availability      | Not a concern — dictionary is in the SOP message of the current version's VT, which is always available. Parent VT truncation is harmless.                                              |
| Controller leader failover               | Step 1: Degraded DC set stored in ZK, survives failover. `emergencySourceRegion` is a controller config that persists across restarts.                                                  |
| Concurrent operator mark/unmark          | Step 1: ZK versioned writes (CAS) prevent lost updates on degraded DC set                                                                                                               |
| In-flight push when DC marked degraded   | Does not affect current push (only affects new pushes at version creation). Operator should wait for in-flight push to complete/fail before marking.                                    |
| Store migration during degraded mode     | `DataRecoveryManager` rejects recovery for migrating stores. Recovery must wait until migration completes. Alert operator.                                                              |
| PARTIALLY_ONLINE versions accumulate     | Normal version retirement applies — no special handling needed; recovery targets latest version only                                                                                    |
| Recovery fails for some stores           | Step 8: Retry with limit; expose failures via `getRecoveryProgress()` API; alert on persistent failures                                                                                 |
| Post-recovery data correctness           | Step 8: Validate partition count and record count between source and recovered DC                                                                                                       |

---

## Industry Alignment

This design was validated against industry patterns. Full analysis in `docs/architecture/degraded-mode-review.md`.

| Aspect             | Venice                             | Industry Norm                                                                         | Assessment                                                                               |
| ------------------ | ---------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Core pattern       | Exclude unhealthy DC from writes   | Cassandra `LOCAL_QUORUM`, Kafka independent consumers, DynamoDB regional independence | Well-aligned                                                                             |
| Detection          | Operator-driven                    | Mostly automatic (CockroachDB Raft, Cassandra gossip)                                 | Conservative but justified — batch pushes not latency-critical, false-positive cost high |
| Degradation status | Explicit `PARTIALLY_ONLINE`        | Usually implicit                                                                      | Strength — better operational visibility                                                 |
| Recovery           | Version replay from sibling DC     | Cassandra anti-entropy repair, CockroachDB Raft snapshot, Kafka offset replay         | Well-aligned; simpler due to atomic version model                                        |
| Success criteria   | All non-degraded DCs must complete | Tunable quorum (Cassandra)                                                            | Simpler; appropriate for version-level pushes                                            |

### Gaps addressed from industry review:

- Auto-unmark timeout (circuit breaker pattern) → Step 9
- Recovery progress tracking → Step 8 with `getRecoveryProgress()` API
- Feature flag for quick disable → Step 1 with `degradedModeEnabled`
- Metrics/observability → Metrics section added

### Deferred to future phases:

- Per-store degradation policy (some stores may prefer to fail rather than proceed without a DC)
- Automatic DC health detection (auto-suggest degradation based on cross-store failure correlation)
- Pre-degradation health signals (per-DC push health metrics)

---

## Verification Plan

1. **Unit tests**: Mock degraded DC set in controller tests — verify auto-conversion to targeted region push
2. **AdminExecutionTask test**: Verify `skipConsumption` is enforced for degraded DCs even with
   `versionSwapDeferred=true`
3. **VPJ test**: Verify VPJ detects `degradedDatacenters` in `VersionCreationResponse` and accepts `PARTIALLY_ONLINE`
4. **Integration test**: Extend `TestDeferredVersionSwapWithFailingRegions.java` to cover:
   - Mark DC as degraded → push succeeds in healthy DCs → version is PARTIALLY_ONLINE
   - Unmark DC → data recovery triggered → version transitions to ONLINE
5. **NR source fabric test**: Mark NR source as degraded → verify cluster-level `emergencySourceRegion` is set → push
   uses alternate source
6. **Incremental push guard test**: With degraded DC, attempt incremental push on non-AA store → verify it is blocked
7. **VT availability test**: Verify current version VT exists in NR source fabric after degraded-mode push; verify
   backup version retention preserves VT through one version transition
8. **Feature flag test**: Verify `markDatacenterDegraded` is rejected when `degradedModeEnabled=false`
9. **End-to-end multi-DC test**: Use `ServiceFactory` multi-region wrapper to simulate full degraded-mode lifecycle
