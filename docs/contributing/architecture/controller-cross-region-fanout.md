# Controller Cross-Region Fan-Out and Retry Behavior

This page documents how the Venice controller talks to other regions/fabrics, and the (currently non-uniform) retry
behavior of each cross-region call site. It is a reference for anyone touching the parent/child controllers or the
cross-region coordination layer.

> Line numbers below are indicative and drift as the files change; the stable anchor is the **method name**. Search for
> the method in `VeniceParentHelixAdmin` (parent) or `VeniceHelixAdmin` (child).

## Where the controller clients come from

Both controllers reach other regions through `ControllerClient` instances keyed by region/fabric. Ownership of those
client maps is centralized in `FabricControllerClientProvider`:

- `getControllerClientMap(clusterName)` — the standard per-cluster/per-fabric map (built from the child data-center URL
  and D2 allowlists).
- `getFabricBuildoutControllerClient(clusterName, fabric)` — a single client for a fabric that may be outside the
  standard allowlist (build-out / data-recovery destinations), cached separately.

`VeniceHelixAdmin` (the child) constructs one provider and exposes it via `getFabricControllerClientProvider()`;
`VeniceParentHelixAdmin` (the parent) shares the same instance. The child's `getControllerClientMap(...)` delegates to
the provider, and ~25 production call sites plus the parent's fan-out methods draw clients from it.

## The fan-out helper

Most "ask every region the same thing" loops follow one best-effort shape, captured by
`FabricControllerClientProvider.queryAllRegions(...)`:

```java
<R extends ControllerResponse, V> Map<String, V> queryAllRegions(
    Map<String, ControllerClient> controllerClients,
    String clusterName,
    int maxAttempts,                       // 1 = no retry; >1 wraps in ControllerClient.retryableRequest
    Function<ControllerClient, R> request, // the RPC to issue per region
    Function<R, V> onSuccess,              // map a successful response to a per-region value
    V errorSentinel)                       // value stored for a region whose query errored
```

For each region it runs `request` (retried when `maxAttempts > 1`), and on a per-region error it **logs and stores
`errorSentinel`** rather than aborting. The parent's multi-fabric version queries (`getCurrentVersionForMultiRegions`,
`getFutureVersionsForMultiColos`, `getBackupVersionsForMultiColos`) use it. The remaining fan-out sites still hand-roll
their loops because their error policy is bespoke (fail-fast, throw, region-filtered, error-collecting,
first-success-wins, fold/max, etc.).

## The two retry engines

There is **no single retry policy**. Cross-region calls that retry use one of two mechanisms, with different semantics:

### 1. `ControllerClient.retryableRequest(client, totalAttempts, request)`

- **Fixed delay** of `Utils.sleep(2000)` (2 s) between attempts — _not_ exponential. Five attempts therefore cost up to
  ~8 s of sleeping on the unhappy path.
- Stops early on success, on a "value schema not found" response, or when an optional `abortRetryCondition` returns true
  (the overload used by `queryAllRegions` passes `r -> false`).
- After exhausting attempts: **throws** `VeniceException` if the request threw an exception, but **returns the last
  error response** if it merely returned `isError()`.

### 2. `RetryUtils.executeWithMaxAttemptAndExponentialBackoff(op, attempts, initialDelay, maxDelay, maxTotalDelay, retryFailureTypes)`

- True **exponential backoff**, bounded by `maxDelay` and `maxTotalDelay`.
- Retries **only** on the listed exception types; other exceptions propagate immediately.
- Used by sites whose body throws a sentinel exception to signal "not done yet, retry the whole region sweep."

## Retry catalog

### Fan-out sites that retry (all use 5 attempts)

| Method                                        | Class:line  | RPC                          | Engine                                    | Backoff / cost                | Error policy                        |
| --------------------------------------------- | ----------- | ---------------------------- | ----------------------------------------- | ----------------------------- | ----------------------------------- |
| `getFutureVersionsForMultiColos`              | Parent:2264 | `getFutureVersions`          | `queryAllRegions(5)` → `retryableRequest` | fixed 2 s between (~8 s)      | sentinel                            |
| `isActiveActiveReplicationEnabledInAllRegion` | Parent:1702 | `getStore`                   | `retryableRequest(5)`                     | fixed 2 s between (~8 s)      | log+continue; throw on A/A mismatch |
| `rollForwardToFutureVersion`                  | Parent:2446 | `rollForwardToFutureVersion` | `RetryUtils` exp-backoff (5)              | 100 ms → 500 ms cap, 10 s max | throw; track `failedRegions`        |
| `pollChildRegionsForRollbackStatus`           | Parent:2603 | `getStore`                   | `RetryUtils` exp-backoff (5)              | 1 s → 10 s cap, 30 s max      | fold + full re-poll                 |
| `isRTTopicDeletionPermittedByAllControllers`  | Child:4267  | `getStore`                   | `RetryUtils` exp-backoff (5)              | 10 ms → 500 ms cap, 5 s max   | skip-missing; fail-fast → false     |

### Fan-out sites with no retry (single attempt)

| Method                                    | Class:line  | RPC                                         | Error policy             |
| ----------------------------------------- | ----------- | ------------------------------------------- | ------------------------ |
| `getCurrentVersionForMultiRegions`        | Parent:2301 | `getStore`                                  | sentinel                 |
| `getBackupVersionsForMultiColos`          | Parent:2276 | `getBackupVersions`                         | sentinel                 |
| `getInUseValueSchemaIds`                  | Parent:764  | `getInUseSchemaIds`                         | fail-fast → empty set    |
| `getOffLineJobStatus`                     | Parent:3451 | `getLeaderControllerUrl` / `queryJobStatus` | log → UNKNOWN, aggregate |
| `getClusterStaleStores`                   | Parent:4835 | `getClusterStores`                          | throw                    |
| `getLargestUsedVersionFromStoreGraveyard` | Parent:4906 | `getStoreLargestUsedVersion`                | fold/max                 |
| `getLargestUsedVersion`                   | Parent:4919 | `getStoreLargestUsedVersion`                | fold/max                 |
| `listStorePushInfo`                       | Parent:4955 | `getRegionPushDetails`                      | skip null/continue       |
| `checkResourceCleanupBeforeStoreCreation` | Parent:4982 | `checkResourceCleanupForStoreCreation`      | fail-fast throw          |
| `removeStoreFromGraveyard`                | Parent:5519 | `removeStoreFromGraveyard`                  | fail-fast throw          |
| `validateStoreDeleted`                    | Parent:5585 | `validateStoreDeleted`                      | collect errors           |
| `sendPushJobDetails`                      | Child:1623  | `sendPushJobDetails`                        | first-success-wins       |
| `getStoreInfoInChildColos`                | Child:1963  | `getStore`                                  | fail-fast throw          |
| `deleteRTTopicFromAllFabrics`             | Child:4335  | `deleteKafkaTopic`                          | log + continue           |

### Single-region calls (pick one region's client) — all no-retry

`getRepushInfo` (Parent), `getCurrentVersionInRegion` (Parent), `getOffLinePushStatus` single-region path (Parent),
`getStoreInChildRegion` (Parent), `initiateDataRecovery` / `prepareDataRecovery` / `isStoreVersionReadyForDataRecovery`
(Parent), `wipeCluster` (Parent), `compareStore` (Parent), `copyOverStoreSchemasAndConfigs` (Parent), `getStoreInfo`
(Child), and the store-migration source lookup (Child). Each issues a single attempt; the error policy is `throw`,
except `isStoreVersionReadyForDataRecovery` (collect) and `getCurrentVersionInRegion` (returns `-1`).

## Known inconsistencies and future work

- **The three sibling version queries disagree on retry:** `getFutureVersionsForMultiColos` retries (5× / fixed 2 s),
  while `getCurrentVersionForMultiRegions` and `getBackupVersionsForMultiColos` do not. The future-version retry was
  added deliberately (it gates a "don't start a push when a future version exists" decision), so unifying is a semantic
  choice, not a pure cleanup. Because all three now go through `queryAllRegions`, changing a policy is a one-number
  (`maxAttempts`) edit per site.
- **Two engines, four profiles:** any "unify retry" effort must pick both an engine (fixed-delay `retryableRequest` vs
  exponential `RetryUtils`) and a delay profile; the five retrying sites currently use four different profiles.
- **Bespoke loops remain hand-rolled:** only the three uniform best-effort version queries were migrated to
  `queryAllRegions`; the rest keep their own loops because their error policy differs. Consolidating them further would
  require a more configurable helper and careful behavior preservation.
