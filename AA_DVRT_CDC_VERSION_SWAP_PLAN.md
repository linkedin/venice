# AA DVRT CDC Version-Swap Coordination — Plan & Design

Working notes for PR [#2795 "[dvc][cc] Fix unsafe DVRT CDC version swap"](https://github.com/linkedin/venice/pull/2795).
This doc captures the problem, design, and remaining work so the context survives across sessions. It is a planning
artifact, not user-facing documentation — it can be dropped in a pre-merge cleanup.

## Status

- **PR:** #2795 (open, not draft) against `linkedin/venice`, head branch `worktree-dvrt-cdc-aa-version-swap`.
- **Mergeability:** no git conflicts, but **BLOCKED** on CI.
- **CI:** `IntegrationTests_67` and `IntegrationTests_87` failing; `E2ETestsFailureAlert` fires as the aggregate of
  those two. ~110 other checks green.
- **Review:** no maintainer decision yet. Only open threads are from the Copilot bot.
- **Last move:** HEAD reverts an earlier "tighten swap-settle window to cut shard runtime" test-timing tweak — the AA
  swap integration tests were being tuned for shard runtime/flakiness when work paused.

## Problem

The DVRT-based CDC consumer's per-partition version-swap flip is unsafe in three ways — the same three issues PR #2280
fixed on the legacy `VeniceChangelogConsumerImpl`, layered on top of the #2245 server-side multi-region
VersionSwapMessage (VSM) broadcast:

1. **Acts on VSMs from another region.** Under active-active the same logical swap is broadcast once per source region.
   The client must act only on VSMs whose `sourceRegion` matches its own region.
2. **Acts on stale VSMs from a previous version.** Re-pushes replay prior VTs' VSMs into the new VT;
   restart-from-EARLIEST and rollback can also replay historical VSMs. Must filter by `generationId` +
   `oldServingVersionTopic`/`newServingVersionTopic` and reject any swap targeting a version not greater than the
   highest already served (and reject `NON_EXISTING_VERSION`).
3. **Future-version ingestion outpaces current-version ingestion → data loss at swap.** The current-version transformer
   is throttled by the user's post-poll processing (records drain through `poll()` before more are ingested), but the
   future-version transformer's ingestion bypasses user post-poll. Whenever user post-poll is slow (the common case, not
   just A/A cross-DC skew) the future side can race past the swap point before the current side reaches it, silently
   swallowing in-flight records that should have surfaced on the current side.

All three are concrete safety bugs documented in #2245 and #2280. Issue 3 is more general than A/A — it is a property of
how DVRT CDC interleaves user post-poll back-pressure with version-specific ingestion.

## Design

New class `RecordTransformerVersionSwapCoordinator`
(`clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/`). Owned by
`VeniceChangelogConsumerDaVinciRecordTransformerImpl` when
`ChangelogClientConfig.isVersionSwapByControlMessageEnabled()` is true.

- **Barrier.** Enforces a cross-partition, cross-region barrier: a swap commits only after every assigned partition has
  observed VSMs from every region on **both** the current and future version topics. Per-partition × per-region
  observations are accumulated separately for the current (`currentVersionRegionsConsumed`) and future
  (`futureVersionRegionsConsumed`) sides.
- **State machine.** `IDLE → IN_PROGRESS → {COMMITTED, TIMED_OUT, FAILED} → IDLE`. Per-swap state (generationId, old/new
  VT, target version, the max-served snapshot, per-side region sets, paused partition sets) is reset on arm and after
  any terminal state.
- **VSM gating (`isRelevant`).** Rejects a VSM unless: `generationId != -1`; `sourceRegion` equals the client's region;
  both old/new serving VTs are present and the new one is a valid version topic; the topic matches this side (old for
  current, new for future); the target version is greater than `NON_EXISTING_VERSION` and greater than the arm-time
  max-served snapshot; and, while `IN_PROGRESS`, the `generationId` and old/new topics match the active swap.
  `maxServedVersion` is snapshotted once at arm-time so gating stays O(partitions), not O(partitions²).
- **Cutover / pause.** Once a partition's per-side barrier closes, Kafka prefetch is paused on that side.
  `consumer.pause()` does not truncate the in-flight batch, so records past the VSM in the same poll-batch still flow
  through `processPut`: surfaced on the current side, dropped on the future side.
- **Commit.** When the barrier closes for all assigned partitions, `partitionToVersionToServe` is flipped atomically for
  all of them inside the synchronized block, then future-side prefetch resumes.
- **Timeout watchdog.** A daemon `ScheduledExecutorService` force-commits (counts as SUCCESS, not failure) if the
  barrier does not close within `ChangelogClientConfig.setVersionSwapTimeoutInMs(...)`, bounding the vulnerable cutover
  window.
- **Mid-swap unsubscribe.** Removing a partition drops it from the barrier; if that unblocks the remaining set, the swap
  commits automatically.
- **Thread safety.** Every state-mutating method is `synchronized` on the instance; the watchdog timer contends for the
  same lock.

### Wiring

- `VeniceChangelogConsumerDaVinciRecordTransformerImpl.onVersionSwap()` has two branches — AA (coordinator) and legacy
  per-partition flip — selected by `isVersionSwapByControlMessageEnabled()`. The legacy path (`false`) must be preserved
  exactly.
- `InternalDaVinciRecordTransformer` gains the pause/resume partition-consumption hooks and a back-reference lifecycle
  hook so the coordinator can pause/resume the right side's Kafka prefetch.
- `StoreIngestionTask` wires the pause/resume lambdas to the version's Kafka consumer service at transformer
  construction and sets the back-reference post-construction.

## Known tradeoff

The coordinator intentionally allows record loss inside the cutover window, under the assumption that current and future
leaders process RT events in approximately aligned poll-batches. This is documented in the coordinator's class javadoc
and asserted in the AA integration tests. The timeout watchdog bounds the window. Any change to swap behavior must keep
the legacy path (`isVersionSwapByControlMessageEnabled == false`) untouched and respect the coordinator's at-least-once
guarantee scope.

## Files in the change

Main:

- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/RecordTransformerVersionSwapCoordinator.java`
  (new)
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/VeniceChangelogConsumerDaVinciRecordTransformerImpl.java`
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/InternalDaVinciRecordTransformer.java`
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/kafka/consumer/StoreIngestionTask.java`

Tests:

- `clients/da-vinci-client/src/test/java/com/linkedin/davinci/consumer/RecordTransformerVersionSwapCoordinatorTest.java`
  (new)
- `clients/da-vinci-client/src/test/java/com/linkedin/davinci/consumer/VeniceChangelogConsumerDaVinciRecordTransformerAaVersionSwapTest.java`
  (new)
- `clients/da-vinci-client/src/test/java/com/linkedin/davinci/consumer/VeniceChangelogConsumerDaVinciRecordTransformerImplTest.java`
- `clients/da-vinci-client/src/test/java/com/linkedin/davinci/transformer/RecordTransformerTest.java`
- `internal/venice-test-common/src/integrationTest/java/com/linkedin/venice/endToEnd/AbstractDvrtCdcAaVersionSwapTest.java`
  (new — shared base)
- `internal/venice-test-common/src/integrationTest/java/com/linkedin/venice/endToEnd/TestDvrtCdcAaVersionSwap.java`
  (new)
- `internal/venice-test-common/src/integrationTest/java/com/linkedin/venice/endToEnd/TestDvrtCdcAaVersionSwapMultiVersion.java`
  (new)

CI / infra:

- `.github/workflows/VeniceCI-E2ETests.yml`
- `internal/venice-test-common/test-shard-assignments.json`

## Remaining work

1. **Unblock CI.** Diagnose `IntegrationTests_67` and `IntegrationTests_87` — determine whether the failures are from
   this change or timing/flakiness in the AA swap e2e tests (the reverted swap-settle tweak suggests the latter was in
   play). Fix or re-stabilize, then confirm green.
2. **Get a maintainer review.** `reviewDecision` is empty; only Copilot has commented. Resolve the remaining bot threads
   and request a human reviewer.
3. **Pre-merge cleanup.** Remove this planning doc (or move it out of the diff) before merge.
