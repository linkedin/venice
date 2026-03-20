# Plan: Fix CDC Consumer Shutdown Bottleneck

## Problem

In a Flink CDC pipeline, when a task restarts, Flink gives the old task 30 seconds to close. The CDC consumer's
`close()` exceeds this timeout because `StoreIngestionTask.shutdownPartitionConsumptionStates()` serializes
per-partition Kafka unsubscriptions. Each partition waits up to 10 seconds in
`SharedKafkaConsumer.waitAfterUnsubscribe()`, and partitions sharing the same `SharedKafkaConsumer` serialize on
`consumerToLocks`. With 32 partitions across 12 consumers, the worst case is 12 consumers × 10s = 120s (sequential
iteration in `KafkaConsumerService.batchUnsubscribe()`), or 32 × 10s = 320s (per-partition `unSubscribe()` serialized on
`ConcurrentHashMap.compute(versionTopic)`).

When `close()` exceeds 30 seconds, `deregisterClient()` and `clearPartitionState()` in
`VeniceChangelogConsumerDaVinciRecordTransformerImpl.stop()` never execute. The factory returns the stale cached
consumer with populated `subscribedPartitions`, and the new task hits:
`"Cannot subscribe to partitions: [...] as they are already subscribed"`

## Scope

**In scope:**

- Fix the shutdown serialization bottleneck in `KafkaConsumerService.batchUnsubscribe()` (Option A)
- Restructure `shutdownPartitionConsumptionStates()` to batch-unsubscribe before per-partition checkpoint
- Move `shutdownLatch.countDown()` to `finally` block (Fix C — handles exception paths)
- Regression test reproducing the Flink "already subscribed" crash loop

**Out of scope:**

- Reordering `stop()` to call `deregisterClient()` before `daVinciClient.close()` (unsafe: old SIT's
  `internalClose.unsubscribeAll()` destroys new SIT's subscriptions — two SITs for the same version topic cannot
  coexist)
- Changing `waitAfterUnsubscribe` timeout to 0 (breaks event delivery — root cause unknown, confirmed across multiple
  attempts in this session)
- Modifying `VeniceChangelogConsumerDaVinciRecordTransformerImpl` consumer properties (setting
  `SERVER_INGESTION_CHECKPOINT_DURING_GRACEFUL_SHUTDOWN_ENABLED=false` also breaks event delivery)

## Assumptions

1. The per-consumer locks (`consumerToLocks`) are independent — different SharedKafkaConsumers have different locks.
   Parallelizing across consumers introduces no deadlock risk.
2. `ConsumptionTask.removeDataReceiver()` is thread-safe (uses `ConcurrentHashMap.remove()`).
3. `versionTopicToTopicPartitionToConsumer.compute()` is thread-safe (`ConcurrentHashMap`).
4. `SharedKafkaConsumer.batchUnsubscribe()` is `synchronized` on the consumer instance — only one thread can call it per
   consumer, which is guaranteed by `consumerToLocks`.
5. Existing `consumerBatchUnsubscribe()` on `StoreIngestionTask` (line 4225) and
   `AggKafkaConsumerService.batchUnsubscribeConsumerFor()` (line 496) are already production code.

## Files to Change

| File                                                                  | Change                                                                                                                                                                                                                                                                                |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `clients/da-vinci-client/.../KafkaConsumerService.java`               | Parallelize per-consumer iteration in `batchUnsubscribe()`                                                                                                                                                                                                                            |
| `clients/da-vinci-client/.../StoreIngestionTask.java`                 | (1) Add `consumerBatchUnsubscribeAllTopics()`, (2) call it in `shutdownPartitionConsumptionStates()` before parallel futures, (3) remove `consumerUnSubscribeAllTopics()` from `executeShutdownRunnable()`, (4) Fix C: `shutdownLatch.countDown()` in `finally` after `internalClose` |
| `clients/da-vinci-client/.../LeaderFollowerStoreIngestionTask.java`   | Override `consumerBatchUnsubscribeAllTopics()` — collect all PCS topic-partitions (respecting leader/follower), close VeniceWriter sub-partitions, call `consumerBatchUnsubscribe(allPartitions)`                                                                                     |
| `internal/venice-test-common/.../VersionSpecificCDCShutdownTest.java` | Regression test: reproduce "already subscribed" crash loop, verify fix                                                                                                                                                                                                                |

## Implementation Steps

### Step 1: Parallelize `KafkaConsumerService.batchUnsubscribe()`

**Current code** (line 327):

```java
consumerUnSubTopicPartitionSet.forEach((sharedConsumer, tpSet) -> {
    // per-consumer unsubscribe + waitAfterUnsubscribe(10s)
    // SEQUENTIAL — 12 consumers × 10s = 120s
});
```

**Change to:**

```java
// Per-consumer locks are independent — parallelize the per-consumer work.
// Each consumer's batchUnsubscribe + waitAfterUnsubscribe runs on its own thread.
// With N consumers: max(10s) instead of N × 10s.
List<CompletableFuture<Void>> futures = new ArrayList<>();
consumerUnSubTopicPartitionSet.forEach((sharedConsumer, tpSet) -> {
    futures.add(CompletableFuture.runAsync(() -> {
        ConsumptionTask task = consumerToConsumptionTask.get(sharedConsumer);
        try (AutoCloseableLock ignored = AutoCloseableLock.of(consumerToLocks.get(sharedConsumer))) {
            sharedConsumer.batchUnsubscribe(tpSet);
            tpSet.forEach(task::removeDataReceiver);
        }
        tpSet.forEach(tp -> versionTopicToTopicPartitionToConsumer.compute(versionTopic, ...));
    }));
});
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
```

**Verify:** Existing callers (`consumerBatchUnsubscribe` from SIT run loop) still work — parallel execution with 1-2
consumers is a no-op. Thread safety is guaranteed by independent per-consumer locks and ConcurrentHashMap operations.

### Step 2: Add `consumerBatchUnsubscribeAllTopics()` to SIT

Add to `StoreIngestionTask.java`:

```java
// Called once in shutdownPartitionConsumptionStates before the parallel checkpoint futures.
// Subclasses override to handle leader/follower topic selection.
protected void consumerBatchUnsubscribeAllTopics() {
    // Default: collect all version-topic partitions and batch unsubscribe
    Set<PubSubTopicPartition> allPartitions = new HashSet<>();
    for (PartitionConsumptionState pcs : partitionConsumptionStateMap.values()) {
        allPartitions.add(pcs.getReplicaTopicPartition());
    }
    if (!allPartitions.isEmpty()) {
        consumerBatchUnsubscribe(allPartitions);
    }
}
```

Override in `LeaderFollowerStoreIngestionTask.java`:

```java
@Override
protected void consumerBatchUnsubscribeAllTopics() {
    Set<PubSubTopicPartition> allPartitions = new HashSet<>();
    for (PartitionConsumptionState pcs : partitionConsumptionStateMap.values()) {
        PubSubTopic leaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
        int partitionId = pcs.getPartition();
        if (pcs.getLeaderFollowerState().equals(LEADER) && leaderTopic != null) {
            allPartitions.add(new PubSubTopicPartitionImpl(leaderTopic, partitionId));
        } else {
            allPartitions.add(new PubSubTopicPartitionImpl(versionTopic, partitionId));
        }
        // Close VeniceWriter sub-partitions (was in consumerUnSubscribeAllTopics)
        if (pcs.getVeniceWriterLazyRef() != null) {
            pcs.getVeniceWriterLazyRef().ifPresent(vw -> vw.closePartition(partitionId));
        }
    }
    if (!allPartitions.isEmpty()) {
        consumerBatchUnsubscribe(allPartitions);
    }
}
```

### Step 3: Restructure `shutdownPartitionConsumptionStates()`

In `StoreIngestionTask.shutdownPartitionConsumptionStates()`:

```java
void shutdownPartitionConsumptionStates() throws ... {
    // NEW: Batch-unsubscribe all partitions upfront. This replaces the per-partition
    // consumerUnSubscribeAllTopics() that was inside each executeShutdownRunnable.
    // With the parallelized KafkaConsumerService.batchUnsubscribe(), all consumers
    // unsubscribe simultaneously: ~10s total instead of N×10s sequential.
    consumerBatchUnsubscribeAllTopics();

    // ... existing parallel executor setup (unchanged) ...
    for (entry : partitionConsumptionStateMap.entrySet()) {
        executeShutdownRunnable(entry.getValue(), shutdownFutures, shutdownExecutor);
    }
    // ... existing allOf.get(60s) (unchanged) ...
}
```

### Step 4: Remove per-partition unsubscribe from `executeShutdownRunnable()`

```java
void executeShutdownRunnable(...) {
    Runnable shutdownRunnable = () -> {
        // REMOVED: consumerUnSubscribeAllTopics(partitionConsumptionState)
        // — already done in batch by consumerBatchUnsubscribeAllTopics() above

        // Unchanged: per-partition checkpoint (syncOffset + drain)
        if (getServerConfig().isServerIngestionCheckpointDuringGracefulShutdownEnabled()
            && !isGlobalRtDivEnabled()) {
            // ... syncOffset + drain ...
        }
    };
}
```

### Step 5: Fix C — `shutdownLatch.countDown()` in `finally`

Already implemented in current branch. Move `shutdownLatch.countDown()` from the `try` block to `finally` after
`internalClose(doFlush)`. Handles the exception path where `shutdownPartitionConsumptionStates()` throws (e.g., the 60s
`CompletableFuture.allOf()` timeout).

### Step 6: Regression test

`VersionSpecificCDCShutdownTest.testVersionSpecificCDCConsumerRestartWithinFlinkTimeout()`:

1. Setup store A and store B, both with version-specific CDC consumers
2. Store B consumer stays alive throughout (DaVinciBackend singleton stays up)
3. Store A consumer subscribes + polls events (verified working)
4. Close store A in background thread, wait Flink's 30s
5. **Assert close completed within 30s** — fails without fix (serialized unsubscribe = 30s+)
6. Create new consumer from same factory + subscribeAll
7. **Assert no "already subscribed" error** — fails without fix (deregisterClient never ran)
8. **Assert new consumer receives events** — end-to-end verification
9. **Assert store B still works** — no cross-store interference

## Acceptance Criteria

| #   | Criterion                                                                             | Verification                                                             |
| --- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| 1   | `KafkaConsumerService.batchUnsubscribe()` parallelizes per-consumer work              | Code review: `CompletableFuture.runAsync` per consumer entry             |
| 2   | `shutdownPartitionConsumptionStates()` does batch unsubscribe before parallel futures | Code review: `consumerBatchUnsubscribeAllTopics()` call before the loop  |
| 3   | `executeShutdownRunnable()` no longer calls `consumerUnSubscribeAllTopics()`          | Code review: line removed                                                |
| 4   | `shutdownLatch.countDown()` is in `finally` after `internalClose`                     | Code review + `javap` verification                                       |
| 5   | Regression test reproduces "already subscribed" without fix                           | Run test without fix → fails at close timeout assertion                  |
| 6   | Regression test passes with fix                                                       | Run test with fix → close < 30s, new consumer subscribes + receives data |
| 7   | Existing CDC tests pass                                                               | `StatefulVeniceChangelogConsumerTest` passes                             |
| 8   | Existing version-specific CDC tests pass                                              | `TestVersionSpecificChangelogConsumer` passes                            |
| 9   | da-vinci-client unit tests pass                                                       | `./gradlew :clients:da-vinci-client:test`                                |
| 10  | Server tests pass                                                                     | `./gradlew :services:venice-server:test`                                 |

## Verification Commands

```bash
# Step-by-step validation (use -Dretry.maxRetries=0 for fast feedback during development)

# 1. Compile
./gradlew :clients:da-vinci-client:compileJava

# 2. New regression test passes (clean modules first, avoid --rerun-tasks on integrationTest)
./gradlew :clients:da-vinci-client:clean :internal:venice-test-common:clean
./gradlew :internal:venice-test-common:integrationTest \
  --tests "com.linkedin.venice.consumer.VersionSpecificCDCShutdownTest"

# 3. Existing CDC tests pass
./gradlew :internal:venice-test-common:integrationTest \
  --tests "com.linkedin.venice.consumer.StatefulVeniceChangelogConsumerTest.testVeniceChangelogConsumerDaVinciRecordTransformerImpl"

# 4. Existing version-specific CDC tests pass
./gradlew :internal:venice-test-common:integrationTest \
  --tests "com.linkedin.venice.consumer.TestVersionSpecificChangelogConsumer"

# 5. da-vinci-client unit tests
./gradlew :clients:da-vinci-client:test

# 6. Server tests
./gradlew :services:venice-server:test
```

## Completion Checklist

- [ ] `KafkaConsumerService.batchUnsubscribe()` parallelized (Step 1)
- [ ] `consumerBatchUnsubscribeAllTopics()` added to SIT + LFSIT override (Step 2)
- [ ] `shutdownPartitionConsumptionStates()` calls batch unsubscribe before loop (Step 3)
- [ ] `consumerUnSubscribeAllTopics()` removed from `executeShutdownRunnable()` (Step 4)
- [ ] `shutdownLatch.countDown()` in `finally` (Step 5 — already done)
- [ ] Regression test written and verified (Step 6)
- [ ] All 6 verification commands pass (Acceptance Criteria 1-10)
