package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.UNIQUE_KEY_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


/**
 * Integration-style tests: full lifecycle scenarios through PCS+OffsetRecord persistence.
 * Covers push types, crash/recovery, leader transition, version swap, chunking, mixed deployment.
 * No duplication with UniqueKeyCountTest which covers field operations and encoding logic.
 */
public class UniqueKeyCountIntegrationTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String LOCAL_REGION = "dc-1";
  private static final String TEST_PREFIX = "test_prefix";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final PubSubTopicPartition TP = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("store_v1"), 0);

  // Helpers (shared, no duplication)

  private PartitionConsumptionState freshPcs() {
    return new PartitionConsumptionState(
        TP,
        new OffsetRecord(
            AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
            DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
  }

  private PartitionConsumptionState restoreFrom(OffsetRecord checkpoint) {
    return new PartitionConsumptionState(
        TP,
        checkpoint,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
  }

  /** Simulate syncOffset: copy PCS count into OffsetRecord, serialize, deserialize. */
  private OffsetRecord checkpoint(PartitionConsumptionState pcs) {
    OffsetRecord or = pcs.getOffsetRecord();
    or.setUniqueKeyCount(pcs.getUniqueKeyCount());
    byte[] bytes = or.toBytes();
    return new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
  }

  private void doBatch(PartitionConsumptionState pcs, int count) {
    for (int i = 0; i < count; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    pcs.finalizeUniqueKeyCountForBatchPush();
  }

  /**
   * Simulates batch ingestion for a chunked store. For each logical key, there are
   * {@code chunksPerKey} chunk fragment records (schemaId=CHUNK, NOT counted) followed
   * by 1 manifest record (schemaId=CHUNKED_VALUE_MANIFEST, counted).
   * Only the manifest calls incrementUniqueKeyCountForBatchRecord — this mirrors the filtering in
   * processKafkaDataMessage where schemaId != CHUNK_SCHEMA_ID gates the counter.
   */
  private void doBatchChunked(PartitionConsumptionState pcs, int logicalKeyCount, int chunksPerKey) {
    for (int key = 0; key < logicalKeyCount; key++) {
      // Chunk fragments: schemaId == CHUNK → filtered out by processKafkaDataMessage, NOT counted
      // (we simply don't call incrementUniqueKeyCountForBatchRecord for these)

      // Manifest: schemaId == CHUNKED_VALUE_MANIFEST → passes filter, counted
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    pcs.finalizeUniqueKeyCountForBatchPush();
  }

  // Push Type: Standard Batch (non-chunked, no RMD)

  @Test
  public void testStandardBatchPushNonChunked() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 100);
    assertEquals(pcs.getUniqueKeyCount(), 100L);
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 100L, "Persisted count survives round-trip");
  }

  // Push Type: Standard Batch (chunked — only manifests counted)

  @Test
  public void testStandardBatchPushChunked() {
    // 10 logical keys × (1 manifest + 3 chunk fragments) = 40 records ingested
    // Only 10 manifests pass the schemaId != CHUNK filter → uniqueKeyCount = 10
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 10, 3);
    assertEquals(pcs.getUniqueKeyCount(), 10L, "Chunked: logical key count, not total record count");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 10L, "Persisted count survives round-trip");
  }

  // Push Type: Repush / KIF (already has RMD — VALUE_AND_RMD path, our code never fires)

  @Test
  public void testRepushWithExistingRMD() {
    // Repush records have RMD → getStorageOperationTypeForPut returns VALUE_AND_RMD.
    // The VALUE case (our batch RMD code) is never reached.
    // Batch counter still counts because it runs in processKafkaDataMessage (above storage layer).
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 50); // VPJ de-duped keys, same count whether standard or repush
    assertEquals(pcs.getUniqueKeyCount(), 50L, "Repush: batch counter counts logical keys regardless of RMD path");
  }

  // Push Type: Incremental Push (writes to RT → leader-computed signals)

  @Test
  public void testIncrementalPush() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 30);
    assertEquals(pcs.getUniqueKeyCount(), 30L);

    // Inc push: 10 new keys (not in batch), 5 updates to batch keys
    for (int i = 0; i < 10; i++) {
      pcs.incrementUniqueKeyCount(); // new key via leader signal computation
    }
    // 5 updates: wasAlive=true, isAlive=true → signal=0 (no change)
    assertEquals(pcs.getUniqueKeyCount(), 40L);
  }

  // Hybrid A/A: Full lifecycle (batch → new keys → updates → deletes → re-puts)

  @Test
  public void testHybridAAFullLifecycle() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 50);

    // RT: +10 new, 0 for 5 updates, -3 deletes, +2 re-puts
    for (int i = 0; i < 10; i++) {
      pcs.incrementUniqueKeyCount();
    }
    for (int i = 0; i < 3; i++) {
      pcs.decrementUniqueKeyCount();
    }
    for (int i = 0; i < 2; i++) {
      pcs.incrementUniqueKeyCount();
    }
    assertEquals(pcs.getUniqueKeyCount(), 59L); // 50 + 10 - 3 + 2
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 59L);
  }

  // Follower Lifecycle (batch + "kcs" headers)

  @Test
  public void testFollowerLifecycle() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 8);

    pcs.incrementUniqueKeyCount(); // new key
    pcs.decrementUniqueKeyCount(); // delete
    pcs.incrementUniqueKeyCount(); // new key
    // No header → signal=0 → no call (5 updates)
    assertEquals(pcs.getUniqueKeyCount(), 9L); // 8 + 1 - 1 + 1
  }

  // Leader-Follower Convergence (same data → same count)

  @Test
  public void testLeaderFollowerConverge() {
    PartitionConsumptionState leader = freshPcs();
    PartitionConsumptionState follower = freshPcs();

    doBatch(leader, 50);
    doBatch(follower, 50);

    // Leader: leader signal → +3, -1
    leader.incrementUniqueKeyCount();
    leader.incrementUniqueKeyCount();
    leader.incrementUniqueKeyCount();
    leader.decrementUniqueKeyCount();

    // Follower: same signals via headers
    follower.incrementUniqueKeyCount();
    follower.incrementUniqueKeyCount();
    follower.incrementUniqueKeyCount();
    follower.decrementUniqueKeyCount();

    assertEquals(leader.getUniqueKeyCount(), follower.getUniqueKeyCount(), "Must converge");
    assertEquals(leader.getUniqueKeyCount(), 52L);
  }

  // Crash/Restart During Batch

  @Test
  public void testCrashDuringBatch() {
    PartitionConsumptionState pcs = freshPcs();
    for (int i = 0; i < 30; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(pcs.getUniqueKeyCount(), 30L, "uniqueKeyCount grows during batch for checkpoint safety");

    // Simulate mid-batch checkpoint: uniqueKeyCount=30 persisted
    OffsetRecord midBatchCheckpoint = checkpoint(pcs);
    assertEquals(midBatchCheckpoint.getUniqueKeyCount(), 30L);

    // Crash! Restart from checkpoint. uniqueKeyCount restored to 30.
    PartitionConsumptionState restarted = restoreFrom(midBatchCheckpoint);
    assertEquals(restarted.getUniqueKeyCount(), 30L);

    // Replay only post-checkpoint records (20 remaining)
    for (int i = 0; i < 20; i++) {
      restarted.incrementUniqueKeyCountForBatchRecord();
    }
    restarted.finalizeUniqueKeyCountForBatchPush();
    assertEquals(restarted.getUniqueKeyCount(), 50L, "30 from checkpoint + 20 replayed = 50");
  }

  // Crash/Restart During RT (Follower)

  @Test
  public void testFollowerCrashDuringRT() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 40);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 41L);

    // checkpoint at uniqueKeyCount=41
    OffsetRecord cp = checkpoint(pcs);

    // Crash! Restart from checkpoint
    PartitionConsumptionState restarted = restoreFrom(cp);
    assertEquals(restarted.getUniqueKeyCount(), 41L);

    // Replay VT from checkpoint: re-apply headers for records after checkpoint
    restarted.incrementUniqueKeyCount();
    restarted.decrementUniqueKeyCount();
    assertEquals(restarted.getUniqueKeyCount(), 41L, "Replayed signals net to 0");
  }

  // Crash/Restart During RT (Leader)

  @Test
  public void testLeaderCrashDuringRT() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 30);
    for (int i = 0; i < 5; i++) {
      pcs.incrementUniqueKeyCount();
    }
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 34L);

    OffsetRecord cp = checkpoint(pcs);

    // Leader crash! Promoted follower restores from its own checkpoint (same count)
    PartitionConsumptionState promotedFollower = restoreFrom(cp);
    assertEquals(promotedFollower.getUniqueKeyCount(), 34L);

    // New leader's signal computation takes over
    promotedFollower.incrementUniqueKeyCount();
    assertEquals(promotedFollower.getUniqueKeyCount(), 35L);
  }

  // Leader Transition (no crash — PCS preserved)

  @Test
  public void testFollowerPromotedSeamlessly() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 20);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 21L);

    // Promoted: same PCS object, leader signal computation resumes from base=21
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 21L);
  }

  // Version Swap (independent counts)

  @Test
  public void testVersionSwap() {
    PartitionConsumptionState v1 = freshPcs();
    doBatch(v1, 50);
    v1.incrementUniqueKeyCount();
    assertEquals(v1.getUniqueKeyCount(), 51L);

    PartitionConsumptionState v2 = freshPcs();
    doBatch(v2, 60);
    assertEquals(v2.getUniqueKeyCount(), 60L);

    assertEquals(v1.getUniqueKeyCount(), 51L, "v1 unchanged by v2");
    assertEquals(v2.getUniqueKeyCount(), 60L, "v2 independent of v1");
  }

  // New Standby Bootstrap From VT Offset 0

  @Test
  public void testNewStandbyBootstrap() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 25);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 26L, "VT self-describing: batch + headers = complete count");
  }

  // Blob Transfer

  @Test
  public void testBlobTransfer() {
    PartitionConsumptionState src = freshPcs();
    doBatch(src, 15);
    for (int i = 0; i < 5; i++) {
      src.incrementUniqueKeyCount();
    }
    src.decrementUniqueKeyCount();
    assertEquals(src.getUniqueKeyCount(), 19L);

    // Serialize for blob transfer
    OffsetRecord transferred = checkpoint(src);
    assertEquals(transferred.getUniqueKeyCount(), 19L);

    // Receiver restores and catches up
    PartitionConsumptionState receiver = restoreFrom(transferred);
    assertEquals(receiver.getUniqueKeyCount(), 19L);
    receiver.incrementUniqueKeyCount();
    assertEquals(receiver.getUniqueKeyCount(), 20L);
  }

  // Delete-then-Re-PUT Cycles

  @Test
  public void testDeleteRePutCycleIsReversible() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 10);

    // DELETE-REPUT-DELETE-REPUT
    pcs.decrementUniqueKeyCount(); // 9
    pcs.incrementUniqueKeyCount(); // 10
    pcs.decrementUniqueKeyCount(); // 9
    pcs.incrementUniqueKeyCount(); // 10
    assertEquals(pcs.getUniqueKeyCount(), 10L, "DELETE-REPUT cycles are reversible");
  }

  // Data Recovery

  @Test
  public void testDataRecovery() {
    // Recovery re-ingests batch (DCR skipped), then RT leader signal computation resumes
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 45);
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 46L);
  }

  // Multiple Partitions Independent

  @Test
  public void testMultiplePartitionsIndependent() {
    PartitionConsumptionState[] parts = new PartitionConsumptionState[3];
    for (int p = 0; p < 3; p++) {
      PubSubTopicPartition tp = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("store_v1"), p);
      parts[p] = new PartitionConsumptionState(
          tp,
          new OffsetRecord(
              AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
              DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING),
          DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
          true,
          Schema.create(Schema.Type.STRING));
    }
    doBatch(parts[0], 100);
    doBatch(parts[1], 200);
    doBatch(parts[2], 300);

    parts[0].incrementUniqueKeyCount();
    parts[1].decrementUniqueKeyCount();

    assertEquals(parts[0].getUniqueKeyCount(), 101L);
    assertEquals(parts[1].getUniqueKeyCount(), 199L);
    assertEquals(parts[2].getUniqueKeyCount(), 300L, "Partition 2 unchanged");
  }

  // Large-Scale

  @Test
  public void testLargeScale() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 1_000_000);
    assertEquals(pcs.getUniqueKeyCount(), 1_000_000L);
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 1_000_000L, "Large count survives serialization");
  }

  // Edge: Batch With DELETEs (repush — only PUTs counted)

  @Test
  public void testBatchWithDeletesOnlyCountsPuts() {
    // 80 PUTs + 20 DELETEs during batch → only 80 counted
    // (processKafkaDataMessage filters: messageType == PUT)
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 80); // only PUTs reach incrementUniqueKeyCountForBatchRecord
    assertEquals(pcs.getUniqueKeyCount(), 80L);
  }

  // Edge: Empty Partition

  @Test
  public void testEmptyPartition() {
    PartitionConsumptionState pcs = freshPcs();
    pcs.finalizeUniqueKeyCountForBatchPush(); // 0 records
    assertEquals(pcs.getUniqueKeyCount(), 0L, "Empty partition → 0, not -1");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 0L);
  }

  // Edge: Multi-Fabric Independent

  @Test
  public void testMultiFabricIndependent() {
    PartitionConsumptionState f1 = freshPcs();
    PartitionConsumptionState f2 = freshPcs();
    doBatch(f1, 100);
    doBatch(f2, 100);

    f1.incrementUniqueKeyCount();
    f2.decrementUniqueKeyCount();

    assertEquals(f1.getUniqueKeyCount(), 101L);
    assertEquals(f2.getUniqueKeyCount(), 99L);
  }

  // Before/After: Feature Not Started

  @Test
  public void testFeatureNotYetStarted() {
    PartitionConsumptionState pcs = freshPcs();
    assertEquals(pcs.getUniqueKeyCount(), -1L, "-1 when feature not started");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), -1L, "-1 preserved through persistence");
  }

  // Persistence Consistency: Full syncOffset Simulation

  @Test
  public void testSyncOffsetAtomicity() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 20);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 22L);

    // syncOffset: PCS → OffsetRecord → serialize → deserialize → PCS
    OffsetRecord cp = checkpoint(pcs);
    PartitionConsumptionState restored = restoreFrom(cp);
    assertEquals(restored.getUniqueKeyCount(), 22L, "Count survives full sync cycle");
  }

  // Rapid Signals (stress test — not flaky, deterministic)

  @Test
  public void testRapidSignals() {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 1000);
    for (int i = 0; i < 500; i++) {
      pcs.incrementUniqueKeyCount();
    }
    for (int i = 0; i < 200; i++) {
      pcs.decrementUniqueKeyCount();
    }
    for (int i = 0; i < 100; i++) {
      pcs.incrementUniqueKeyCount();
    }
    assertEquals(pcs.getUniqueKeyCount(), 1400L); // 1000 + 500 - 200 + 100
  }

  // CHUNKED STORE SCENARIOS — comprehensive coverage

  @Test
  public void testChunkedHybridAAFullLifecycle() {
    // Chunked batch (5 logical keys, 4 chunks each) → RT new/update/delete → persistence
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 5, 4); // 5 manifests counted, 20 chunk fragments skipped
    assertEquals(pcs.getUniqueKeyCount(), 5L);

    // RT: leader adds 3 new chunked keys (signal=+1 per manifest on VT)
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 8L);

    // RT: update to existing chunked key (signal=0, no change)
    assertEquals(pcs.getUniqueKeyCount(), 8L);

    // RT: delete a chunked key (signal=-1)
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 7L);

    // RT: re-put the deleted chunked key (signal=+1)
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 8L);

    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 8L, "Chunked hybrid lifecycle persists correctly");
  }

  @Test
  public void testChunkedFollowerLifecycle() {
    // Follower: batch with chunks → "kcs" headers from VT (headers only on manifests)
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 10, 3);
    assertEquals(pcs.getUniqueKeyCount(), 10L);

    // Follower reads "kcs" headers from VT manifest records only
    // Chunk fragments on VT have EmptyPubSubMessageHeaders (no "kcs")
    // The schemaId != CHUNK filter in processKafkaDataMessage prevents applying headers from chunks
    pcs.incrementUniqueKeyCount(); // manifest with +1
    pcs.incrementUniqueKeyCount(); // manifest with +1
    pcs.decrementUniqueKeyCount(); // manifest with -1
    // 3 chunk fragments per new key on VT → no signal applied (filtered out)
    assertEquals(pcs.getUniqueKeyCount(), 11L); // 10 + 1 + 1 - 1
  }

  @Test
  public void testChunkedLeaderFollowerConverge() {
    // Both process same chunked batch, leader and follower should converge
    PartitionConsumptionState leader = freshPcs();
    PartitionConsumptionState follower = freshPcs();

    doBatchChunked(leader, 20, 5);
    doBatchChunked(follower, 20, 5);

    // Leader: +2 new chunked keys, -1 delete
    leader.incrementUniqueKeyCount();
    leader.incrementUniqueKeyCount();
    leader.decrementUniqueKeyCount();

    // Follower: same signals from "kcs" headers on VT manifests
    follower.incrementUniqueKeyCount();
    follower.incrementUniqueKeyCount();
    follower.decrementUniqueKeyCount();

    assertEquals(leader.getUniqueKeyCount(), 21L);
    assertEquals(follower.getUniqueKeyCount(), 21L);
    assertEquals(leader.getUniqueKeyCount(), follower.getUniqueKeyCount(), "Chunked leader and follower must converge");
  }

  @Test
  public void testChunkedCrashDuringBatch() {
    // Mid-batch crash with chunked records: checkpoint captures partial manifest count
    PartitionConsumptionState pcs = freshPcs();
    // Ingest 15 of 30 chunked keys before crash
    for (int i = 0; i < 15; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord(); // manifest only
    }
    assertEquals(pcs.getUniqueKeyCount(), 15L);

    OffsetRecord cp = checkpoint(pcs);
    assertEquals(cp.getUniqueKeyCount(), 15L);

    // Crash! Restart from checkpoint
    PartitionConsumptionState restarted = restoreFrom(cp);
    assertEquals(restarted.getUniqueKeyCount(), 15L);

    // Replay remaining 15 chunked keys (only manifests reach incrementUniqueKeyCountForBatchRecord)
    for (int i = 0; i < 15; i++) {
      restarted.incrementUniqueKeyCountForBatchRecord();
    }
    restarted.finalizeUniqueKeyCountForBatchPush();
    assertEquals(restarted.getUniqueKeyCount(), 30L, "15 checkpointed + 15 replayed = 30 logical keys");
  }

  @Test
  public void testChunkedCrashDuringRT() {
    // Chunked batch → RT with chunked new keys → crash → restore + replay
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 10, 3);

    // RT: 3 new chunked keys, 1 delete
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 12L); // 10 + 3 - 1

    OffsetRecord cp = checkpoint(pcs);

    PartitionConsumptionState restarted = restoreFrom(cp);
    assertEquals(restarted.getUniqueKeyCount(), 12L);

    // Replay: 1 more new chunked key via header
    restarted.incrementUniqueKeyCount();
    assertEquals(restarted.getUniqueKeyCount(), 13L);
  }

  @Test
  public void testChunkedIncrementalPush() {
    // Batch: 20 chunked keys → Inc push: 5 new chunked keys, 3 updates
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 20, 4);
    assertEquals(pcs.getUniqueKeyCount(), 20L);

    // Inc push writes to RT → leader signal computation for each manifest
    for (int i = 0; i < 5; i++) {
      pcs.incrementUniqueKeyCount(); // new chunked key (wasAlive=false, isAlive=true)
    }
    // 3 updates to existing chunked keys (wasAlive=true, isAlive=true → signal=0)
    assertEquals(pcs.getUniqueKeyCount(), 25L); // 20 + 5
  }

  @Test
  public void testChunkedVersionSwap() {
    // v1: 30 chunked keys + RT → v2: 50 chunked keys (independent)
    PartitionConsumptionState v1 = freshPcs();
    doBatchChunked(v1, 30, 3);
    v1.incrementUniqueKeyCount();
    assertEquals(v1.getUniqueKeyCount(), 31L);

    PartitionConsumptionState v2 = freshPcs();
    doBatchChunked(v2, 50, 5);
    assertEquals(v2.getUniqueKeyCount(), 50L);

    assertEquals(v1.getUniqueKeyCount(), 31L, "v1 unchanged");
    assertEquals(v2.getUniqueKeyCount(), 50L, "v2 independent");
  }

  @Test
  public void testChunkedBlobTransfer() {
    // Source: chunked batch + RT → blob transfer → receiver catches up
    PartitionConsumptionState src = freshPcs();
    doBatchChunked(src, 8, 4);
    src.incrementUniqueKeyCount();
    src.incrementUniqueKeyCount();
    src.decrementUniqueKeyCount();
    assertEquals(src.getUniqueKeyCount(), 9L); // 8 + 2 - 1

    OffsetRecord transferred = checkpoint(src);
    PartitionConsumptionState receiver = restoreFrom(transferred);
    assertEquals(receiver.getUniqueKeyCount(), 9L);

    // Receiver catches up via VT headers (only on manifests)
    receiver.incrementUniqueKeyCount();
    assertEquals(receiver.getUniqueKeyCount(), 10L);
  }

  @Test
  public void testChunkedNewStandbyBootstrap() {
    // New standby bootstraps chunked store from VT offset 0
    PartitionConsumptionState pcs = freshPcs();
    // Batch: 12 chunked keys (only manifests counted)
    doBatchChunked(pcs, 12, 6);
    assertEquals(pcs.getUniqueKeyCount(), 12L);

    // RT records from VT with "kcs" headers (only on manifests)
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 13L, "VT self-describing for chunked stores");
  }

  @Test
  public void testChunkedDeleteRePutCycles() {
    // Chunked store: delete-reput cycles are reversible
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 5, 3);

    // DELETE chunked key → re-PUT → DELETE → re-PUT
    pcs.decrementUniqueKeyCount(); // 4
    pcs.incrementUniqueKeyCount(); // 5
    pcs.decrementUniqueKeyCount(); // 4
    pcs.incrementUniqueKeyCount(); // 5
    assertEquals(pcs.getUniqueKeyCount(), 5L, "Chunked DELETE-REPUT cycles reversible");
  }

  @Test
  public void testChunkedDataRecovery() {
    // Data recovery with chunked store: batch counted, then RT resumes
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 25, 4);
    assertEquals(pcs.getUniqueKeyCount(), 25L);

    // After recovery completes: RT signal computation takes over
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 25L);
  }

  @Test
  public void testChunkedLeaderTransition() {
    // Follower with chunked batch + headers → promoted to leader
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 15, 3);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 16L);

    // Promoted: leader signal computation resumes from base=16
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 17L);
  }

  @Test
  public void testChunkedEmptyPartition() {
    // Empty partition in a chunked store
    PartitionConsumptionState pcs = freshPcs();
    // No records at all
    pcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(pcs.getUniqueKeyCount(), 0L, "Chunked empty partition → 0");
  }

  @Test
  public void testChunkedLargeScale() {
    // Large chunked store: 100K logical keys × 5 chunks each = 600K total records
    // Only 100K manifests counted
    PartitionConsumptionState pcs = freshPcs();
    doBatchChunked(pcs, 100_000, 5);
    assertEquals(pcs.getUniqueKeyCount(), 100_000L, "Large chunked store: logical keys only");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 100_000L, "Survives persistence");
  }

  // NON-CHUNKED explicitly labeled (for completeness alongside chunked)

  @Test
  public void testNonChunkedHybridAAFullLifecycle() {
    // Explicit non-chunked version of hybrid lifecycle for comparison
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 50); // all PUTs counted 1:1 (no chunk fragments)

    pcs.incrementUniqueKeyCount(); // +1
    pcs.incrementUniqueKeyCount(); // +1
    pcs.decrementUniqueKeyCount(); // -1
    assertEquals(pcs.getUniqueKeyCount(), 51L); // 50 + 2 - 1
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 51L);
  }

  @Test
  public void testNonChunkedCrashDuringBatchWithPersistence() {
    // Non-chunked mid-batch checkpoint + crash + restore
    PartitionConsumptionState pcs = freshPcs();
    for (int i = 0; i < 25; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(pcs.getUniqueKeyCount(), 25L);

    OffsetRecord cp = checkpoint(pcs);
    PartitionConsumptionState restarted = restoreFrom(cp);
    assertEquals(restarted.getUniqueKeyCount(), 25L);

    for (int i = 0; i < 25; i++) {
      restarted.incrementUniqueKeyCountForBatchRecord();
    }
    restarted.finalizeUniqueKeyCountForBatchPush();
    assertEquals(restarted.getUniqueKeyCount(), 50L);
  }

  @Test
  public void testNonChunkedFollowerWithHeaders() {
    // Non-chunked follower receives "kcs" headers directly on PUT VT records
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 30);
    pcs.incrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    pcs.decrementUniqueKeyCount();
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 32L); // 30 + 3 - 1
  }

  // OTel ASYNC_GAUGE metric validation through actual ingestion flow

  private static final String UNIQUE_KEY_METRIC_NAME = UNIQUE_KEY_COUNT.getMetricEntity().getMetricName();

  /** Creates a PCS with the given leader/follower state for the default topic partition. */
  private PartitionConsumptionState freshPcs(LeaderFollowerStateType lfState) {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setLeaderFollowerState(lfState);
    return pcs;
  }

  /** Creates a PCS with the given leader/follower state for a specific partition number. */
  private PartitionConsumptionState freshPcs(LeaderFollowerStateType lfState, int partition) {
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("store_v2"), partition);
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        tp,
        new OffsetRecord(
            AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
            DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
    pcs.setLeaderFollowerState(lfState);
    return pcs;
  }

  private Attributes buildAttributes(VersionRole versionRole, ReplicaType replicaType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .build();
  }

  /** Validates the unique key count gauge for both LEADER and FOLLOWER replica types. */
  private void assertGauge(InMemoryMetricReader reader, long expectedLeader, long expectedFollower) {
    validateLongPointDataFromGauge(
        reader,
        expectedLeader,
        buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
        UNIQUE_KEY_METRIC_NAME,
        TEST_PREFIX);
    validateLongPointDataFromGauge(
        reader,
        expectedFollower,
        buildAttributes(VersionRole.CURRENT, ReplicaType.FOLLOWER),
        UNIQUE_KEY_METRIC_NAME,
        TEST_PREFIX);
  }

  /** Sets up OTel infra, wires PCS list into stats, and returns the reader for gauge validation. */
  private OtelTestContext createOtelContext(PartitionConsumptionState... pcsList) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
    IngestionOtelStats stats = new IngestionOtelStats(metricsRepo, STORE_NAME, CLUSTER_NAME, LOCAL_REGION, true);
    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    doReturn(Arrays.asList(pcsList)).when(mockTask).getPartitionConsumptionStates();
    stats.setIngestionTask(CURRENT_VERSION, mockTask);

    return new OtelTestContext(reader, metricsRepo);
  }

  private static class OtelTestContext implements AutoCloseable {
    final InMemoryMetricReader reader;
    final VeniceMetricsRepository metricsRepo;

    OtelTestContext(InMemoryMetricReader reader, VeniceMetricsRepository metricsRepo) {
      this.reader = reader;
      this.metricsRepo = metricsRepo;
    }

    @Override
    public void close() {
      metricsRepo.close();
    }
  }

  @Test
  public void testOtelGaugeEmitsAfterBatchPushOnLeader() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);

    try (OtelTestContext ctx = createOtelContext(leaderPcs)) {
      assertGauge(ctx.reader, 100L, -1L);
    }
  }

  @Test
  public void testOtelGaugeEmitsAfterBatchPushOnFollower() {
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 80);

    try (OtelTestContext ctx = createOtelContext(followerPcs)) {
      assertGauge(ctx.reader, -1L, 80L);
    }
  }

  @Test
  public void testOtelGaugeLeaderAndFollowerPartitionsTogether() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER, 0);
    doBatch(leaderPcs, 100);
    PartitionConsumptionState follower1 = freshPcs(LeaderFollowerStateType.STANDBY, 1);
    doBatch(follower1, 200);
    PartitionConsumptionState follower2 = freshPcs(LeaderFollowerStateType.STANDBY, 2);
    doBatch(follower2, 300);

    try (OtelTestContext ctx = createOtelContext(leaderPcs, follower1, follower2)) {
      assertGauge(ctx.reader, 100L, 500L);
    }
  }

  @Test
  public void testOtelGaugeUpdatesAfterRTSignals() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 50);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 50);

    try (OtelTestContext ctx = createOtelContext(leaderPcs, followerPcs)) {
      assertGauge(ctx.reader, 50L, 50L);

      // RT: leader computes +3 new keys, -1 delete
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.decrementUniqueKeyCount();

      // Follower applies same signals from "kcs" headers
      followerPcs.incrementUniqueKeyCount();
      followerPcs.incrementUniqueKeyCount();
      followerPcs.incrementUniqueKeyCount();
      followerPcs.decrementUniqueKeyCount();

      assertGauge(ctx.reader, 52L, 52L);
    }
  }

  @Test
  public void testOtelGaugeAfterLeaderTransition() {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(pcs, 40);
    pcs.incrementUniqueKeyCount();

    try (OtelTestContext ctx = createOtelContext(pcs)) {
      // Before transition: count shows under FOLLOWER
      assertGauge(ctx.reader, -1L, 41L);

      // Promote to leader
      pcs.setLeaderFollowerState(LeaderFollowerStateType.LEADER);
      assertGauge(ctx.reader, 41L, -1L);

      // Leader continues with signal computation
      pcs.incrementUniqueKeyCount();
      validateLongPointDataFromGauge(
          ctx.reader,
          42L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          UNIQUE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  @Test
  public void testOtelGaugeChunkedBatchOnlyCountsManifests() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatchChunked(leaderPcs, 20, 5);

    try (OtelTestContext ctx = createOtelContext(leaderPcs)) {
      validateLongPointDataFromGauge(
          ctx.reader,
          20L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          UNIQUE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  @Test
  public void testOtelGaugeFeatureNotStartedEmitsNegativeOne() {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);
    assertEquals(pcs.getUniqueKeyCount(), -1L);

    try (OtelTestContext ctx = createOtelContext(pcs)) {
      assertGauge(ctx.reader, -1L, -1L);
    }
  }
}
