package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.checkpoint;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.doBatch;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.doBatchChunked;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.freshPcs;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.restoreFrom;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.UNIQUE_KEY_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * PCS lifecycle scenario tests: multi-step flows (batch→RT→persistence→crash→recovery)
 * and config-driven behavior with OTel gauge validation.
 *
 * Complementary to:
 * - UniqueKeyCountTest: PCS field operations, schema evolution, encoding
 * - UniqueKeyCountMockTest: production code paths via doCallRealMethod/reflection
 * - TestUniqueKeyCount (endToEnd): real cluster E2E tests
 */
public class UniqueKeyCountScenarioTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String LOCAL_REGION = "dc-1";
  private static final String TEST_PREFIX = "test_prefix";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;

  // Crash/Restart During Batch — representative crash recovery scenario

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

  // New Standby Bootstrap — consumes full VT from offset 0 (batch + RT-produced records)

  @Test
  public void testNewStandbyBootstrapFromVTOffset0() {
    // Simulates a new replica joining after batch + RT writes have already occurred.
    // The VT is self-describing: batch records → Phase 1 counter, RT records → "kcs" headers.
    PartitionConsumptionState pcs = freshPcs();

    // Phase 1: consume batch records from VT (SOP→EOP)
    doBatch(pcs, 100);
    assertEquals(pcs.getUniqueKeyCount(), 100L);

    // Phase 2: consume RT-produced VT records with "kcs" headers
    // These are the leader's produced records that carry delta signals
    pcs.incrementUniqueKeyCount(); // "kcs": +1 (new key)
    pcs.incrementUniqueKeyCount(); // "kcs": +1 (new key)
    pcs.decrementUniqueKeyCount(); // "kcs": -1 (delete)
    // 5 updates: no "kcs" header → no change
    pcs.incrementUniqueKeyCount(); // "kcs": +1 (re-put)

    assertEquals(pcs.getUniqueKeyCount(), 102L, "VT self-describing: batch(100) + 3 - 1 = 102");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 102L, "Persisted correctly");
  }

  // Data Recovery — DCR skipped during recovery, batch counter runs, then Signal1 resumes

  @Test
  public void testDataRecoveryBatchThenRTResumes() {
    // During data recovery, the server takes the L/F path (DCR skipped).
    // Phase 1 batch counter still runs (it's in processKafkaDataMessage, above DCR).
    // After recovery completes and switches to RT, Signal1 takes over.
    PartitionConsumptionState pcs = freshPcs();

    // Recovery phase: re-ingest batch (same as normal batch, DCR not involved)
    doBatch(pcs, 45);
    assertEquals(pcs.getUniqueKeyCount(), 45L, "Batch counter works during data recovery");

    // Recovery completes, switches to RT. Signal1 on leader resumes from batch baseline.
    pcs.incrementUniqueKeyCount(); // new key via Signal1
    pcs.decrementUniqueKeyCount(); // delete via Signal1
    pcs.incrementUniqueKeyCount(); // new key via Signal1

    assertEquals(pcs.getUniqueKeyCount(), 46L, "Signal1 resumes after recovery: 45 + 2 - 1 = 46");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), 46L, "Persisted correctly");
  }

  // Config-driven lifecycle tests: simulate behavior with configs enabled/disabled.
  // When a config is OFF, the corresponding PCS methods are never called (simulating
  // the production guards in trackUniqueKeyCount / processMessageAndMaybeProduceToKafka).

  @DataProvider(name = "batchCountingConfig")
  public Object[][] batchCountingConfig() {
    return new Object[][] { { true, "batch counting ON" }, { false, "batch counting OFF" } };
  }

  @Test(dataProvider = "batchCountingConfig")
  public void testBatchPushWithConfigToggle(boolean batchCountingEnabled, String desc) {
    PartitionConsumptionState pcs = freshPcs();

    if (batchCountingEnabled) {
      doBatch(pcs, 100);
      assertEquals(pcs.getUniqueKeyCount(), 100L, desc + ": batch count = 100");
    } else {
      // Config off: trackUniqueKeyCount never calls incrementUniqueKeyCountForBatchRecord.
      // PCS stays at -1 through the entire batch push.
      // finalizeUniqueKeyCountForBatchPush is also gated by the config, so never called.
      assertEquals(pcs.getUniqueKeyCount(), -1L, desc + ": no counting, stays -1");
    }

    // Persistence round-trip preserves the state regardless of config
    OffsetRecord cp = checkpoint(pcs);
    PartitionConsumptionState restored = restoreFrom(cp);
    long expected = batchCountingEnabled ? 100L : -1L;
    assertEquals(restored.getUniqueKeyCount(), expected, desc + ": persisted correctly");
  }

  @DataProvider(name = "hybridSignalConfig")
  public Object[][] hybridSignalConfig() {
    return new Object[][] { { true, "hybrid signal ON" }, { false, "hybrid signal OFF" } };
  }

  @Test(dataProvider = "hybridSignalConfig")
  public void testHybridRTWithConfigToggle(boolean hybridSignalEnabled, String desc) {
    PartitionConsumptionState pcs = freshPcs();
    doBatch(pcs, 50); // batch always counted (independent config)

    if (hybridSignalEnabled) {
      // Leader computes wasAlive/isAlive signals, follower applies from "kcs" header
      pcs.incrementUniqueKeyCount(); // new key
      pcs.incrementUniqueKeyCount(); // new key
      pcs.decrementUniqueKeyCount(); // delete
      assertEquals(pcs.getUniqueKeyCount(), 51L, desc + ": batch(50) + 2 - 1 = 51");
    } else {
      // Config off: processMessageAndMaybeProduceToKafka and trackUniqueKeyCount
      // never produce/apply signals. Count stays at batch baseline.
      assertEquals(pcs.getUniqueKeyCount(), 50L, desc + ": stays at batch baseline");
    }

    assertEquals(checkpoint(pcs).getUniqueKeyCount(), pcs.getUniqueKeyCount(), desc + ": persisted");
  }

  @DataProvider(name = "configCombinations")
  public Object[][] configCombinations() {
    return new Object[][] {
        // batchCounting, hybridSignal, description
        { true, true, "both ON" }, { true, false, "batch ON, hybrid OFF" }, { false, true, "batch OFF, hybrid ON" },
        { false, false, "both OFF" } };
  }

  @Test(dataProvider = "configCombinations")
  public void testFullLifecycleWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);

    // Phase 1: Batch push
    if (batchCountingEnabled) {
      doBatch(leaderPcs, 40);
      doBatch(followerPcs, 40);
    }

    long expectedAfterBatch = batchCountingEnabled ? 40L : -1L;
    assertEquals(leaderPcs.getUniqueKeyCount(), expectedAfterBatch, desc + ": leader after batch");
    assertEquals(followerPcs.getUniqueKeyCount(), expectedAfterBatch, desc + ": follower after batch");

    // Phase 2: RT signals (only if hybrid enabled AND batch baseline was established)
    if (hybridSignalEnabled && batchCountingEnabled) {
      // Leader signal: +3 new, -1 delete
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.incrementUniqueKeyCount();
      leaderPcs.decrementUniqueKeyCount();

      // Follower applies same signals from "kcs" headers
      followerPcs.incrementUniqueKeyCount();
      followerPcs.incrementUniqueKeyCount();
      followerPcs.incrementUniqueKeyCount();
      followerPcs.decrementUniqueKeyCount();
    }
    // Note: if hybridSignalEnabled but !batchCountingEnabled, the >= 0 guard in
    // trackUniqueKeyCount prevents signal application because uniqueKeyCount is -1.

    long expectedFinal;
    if (batchCountingEnabled && hybridSignalEnabled) {
      expectedFinal = 42L; // 40 + 3 - 1
    } else if (batchCountingEnabled) {
      expectedFinal = 40L; // batch only, no RT adjustment
    } else {
      expectedFinal = -1L; // never started
    }
    assertEquals(leaderPcs.getUniqueKeyCount(), expectedFinal, desc + ": leader final");
    assertEquals(followerPcs.getUniqueKeyCount(), expectedFinal, desc + ": follower final");

    // Verify leader-follower convergence
    assertEquals(
        leaderPcs.getUniqueKeyCount(),
        followerPcs.getUniqueKeyCount(),
        desc + ": leader and follower converge");

    // Persistence round-trip
    assertEquals(checkpoint(leaderPcs).getUniqueKeyCount(), expectedFinal, desc + ": leader persisted");
    assertEquals(checkpoint(followerPcs).getUniqueKeyCount(), expectedFinal, desc + ": follower persisted");
  }

  @Test(dataProvider = "configCombinations")
  public void testChunkedLifecycleWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);

    if (batchCountingEnabled) {
      doBatchChunked(pcs, 20, 5); // 20 manifests counted, 100 fragments skipped
    }

    long expectedAfterBatch = batchCountingEnabled ? 20L : -1L;
    assertEquals(pcs.getUniqueKeyCount(), expectedAfterBatch, desc + ": after chunked batch");

    if (hybridSignalEnabled && batchCountingEnabled) {
      pcs.incrementUniqueKeyCount(); // new chunked key
      pcs.decrementUniqueKeyCount(); // delete chunked key
    }

    long expectedFinal = batchCountingEnabled ? 20L : -1L; // net 0 from signals
    assertEquals(pcs.getUniqueKeyCount(), expectedFinal, desc + ": after chunked RT");
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), expectedFinal, desc + ": persisted");
  }

  @Test(dataProvider = "configCombinations")
  public void testCrashRecoveryWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs();

    if (batchCountingEnabled) {
      // Ingest 30 records, checkpoint, crash, restore, ingest 20 more
      for (int i = 0; i < 30; i++) {
        pcs.incrementUniqueKeyCountForBatchRecord();
      }
    }

    OffsetRecord midBatchCp = checkpoint(pcs);
    PartitionConsumptionState restarted = restoreFrom(midBatchCp);

    if (batchCountingEnabled) {
      assertEquals(restarted.getUniqueKeyCount(), 30L, desc + ": restored mid-batch count");
      for (int i = 0; i < 20; i++) {
        restarted.incrementUniqueKeyCountForBatchRecord();
      }
      restarted.finalizeUniqueKeyCountForBatchPush();
      assertEquals(restarted.getUniqueKeyCount(), 50L, desc + ": 30 + 20 after restart");
    } else {
      assertEquals(restarted.getUniqueKeyCount(), -1L, desc + ": stays -1 after restart");
    }

    if (hybridSignalEnabled && batchCountingEnabled) {
      restarted.incrementUniqueKeyCount();
      assertEquals(restarted.getUniqueKeyCount(), 51L, desc + ": RT signal after recovery");
    }
  }

  @Test(dataProvider = "configCombinations")
  public void testOtelGaugeWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);

    if (batchCountingEnabled) {
      doBatch(leaderPcs, 30);
      doBatch(followerPcs, 30);
    }

    if (hybridSignalEnabled && batchCountingEnabled) {
      leaderPcs.incrementUniqueKeyCount();
      followerPcs.incrementUniqueKeyCount();
    }

    long expectedLeader;
    long expectedFollower;
    if (batchCountingEnabled && hybridSignalEnabled) {
      expectedLeader = 31L;
      expectedFollower = 31L;
    } else if (batchCountingEnabled) {
      expectedLeader = 30L;
      expectedFollower = 30L;
    } else {
      // No batch baseline → gauge returns -1 for both
      expectedLeader = -1L;
      expectedFollower = -1L;
    }

    try (OtelTestContext ctx = createOtelContext(leaderPcs, followerPcs)) {
      assertGauge(ctx.reader, expectedLeader, expectedFollower);
    }
  }

  // OTel ASYNC_GAUGE metric validation through actual ingestion flow

  private static final String UNIQUE_KEY_METRIC_NAME = UNIQUE_KEY_COUNT.getMetricEntity().getMetricName();

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
