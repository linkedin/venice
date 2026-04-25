package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.checkpoint;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.doBatch;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.doBatchChunked;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.freshPcs;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.restoreFrom;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.ACTIVE_KEY_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
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
 * PCS lifecycle scenario tests: multi-step flows (batch, RT, persistence, crash recovery)
 * and config-driven behavior with OTel gauge validation.
 */
public class ActiveKeyCountScenarioTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String LOCAL_REGION = "dc-1";
  private static final String TEST_PREFIX = "test_prefix";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String ACTIVE_KEY_METRIC_NAME = ACTIVE_KEY_COUNT.getMetricEntity().getMetricName();

  @DataProvider(name = "configCombinations")
  public Object[][] configCombinations() {
    return new Object[][] { { true, true, "both ON" }, { true, false, "batch ON, hybrid OFF" },
        { false, true, "batch OFF, hybrid ON" }, { false, false, "both OFF" } };
  }

  @Test
  public void testBatchPushWithConfigToggle() {
    // Batch counting ON: count = 100, persists correctly
    PartitionConsumptionState pcsOn = freshPcs();
    doBatch(pcsOn, 100);
    assertEquals(pcsOn.getActiveKeyCount(), 100L);
    assertEquals(restoreFrom(checkpoint(pcsOn)).getActiveKeyCount(), 100L);
    // Batch counting OFF: stays at -1, persists correctly
    PartitionConsumptionState pcsOff = freshPcs();
    assertEquals(pcsOff.getActiveKeyCount(), -1L);
    assertEquals(restoreFrom(checkpoint(pcsOff)).getActiveKeyCount(), -1L);
  }

  @Test
  public void testHybridRTWithConfigToggle() {
    // Hybrid signal ON: batch(50) + 2 - 1 = 51
    PartitionConsumptionState pcsOn = freshPcs();
    doBatch(pcsOn, 50);
    pcsOn.incrementActiveKeyCount();
    pcsOn.incrementActiveKeyCount();
    pcsOn.decrementActiveKeyCount();
    assertEquals(pcsOn.getActiveKeyCount(), 51L);
    assertEquals(checkpoint(pcsOn).getActiveKeyCount(), 51L);
    // Hybrid signal OFF: stays at batch baseline
    PartitionConsumptionState pcsOff = freshPcs();
    doBatch(pcsOff, 50);
    assertEquals(pcsOff.getActiveKeyCount(), 50L);
    assertEquals(checkpoint(pcsOff).getActiveKeyCount(), 50L);
  }

  @Test(dataProvider = "configCombinations")
  public void testFullLifecycleWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    if (batchCountingEnabled) {
      doBatch(leaderPcs, 40);
      doBatch(followerPcs, 40);
    }
    long expectedAfterBatch = batchCountingEnabled ? 40L : -1L;
    assertEquals(leaderPcs.getActiveKeyCount(), expectedAfterBatch, desc);
    assertEquals(followerPcs.getActiveKeyCount(), expectedAfterBatch, desc);
    if (hybridSignalEnabled && batchCountingEnabled) {
      for (PartitionConsumptionState p: new PartitionConsumptionState[] { leaderPcs, followerPcs }) {
        p.incrementActiveKeyCount();
        p.incrementActiveKeyCount();
        p.incrementActiveKeyCount();
        p.decrementActiveKeyCount();
      }
    }
    long expectedFinal = batchCountingEnabled ? (hybridSignalEnabled ? 42L : 40L) : -1L;
    assertEquals(leaderPcs.getActiveKeyCount(), expectedFinal, desc);
    assertEquals(followerPcs.getActiveKeyCount(), expectedFinal, desc);
    assertEquals(leaderPcs.getActiveKeyCount(), followerPcs.getActiveKeyCount(), desc + ": converge");
    assertEquals(checkpoint(leaderPcs).getActiveKeyCount(), expectedFinal, desc);
    assertEquals(checkpoint(followerPcs).getActiveKeyCount(), expectedFinal, desc);
  }

  @Test(dataProvider = "configCombinations")
  public void testChunkedLifecycleWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);
    if (batchCountingEnabled) {
      doBatchChunked(pcs, 20);
    }
    long expectedAfterBatch = batchCountingEnabled ? 20L : -1L;
    assertEquals(pcs.getActiveKeyCount(), expectedAfterBatch, desc);
    if (hybridSignalEnabled && batchCountingEnabled) {
      pcs.incrementActiveKeyCount();
      pcs.decrementActiveKeyCount();
    }
    assertEquals(pcs.getActiveKeyCount(), expectedAfterBatch, desc); // net 0 from signals
    assertEquals(checkpoint(pcs).getActiveKeyCount(), expectedAfterBatch, desc);
  }

  @Test(dataProvider = "configCombinations")
  public void testCrashRecoveryWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs();
    if (batchCountingEnabled) {
      pcs.initializeActiveKeyCount(); // SOP
      for (int i = 0; i < 30; i++) {
        pcs.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
      }
    }
    PartitionConsumptionState restarted = restoreFrom(checkpoint(pcs));
    if (batchCountingEnabled) {
      assertEquals(restarted.getActiveKeyCount(), 30L, desc);
      for (int i = 30; i < 50; i++) {
        restarted.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
      }
      restarted.cleanupBatchKeyCountState();
      assertEquals(restarted.getActiveKeyCount(), 50L, desc);
    } else {
      assertEquals(restarted.getActiveKeyCount(), -1L, desc);
    }
    if (hybridSignalEnabled && batchCountingEnabled) {
      restarted.incrementActiveKeyCount();
      assertEquals(restarted.getActiveKeyCount(), 51L, desc);
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
      leaderPcs.incrementActiveKeyCount();
      followerPcs.incrementActiveKeyCount();
    }
    long expected = batchCountingEnabled ? (hybridSignalEnabled ? 31L : 30L) : -1L;
    try (OtelTestContext ctx = createOtelContext(leaderPcs, followerPcs)) {
      assertGauge(ctx.reader, expected, expected);
    }
  }

  // OTel gauge validation

  @Test
  public void testOtelGaugeEmitsAfterBatchPush() {
    // Leader-only
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);
    try (OtelTestContext ctx = createOtelContext(leaderPcs)) {
      assertGauge(ctx.reader, 100L, -1L);
    }
    // Follower-only
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
      for (PartitionConsumptionState p: new PartitionConsumptionState[] { leaderPcs, followerPcs }) {
        p.incrementActiveKeyCount();
        p.incrementActiveKeyCount();
        p.incrementActiveKeyCount();
        p.decrementActiveKeyCount();
      }
      assertGauge(ctx.reader, 52L, 52L);
    }
  }

  @Test
  public void testOtelGaugeAfterLeaderTransition() {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(pcs, 40);
    pcs.incrementActiveKeyCount();
    try (OtelTestContext ctx = createOtelContext(pcs)) {
      assertGauge(ctx.reader, -1L, 41L);
      pcs.setLeaderFollowerState(LeaderFollowerStateType.LEADER);
      assertGauge(ctx.reader, 41L, -1L);
      pcs.incrementActiveKeyCount();
      validateLongPointDataFromGauge(
          ctx.reader,
          42L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  @Test
  public void testOtelGaugeChunkedBatchOnlyCountsManifests() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatchChunked(leaderPcs, 20);
    try (OtelTestContext ctx = createOtelContext(leaderPcs)) {
      validateLongPointDataFromGauge(
          ctx.reader,
          20L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  @Test
  public void testOtelGaugeFeatureNotStartedEmitsNegativeOne() {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);
    try (OtelTestContext ctx = createOtelContext(pcs)) {
      assertGauge(ctx.reader, -1L, -1L);
    }
  }

  @Test
  public void testOtelGaugeEmitsNegativeOneAfterUnderflowInvalidation() {
    // Simulate: batch push with 5 keys, then RT delete underflows the count
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 5);
    assertEquals(leaderPcs.getActiveKeyCount(), 5L);

    try (OtelTestContext ctx = createOtelContext(leaderPcs)) {
      // Gauge shows 5 before invalidation
      validateLongPointDataFromGauge(
          ctx.reader,
          5L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);

      // Simulate 5 RT deletes bringing count to 0, then one more causes underflow → -1
      for (int i = 0; i < 5; i++) {
        leaderPcs.decrementActiveKeyCount();
      }
      assertEquals(leaderPcs.getActiveKeyCount(), 0L);

      // The 6th decrement triggers underflow invalidation
      boolean success = leaderPcs.decrementActiveKeyCount();
      assertEquals(success, false);
      assertEquals(leaderPcs.getActiveKeyCount(), -1L);

      // Gauge now reports -1 (invalidated/not tracked)
      validateLongPointDataFromGauge(
          ctx.reader,
          -1L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  @Test
  public void testOtelGaugeEmitsNegativeOneAfterFollowerInvalidateSignal() {
    // Simulate: follower has batch baseline of 10, receives invalidate signal (kcs=0)
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 10);
    assertEquals(followerPcs.getActiveKeyCount(), 10L);

    try (OtelTestContext ctx = createOtelContext(followerPcs)) {
      // Gauge shows 10 for follower
      validateLongPointDataFromGauge(
          ctx.reader,
          10L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.FOLLOWER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);

      // Simulate receiving KEY_COUNT_INVALIDATE_SIGNAL from leader
      followerPcs.setActiveKeyCount(-1);
      assertEquals(followerPcs.getActiveKeyCount(), -1L);

      // Gauge now reports -1 (invalidated)
      validateLongPointDataFromGauge(
          ctx.reader,
          -1L,
          buildAttributes(VersionRole.CURRENT, ReplicaType.FOLLOWER),
          ACTIVE_KEY_METRIC_NAME,
          TEST_PREFIX);
    }
  }

  // OTel helpers

  private Attributes buildAttributes(VersionRole versionRole, ReplicaType replicaType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .build();
  }

  private void assertGauge(InMemoryMetricReader reader, long expectedLeader, long expectedFollower) {
    validateLongPointDataFromGauge(
        reader,
        expectedLeader,
        buildAttributes(VersionRole.CURRENT, ReplicaType.LEADER),
        ACTIVE_KEY_METRIC_NAME,
        TEST_PREFIX);
    validateLongPointDataFromGauge(
        reader,
        expectedFollower,
        buildAttributes(VersionRole.CURRENT, ReplicaType.FOLLOWER),
        ACTIVE_KEY_METRIC_NAME,
        TEST_PREFIX);
  }

  private OtelTestContext createOtelContext(PartitionConsumptionState... pcsList) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
    IngestionOtelStats stats = new IngestionOtelStats(metricsRepo, STORE_NAME, CLUSTER_NAME, LOCAL_REGION, true, false);
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
}
