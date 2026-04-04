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
public class UniqueKeyCountScenarioTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String LOCAL_REGION = "dc-1";
  private static final String TEST_PREFIX = "test_prefix";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String UNIQUE_KEY_METRIC_NAME = UNIQUE_KEY_COUNT.getMetricEntity().getMetricName();

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
    assertEquals(pcsOn.getUniqueKeyCount(), 100L);
    assertEquals(restoreFrom(checkpoint(pcsOn)).getUniqueKeyCount(), 100L);
    // Batch counting OFF: stays at -1, persists correctly
    PartitionConsumptionState pcsOff = freshPcs();
    assertEquals(pcsOff.getUniqueKeyCount(), -1L);
    assertEquals(restoreFrom(checkpoint(pcsOff)).getUniqueKeyCount(), -1L);
  }

  @Test
  public void testHybridRTWithConfigToggle() {
    // Hybrid signal ON: batch(50) + 2 - 1 = 51
    PartitionConsumptionState pcsOn = freshPcs();
    doBatch(pcsOn, 50);
    pcsOn.incrementUniqueKeyCount();
    pcsOn.incrementUniqueKeyCount();
    pcsOn.decrementUniqueKeyCount();
    assertEquals(pcsOn.getUniqueKeyCount(), 51L);
    assertEquals(checkpoint(pcsOn).getUniqueKeyCount(), 51L);
    // Hybrid signal OFF: stays at batch baseline
    PartitionConsumptionState pcsOff = freshPcs();
    doBatch(pcsOff, 50);
    assertEquals(pcsOff.getUniqueKeyCount(), 50L);
    assertEquals(checkpoint(pcsOff).getUniqueKeyCount(), 50L);
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
    assertEquals(leaderPcs.getUniqueKeyCount(), expectedAfterBatch, desc);
    assertEquals(followerPcs.getUniqueKeyCount(), expectedAfterBatch, desc);
    if (hybridSignalEnabled && batchCountingEnabled) {
      for (PartitionConsumptionState p: new PartitionConsumptionState[] { leaderPcs, followerPcs }) {
        p.incrementUniqueKeyCount();
        p.incrementUniqueKeyCount();
        p.incrementUniqueKeyCount();
        p.decrementUniqueKeyCount();
      }
    }
    long expectedFinal = batchCountingEnabled ? (hybridSignalEnabled ? 42L : 40L) : -1L;
    assertEquals(leaderPcs.getUniqueKeyCount(), expectedFinal, desc);
    assertEquals(followerPcs.getUniqueKeyCount(), expectedFinal, desc);
    assertEquals(leaderPcs.getUniqueKeyCount(), followerPcs.getUniqueKeyCount(), desc + ": converge");
    assertEquals(checkpoint(leaderPcs).getUniqueKeyCount(), expectedFinal, desc);
    assertEquals(checkpoint(followerPcs).getUniqueKeyCount(), expectedFinal, desc);
  }

  @Test(dataProvider = "configCombinations")
  public void testChunkedLifecycleWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);
    if (batchCountingEnabled) {
      doBatchChunked(pcs, 20, 5);
    }
    long expectedAfterBatch = batchCountingEnabled ? 20L : -1L;
    assertEquals(pcs.getUniqueKeyCount(), expectedAfterBatch, desc);
    if (hybridSignalEnabled && batchCountingEnabled) {
      pcs.incrementUniqueKeyCount();
      pcs.decrementUniqueKeyCount();
    }
    assertEquals(pcs.getUniqueKeyCount(), expectedAfterBatch, desc); // net 0 from signals
    assertEquals(checkpoint(pcs).getUniqueKeyCount(), expectedAfterBatch, desc);
  }

  @Test(dataProvider = "configCombinations")
  public void testCrashRecoveryWithConfigCombinations(
      boolean batchCountingEnabled,
      boolean hybridSignalEnabled,
      String desc) {
    PartitionConsumptionState pcs = freshPcs();
    if (batchCountingEnabled) {
      for (int i = 0; i < 30; i++) {
        pcs.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
      }
    }
    PartitionConsumptionState restarted = restoreFrom(checkpoint(pcs));
    if (batchCountingEnabled) {
      assertEquals(restarted.getUniqueKeyCount(), 30L, desc);
      for (int i = 30; i < 50; i++) {
        restarted.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
      }
      restarted.finalizeUniqueKeyCountForBatchPush();
      assertEquals(restarted.getUniqueKeyCount(), 50L, desc);
    } else {
      assertEquals(restarted.getUniqueKeyCount(), -1L, desc);
    }
    if (hybridSignalEnabled && batchCountingEnabled) {
      restarted.incrementUniqueKeyCount();
      assertEquals(restarted.getUniqueKeyCount(), 51L, desc);
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
        p.incrementUniqueKeyCount();
        p.incrementUniqueKeyCount();
        p.incrementUniqueKeyCount();
        p.decrementUniqueKeyCount();
      }
      assertGauge(ctx.reader, 52L, 52L);
    }
  }

  @Test
  public void testOtelGaugeAfterLeaderTransition() {
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(pcs, 40);
    pcs.incrementUniqueKeyCount();
    try (OtelTestContext ctx = createOtelContext(pcs)) {
      assertGauge(ctx.reader, -1L, 41L);
      pcs.setLeaderFollowerState(LeaderFollowerStateType.LEADER);
      assertGauge(ctx.reader, 41L, -1L);
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
    try (OtelTestContext ctx = createOtelContext(pcs)) {
      assertGauge(ctx.reader, -1L, -1L);
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
        UNIQUE_KEY_METRIC_NAME,
        TEST_PREFIX);
    validateLongPointDataFromGauge(
        reader,
        expectedFollower,
        buildAttributes(VersionRole.CURRENT, ReplicaType.FOLLOWER),
        UNIQUE_KEY_METRIC_NAME,
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
}
