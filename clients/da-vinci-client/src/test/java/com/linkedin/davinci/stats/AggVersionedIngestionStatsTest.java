package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.davinci.stats.ingestion.NoOpIngestionOtelStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceDCROperation;
import com.linkedin.venice.stats.dimensions.VeniceIngestionFailureReason;
import com.linkedin.venice.stats.dimensions.VenicePartialUpdateOperation;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AggVersionedIngestionStats}.
 * Tests the version cleanup hooks and store deletion handling for OTel stats.
 */
public class AggVersionedIngestionStatsTest {
  private static final String STORE_NAME = "testStore";
  private static final String CLUSTER_NAME = "testCluster";
  private static final String TEST_PREFIX = "test_prefix";
  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;
  private static final int VERSION_3 = 3;

  private ReadOnlyStoreRepository storeRepository;

  @BeforeMethod
  public void setUp() {
    storeRepository = mock(ReadOnlyStoreRepository.class);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(STORE_NAME);
    when(mockStore.getVersions()).thenReturn(Collections.emptyList());
    when(mockStore.getCurrentVersion()).thenReturn(0);
    doReturn(mockStore).when(storeRepository).getStoreOrThrow(anyString());
  }

  private AggVersionedIngestionStats createAggStats(boolean ingestionOtelStatsEnabled) {
    return createAggStats(ingestionOtelStatsEnabled, new MetricsRepository());
  }

  private AggVersionedIngestionStats createAggStats(
      boolean ingestionOtelStatsEnabled,
      MetricsRepository metricsRepository) {
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(config.getClusterName()).thenReturn(CLUSTER_NAME);
    when(config.isUnregisterMetricForDeletedStoreEnabled()).thenReturn(true);
    when(config.isIngestionOtelStatsEnabled()).thenReturn(ingestionOtelStatsEnabled);
    doReturn(Int2ObjectMaps.emptyMap()).when(config).getKafkaClusterIdToAliasMap();
    return new AggVersionedIngestionStats(metricsRepository, storeRepository, config);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSetIngestionTask(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set ingestion task for a version
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    if (ingestionOtelStatsEnabled) {
      assertTrue(otelStatsMap.containsKey(STORE_NAME), "OTel stats should be created for the store when enabled");
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCleanupVersionResources(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set up ingestion tasks for multiple versions
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_2, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);

    if (ingestionOtelStatsEnabled) {
      IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);
      assertNotNull(otelStats, "OTel stats should exist");

      // Set some state for version 1
      otelStats.setIngestionTaskPushTimeoutGauge(VERSION_1, 1);
      otelStats.recordIdleTime(VERSION_1, 5000);

      // Verify state exists before cleanup
      Map<Integer, StoreIngestionTask> tasksByVersion = getIngestionTasksByVersion(otelStats);
      Map<Integer, Integer> pushTimeoutByVersion = getPushTimeoutByVersion(otelStats);
      Map<Integer, ?> idleTimeByVersion = getIdleTimeByVersion(otelStats);

      assertTrue(tasksByVersion.containsKey(VERSION_1), "Task should exist for VERSION_1 before cleanup");
      assertTrue(tasksByVersion.containsKey(VERSION_2), "Task should exist for VERSION_2 before cleanup");
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_1), "Push timeout should exist for VERSION_1 before cleanup");
      assertTrue(idleTimeByVersion.containsKey(VERSION_1), "Idle time should exist for VERSION_1 before cleanup");

      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);

      // Verify VERSION_1 data was cleaned up
      assertFalse(tasksByVersion.containsKey(VERSION_1), "Task should be removed for VERSION_1 after cleanup");
      assertFalse(
          pushTimeoutByVersion.containsKey(VERSION_1),
          "Push timeout should be removed for VERSION_1 after cleanup");
      assertFalse(idleTimeByVersion.containsKey(VERSION_1), "Idle time should be removed for VERSION_1 after cleanup");

      // Verify VERSION_2 data still exists
      assertTrue(tasksByVersion.containsKey(VERSION_2), "Task should still exist for VERSION_2 after cleanup");

      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_2);
      assertTrue(otelStatsMap.containsKey(STORE_NAME), "Metadata cleanup should not retire the store's OTel stats");
      assertFalse(tasksByVersion.containsKey(VERSION_2), "Task should be removed for VERSION_2 after cleanup");
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
      // Cleanup should be a no-op when disabled — must not throw
      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should remain empty after cleanup when disabled");
    }
  }

  @Test
  public void testRemoveIngestionTaskRemovesOtelStatsWhenLastTask() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, task);
    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    assertTrue(otelStatsMap.containsKey(STORE_NAME));

    aggStats.removeIngestionTask(STORE_NAME + "_v" + VERSION_1, task);
    assertFalse(otelStatsMap.containsKey(STORE_NAME), "Last detached task should remove the store's OTel stats");
  }

  @Test
  public void testRemoveIngestionTaskDoesNotRemoveNewerTaskForSameVersion() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    StoreIngestionTask staleTask = mock(StoreIngestionTask.class);
    StoreIngestionTask newerTask = mock(StoreIngestionTask.class);
    when(staleTask.isHybridMode()).thenReturn(false);
    when(newerTask.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, staleTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, newerTask);
    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);

    aggStats.removeIngestionTask(STORE_NAME + "_v" + VERSION_1, staleTask);

    assertTrue(otelStatsMap.containsKey(STORE_NAME), "Stale task detach should not remove the store's OTel stats");
    assertEquals(getIngestionTasksByVersion(otelStats).get(VERSION_1), newerTask);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHandleStoreDeleted(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    if (ingestionOtelStatsEnabled) {
      assertTrue(otelStatsMap.containsKey(STORE_NAME), "OTel stats should exist before deletion when enabled");
    }

    // Delete the store — should not throw regardless of config
    aggStats.handleStoreDeleted(STORE_NAME);

    assertFalse(otelStatsMap.containsKey(STORE_NAME), "OTel stats should be removed after store deletion");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHandleStoreDeletedWithNoOtelStats(boolean ingestionOtelStatsEnabled) {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    // Delete a store that doesn't have OTel stats (should not throw)
    aggStats.handleStoreDeleted("nonExistentStore");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVersionInfoUpdateTriggersCleanup(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_2, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_3, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);

    if (ingestionOtelStatsEnabled) {
      IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);

      otelStats.setIngestionTaskPushTimeoutGauge(VERSION_1, 1);
      otelStats.setIngestionTaskPushTimeoutGauge(VERSION_2, 1);
      otelStats.setIngestionTaskPushTimeoutGauge(VERSION_3, 1);

      Map<Integer, Integer> pushTimeoutByVersion = getPushTimeoutByVersion(otelStats);
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_1), "Push timeout should exist for VERSION_1");
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_2), "Push timeout should exist for VERSION_2");
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_3), "Push timeout should exist for VERSION_3");

      otelStats.updateVersionInfo(VERSION_2, VERSION_3);
      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);

      assertFalse(pushTimeoutByVersion.containsKey(VERSION_1), "Push timeout should be removed for VERSION_1");
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_2), "Push timeout should still exist for VERSION_2");
      assertTrue(pushTimeoutByVersion.containsKey(VERSION_3), "Push timeout should still exist for VERSION_3");
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
      // Cleanup and version update should be no-ops — must not throw
      invokeOnVersionInfoUpdated(aggStats, STORE_NAME, VERSION_2, VERSION_3);
      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should remain empty after lifecycle hooks when disabled");
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOnVersionInfoUpdated(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);

    // Call onVersionInfoUpdated — should not throw regardless of config
    invokeOnVersionInfoUpdated(aggStats, STORE_NAME, VERSION_2, VERSION_3);

    if (ingestionOtelStatsEnabled) {
      IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);
      Object versionInfo = getVersionInfo(otelStats);
      assertNotNull(versionInfo, "Version info should not be null");
      assertEquals(getCurrentVersion(versionInfo), VERSION_2);
      assertEquals(getFutureVersion(versionInfo), VERSION_3);
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAllRecordingMethodsWorkWithConfig(boolean ingestionOtelStatsEnabled) throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(ingestionOtelStatsEnabled);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    // Exercise all recording hot-path methods — none should throw
    long now = System.currentTimeMillis();
    aggStats.recordLeaderConsumed(STORE_NAME, VERSION_1, 100);
    aggStats.recordFollowerConsumed(STORE_NAME, VERSION_1, 200);
    aggStats.recordLeaderProduced(STORE_NAME, VERSION_1, 300, 1);
    aggStats.recordUpdateIgnoredDCR(STORE_NAME, VERSION_1);
    aggStats.recordTotalDCR(STORE_NAME, VERSION_1);
    aggStats.recordTotalDuplicateKeyUpdate(STORE_NAME, VERSION_1);
    aggStats.recordTimestampRegressionDCRError(STORE_NAME, VERSION_1);
    aggStats.recordOffsetRegressionDCRError(STORE_NAME, VERSION_1);
    aggStats.recordTombStoneCreationDCR(STORE_NAME, VERSION_1);
    aggStats.setIngestionTaskPushTimeoutGauge(STORE_NAME, VERSION_1);
    aggStats.resetIngestionTaskPushTimeoutGauge(STORE_NAME, VERSION_1);
    aggStats.recordSubscribePrepLatency(STORE_NAME, VERSION_1, 5.0);
    aggStats.recordProducerCallBackLatency(STORE_NAME, VERSION_1, 3.0, now);
    aggStats.recordLeaderPreprocessingLatency(STORE_NAME, VERSION_1, 2.0, now);
    aggStats.recordInternalPreprocessingLatency(STORE_NAME, VERSION_1, 1.0, now);
    aggStats.recordLeaderLatencies(STORE_NAME, VERSION_1, now, 10.0, 20.0);
    aggStats.recordFollowerLatencies(STORE_NAME, VERSION_1, now, 15.0, 25.0);
    aggStats.recordLeaderProducerCompletionTime(STORE_NAME, VERSION_1, 4.0, now);
    aggStats.recordConsumedRecordEndToEndProcessingLatency(STORE_NAME, VERSION_1, 6.0, now);
    aggStats.recordMaxIdleTime(STORE_NAME, VERSION_1, 1000, true);
    aggStats.recordBatchProcessingRequest(STORE_NAME, VERSION_1, 10, now);
    aggStats.recordBatchProcessingRequestError(STORE_NAME, VERSION_1);
    aggStats.recordBatchProcessingLatency(STORE_NAME, VERSION_1, 7.0, now);
    aggStats.recordRegionHybridConsumption(STORE_NAME, VERSION_1, 0, 512, now, "dc-1", VeniceRegionLocality.LOCAL);

    // New HostLevelIngestionStats OTel methods
    aggStats.recordConsumerQueuePutTime(STORE_NAME, VERSION_1, 5.0);
    aggStats.recordStorageEnginePutTime(STORE_NAME, VERSION_1, 3.0);
    aggStats.recordStorageEngineDeleteTime(STORE_NAME, VERSION_1, 2.0);
    aggStats.recordConsumerActionTime(STORE_NAME, VERSION_1, 4.0);
    aggStats.recordLongRunningTaskCheckTime(STORE_NAME, VERSION_1, 1.0);
    aggStats.recordViewWriterProduceTime(STORE_NAME, VERSION_1, 6.0);
    aggStats.recordViewWriterAckTime(STORE_NAME, VERSION_1, 7.0);
    aggStats.recordProducerEnqueueTime(STORE_NAME, VERSION_1, 8.0);
    aggStats.recordProducerCompressTime(STORE_NAME, VERSION_1, 2.0);
    aggStats.recordProducerSynchronizeTime(STORE_NAME, VERSION_1, 3.0);
    aggStats.recordPartialUpdateTime(STORE_NAME, VERSION_1, VenicePartialUpdateOperation.QUERY, 5.0);
    aggStats.recordDcrLookupTime(STORE_NAME, VERSION_1, VeniceRecordType.DATA, 4.0);
    aggStats.recordDcrMergeTime(STORE_NAME, VERSION_1, VeniceDCROperation.PUT, 3.0);
    aggStats.recordUnexpectedMessageCount(STORE_NAME, VERSION_1);
    aggStats.recordStoreMetadataInconsistentCount(STORE_NAME, VERSION_1);
    aggStats.recordResubscriptionFailureCount(STORE_NAME, VERSION_1);
    aggStats.recordPartialUpdateCacheHitCount(STORE_NAME, VERSION_1);
    aggStats.recordChecksumVerificationFailureCount(STORE_NAME, VERSION_1);
    aggStats.recordBatchPushRecordCountMatch(STORE_NAME, VERSION_1);
    aggStats.recordBatchPushRecordCountMismatch(STORE_NAME, VERSION_1);
    aggStats.recordRecordCountMismatchFailure(STORE_NAME, VERSION_1);
    aggStats.recordIngestionFailureCount(STORE_NAME, VERSION_1, VeniceIngestionFailureReason.GENERAL);
    aggStats.recordDcrLookupCacheHitCount(STORE_NAME, VERSION_1, VeniceRecordType.REPLICATION_METADATA);
    aggStats.recordBytesConsumedAsUncompressedSize(STORE_NAME, VERSION_1, 1024);
    aggStats.recordKeySize(STORE_NAME, VERSION_1, 64);
    aggStats.recordValueSize(STORE_NAME, VERSION_1, 256);
    aggStats.recordAssembledSize(STORE_NAME, VERSION_1, VeniceRecordType.DATA, 512);
    aggStats.recordAssembledSizeRatio(STORE_NAME, VERSION_1, 0.5);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    if (ingestionOtelStatsEnabled) {
      assertTrue(otelStatsMap.containsKey(STORE_NAME), "OTel stats should exist when enabled");
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
    }
  }

  @Test
  public void testPushTimeoutSurvivesDetachUntilVersionCleanup() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repo = createOtelEnabledRepo(reader)) {
      AggVersionedIngestionStats aggStats = createAggStats(true, repo);
      setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1, VERSION_2);
      StoreIngestionTask task = mockTask();

      aggStats.setIngestionTask(versionTopic(VERSION_2), task);
      aggStats.setIngestionTaskPushTimeoutGauge(STORE_NAME, VERSION_2);
      aggStats.removeIngestionTask(versionTopic(VERSION_2), task);

      assertGaugeValue(
          reader,
          INGESTION_TASK_PUSH_TIMEOUT_COUNT.getMetricEntity().getMetricName(),
          STORE_NAME,
          VersionRole.FUTURE,
          1L);

      aggStats.cleanupVersionResources(STORE_NAME, VERSION_2);
      assertNoGaugePoint(
          reader,
          INGESTION_TASK_PUSH_TIMEOUT_COUNT.getMetricEntity().getMetricName(),
          STORE_NAME,
          VersionRole.FUTURE);
    }
  }

  @Test
  public void testCleanupWithRegisteredTaskDefersOtelStatsCloseUntilDetach() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1);
    StoreIngestionTask task = mockTask();

    aggStats.setIngestionTask(versionTopic(VERSION_1), task);
    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    aggStats.cleanupVersionResources(STORE_NAME, VERSION_1);

    assertTrue(
        otelStatsMap.containsKey(STORE_NAME),
        "Metadata cleanup must not close OTel stats that have a running task");
    aggStats.removeIngestionTask(versionTopic(VERSION_1), task);
    assertFalse(otelStatsMap.containsKey(STORE_NAME), "Task detach should close the deferred idle OTel stats");
  }

  @Test
  public void testDetachingOneVersionKeepsOtherVersionGaugesUntilLastDetach() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repo = createOtelEnabledRepo(reader)) {
      AggVersionedIngestionStats aggStats = createAggStats(true, repo);
      setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1, VERSION_2);
      StoreIngestionTask currentTask = mockTask();
      StoreIngestionTask futureTask = mockTask();

      aggStats.setIngestionTask(versionTopic(VERSION_1), currentTask);
      aggStats.setIngestionTask(versionTopic(VERSION_2), futureTask);
      aggStats.removeIngestionTask(versionTopic(VERSION_1), currentTask);

      assertGaugeValue(
          reader,
          INGESTION_TASK_COUNT.getMetricEntity().getMetricName(),
          STORE_NAME,
          VersionRole.FUTURE,
          1L);

      aggStats.removeIngestionTask(versionTopic(VERSION_2), futureTask);
      assertNoGaugePoint(
          reader,
          INGESTION_TASK_COUNT.getMetricEntity().getMetricName(),
          STORE_NAME,
          VersionRole.FUTURE);
    }
  }

  @Test
  public void testBaseOtelStatsStayOpenUntilTheLastViewTaskStops() throws Exception {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repo = createOtelEnabledRepo(reader)) {
      AggVersionedIngestionStats aggStats = createAggStats(true, repo);
      String firstViewTopic = viewTopic(VERSION_1, "firstView");
      String secondViewTopic = viewTopic(VERSION_1, "secondView");
      setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1);
      setStoreVersionInfo(aggStats, VeniceView.parseStoreAndViewFromViewTopic(firstViewTopic), VERSION_1, VERSION_1);
      setStoreVersionInfo(aggStats, VeniceView.parseStoreAndViewFromViewTopic(secondViewTopic), VERSION_1, VERSION_1);
      StoreIngestionTask firstViewTask = mockTask();
      StoreIngestionTask secondViewTask = mockTask();
      Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);

      // Two views ingest on this host without a task of the base store, and both record into its stats.
      aggStats.setIngestionTask(firstViewTopic, firstViewTask);
      aggStats.setIngestionTask(secondViewTopic, secondViewTask);
      IngestionOtelStats baseStats = otelStatsMap.get(STORE_NAME);
      assertNotNull(baseStats);

      aggStats.removeIngestionTask(firstViewTopic, firstViewTask);
      assertSame(otelStatsMap.get(STORE_NAME), baseStats);

      aggStats.removeIngestionTask(secondViewTopic, secondViewTask);
      assertNull(otelStatsMap.get(STORE_NAME));
    }
  }

  @Test
  public void testViewTopicDetachRetiresViewAndIdleBaseOtelStats() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repo = createOtelEnabledRepo(reader)) {
      AggVersionedIngestionStats aggStats = createAggStats(true, repo);
      String viewTopic = viewTopic(VERSION_1);
      String viewStoreName = VeniceView.parseStoreAndViewFromViewTopic(viewTopic);
      setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1);
      setStoreVersionInfo(aggStats, viewStoreName, VERSION_1, VERSION_1);
      StoreIngestionTask viewTask = mockTask();

      String taskCount = INGESTION_TASK_COUNT.getMetricEntity().getMetricName();
      String recordsConsumed = INGESTION_RECORDS_CONSUMED.getMetricEntity().getMetricName();
      Attributes baseLeaderCurrent = attributes(STORE_NAME, VersionRole.CURRENT, ReplicaType.LEADER);

      aggStats.setIngestionTask(viewTopic, viewTask);
      // The view task records under the base store name, as StoreIngestionTask does.
      aggStats.recordLeaderConsumed(STORE_NAME, VERSION_1, 7);
      assertGaugeValue(reader, taskCount, viewStoreName, VersionRole.CURRENT, 1L);
      aggStats.removeIngestionTask(viewTopic, viewTask);

      // Gauges stop at once; the closed base-store counter reports its final total once, then stops.
      Collection<MetricData> firstCollection = reader.collectAllMetrics();
      assertNull(
          OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
              firstCollection,
              taskCount,
              TEST_PREFIX,
              attributes(viewStoreName, VersionRole.CURRENT)));
      assertEquals(
          OpenTelemetryDataTestUtils
              .getLongPointDataFromSum(firstCollection, recordsConsumed, TEST_PREFIX, baseLeaderCurrent)
              .getValue(),
          1L);
      OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
          reader.collectAllMetrics(),
          recordsConsumed,
          TEST_PREFIX,
          baseLeaderCurrent);
    }
  }

  @Test
  public void testViewTopicDetachKeepsBaseOtelStatsWithRegisteredBaseTask() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    String viewTopic = viewTopic(VERSION_1);
    String viewStoreName = VeniceView.parseStoreAndViewFromViewTopic(viewTopic);
    setStoreVersionInfo(aggStats, STORE_NAME, VERSION_1, VERSION_1);
    setStoreVersionInfo(aggStats, viewStoreName, VERSION_1, VERSION_1);
    StoreIngestionTask baseTask = mockTask();
    StoreIngestionTask viewTask = mockTask();

    aggStats.setIngestionTask(versionTopic(VERSION_1), baseTask);
    aggStats.setIngestionTask(viewTopic, viewTask);
    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    aggStats.removeIngestionTask(viewTopic, viewTask);

    assertFalse(
        otelStatsMap.containsKey(viewStoreName),
        "View store's OTel stats should close after its task detaches");
    assertTrue(
        otelStatsMap.containsKey(STORE_NAME),
        "Base store's OTel stats should stay open for its registered base task");
  }

  @Test
  public void testRemoveIngestionTaskNeverThrows() {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    StoreIngestionTask task = mockTask();
    try {
      aggStats.removeIngestionTask("not_a_version_topic", task);
      aggStats.setIngestionTask(versionTopic(VERSION_1), task);
      when(task.isHybridMode()).thenThrow(new RuntimeException("unexpected task method call"));
      aggStats.removeIngestionTask(versionTopic(VERSION_1), task);
    } catch (Exception e) {
      fail("removeIngestionTask should catch and log bad topics or task failures", e);
    }
  }

  @Test
  public void testGetIngestionOtelStatsReturnsRealStatsWhenEnabled() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(true);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    IngestionOtelStats result = invokeGetIngestionOtelStats(aggStats, STORE_NAME);
    assertNotNull(result, "Should return a non-null stats object when enabled");
    assertFalse(
        result instanceof NoOpIngestionOtelStats,
        "Should return a real IngestionOtelStats, not a NoOp, when enabled");
  }

  @Test
  public void testGetIngestionOtelStatsReturnsNoOpWhenDisabled() throws Exception {
    AggVersionedIngestionStats aggStats = createAggStats(false);

    IngestionOtelStats result = invokeGetIngestionOtelStats(aggStats, STORE_NAME);
    assertTrue(result instanceof NoOpIngestionOtelStats, "Should return NoOpIngestionOtelStats when disabled");
    assertTrue(result == NoOpIngestionOtelStats.INSTANCE, "Should return the shared INSTANCE singleton");
  }

  private static VeniceMetricsRepository createOtelEnabledRepo(InMemoryMetricReader reader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
  }

  private static StoreIngestionTask mockTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isHybridMode()).thenReturn(false);
    return task;
  }

  private static String versionTopic(int version) {
    return Version.composeKafkaTopic(STORE_NAME, version);
  }

  private static String viewTopic(int version) {
    return viewTopic(version, "testView");
  }

  private static String viewTopic(int version, String viewName) {
    return versionTopic(version) + VeniceView.VIEW_NAME_SEPARATOR + viewName
        + MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
  }

  private void setStoreVersionInfo(
      AggVersionedIngestionStats aggStats,
      String storeName,
      int currentVersion,
      int... versions) {
    Store store = mock(Store.class);
    List<Version> versionList = Arrays.stream(versions)
        .mapToObj(version -> new VersionImpl(storeName, version, "push-" + version))
        .collect(Collectors.toList());
    when(store.getName()).thenReturn(storeName);
    when(store.getVersions()).thenReturn(versionList);
    when(store.getCurrentVersion()).thenReturn(currentVersion);
    doReturn(store).when(storeRepository).getStoreOrThrow(storeName);
    aggStats.handleStoreChanged(store);
  }

  private static Attributes attributes(String storeName, VersionRole role) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  private static Attributes attributes(String storeName, VersionRole role, ReplicaType replicaType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .build();
  }

  private static void assertGaugeValue(
      InMemoryMetricReader reader,
      String metricName,
      String storeName,
      VersionRole role,
      long expected) {
    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(reader, expected, attributes(storeName, role), metricName, TEST_PREFIX);
  }

  private static void assertNoGaugePoint(
      InMemoryMetricReader reader,
      String metricName,
      String storeName,
      VersionRole role) {
    assertNull(
        OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
            reader.collectAllMetrics(),
            metricName,
            TEST_PREFIX,
            attributes(storeName, role)));
  }

  // Helper methods to access private fields and methods via reflection

  @SuppressWarnings("unchecked")
  private Map<String, IngestionOtelStats> getOtelStatsMap(AggVersionedIngestionStats stats) throws Exception {
    Field field = AggVersionedIngestionStats.class.getDeclaredField("otelStats");
    field.setAccessible(true);
    Object registry = field.get(stats);
    Method method = registry.getClass().getDeclaredMethod("getStatsByStore");
    method.setAccessible(true);
    return (Map<String, IngestionOtelStats>) method.invoke(registry);
  }

  private void invokeCleanupVersionResources(AggVersionedIngestionStats stats, String storeName, int version)
      throws Exception {
    java.lang.reflect.Method method =
        AggVersionedIngestionStats.class.getDeclaredMethod("cleanupVersionResources", String.class, int.class);
    method.setAccessible(true);
    method.invoke(stats, storeName, version);
  }

  private void invokeOnVersionInfoUpdated(
      AggVersionedIngestionStats stats,
      String storeName,
      int currentVersion,
      int futureVersion) throws Exception {
    java.lang.reflect.Method method = AbstractVeniceAggVersionedStats.class
        .getDeclaredMethod("onVersionInfoUpdated", String.class, int.class, int.class);
    method.setAccessible(true);
    method.invoke(stats, storeName, currentVersion, futureVersion);
  }

  private Object getVersionInfo(IngestionOtelStats otelStats) throws Exception {
    java.lang.reflect.Method method = IngestionOtelStats.class.getDeclaredMethod("getVersionInfo");
    method.setAccessible(true);
    return method.invoke(otelStats);
  }

  private int getCurrentVersion(Object versionInfo) throws Exception {
    java.lang.reflect.Method method = versionInfo.getClass().getDeclaredMethod("getCurrentVersion");
    method.setAccessible(true);
    return (int) method.invoke(versionInfo);
  }

  private int getFutureVersion(Object versionInfo) throws Exception {
    java.lang.reflect.Method method = versionInfo.getClass().getDeclaredMethod("getFutureVersion");
    method.setAccessible(true);
    return (int) method.invoke(versionInfo);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, StoreIngestionTask> getIngestionTasksByVersion(IngestionOtelStats otelStats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("ingestionTasksByVersion");
    field.setAccessible(true);
    return (Map<Integer, StoreIngestionTask>) field.get(otelStats);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, Integer> getPushTimeoutByVersion(IngestionOtelStats otelStats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("pushTimeoutByVersion");
    field.setAccessible(true);
    return (Map<Integer, Integer>) field.get(otelStats);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, ?> getIdleTimeByVersion(IngestionOtelStats otelStats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("idleTimeByVersion");
    field.setAccessible(true);
    return (Map<Integer, ?>) field.get(otelStats);
  }

  private IngestionOtelStats invokeGetIngestionOtelStats(AggVersionedIngestionStats stats, String storeName)
      throws Exception {
    Method method = AggVersionedIngestionStats.class.getDeclaredMethod("getIngestionOtelStats", String.class);
    method.setAccessible(true);
    return (IngestionOtelStats) method.invoke(stats, storeName);
  }
}
