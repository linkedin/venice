package com.linkedin.davinci.stats;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.davinci.stats.ingestion.NoOpIngestionOtelStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.dimensions.VeniceDCROperation;
import com.linkedin.venice.stats.dimensions.VeniceIngestionFailureReason;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceWriteComputeOperation;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AggVersionedIngestionStats}.
 * Tests the version cleanup hooks and store deletion handling for OTel stats.
 */
public class AggVersionedIngestionStatsTest {
  private static final String STORE_NAME = "testStore";
  private static final String CLUSTER_NAME = "testCluster";
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
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(config.getClusterName()).thenReturn(CLUSTER_NAME);
    when(config.isUnregisterMetricForDeletedStoreEnabled()).thenReturn(true);
    when(config.isIngestionOtelStatsEnabled()).thenReturn(ingestionOtelStatsEnabled);
    doReturn(Int2ObjectMaps.emptyMap()).when(config).getKafkaClusterIdToAliasMap();
    return new AggVersionedIngestionStats(new MetricsRepository(), storeRepository, config);
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
    } else {
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should stay empty when disabled");
      // Cleanup should be a no-op when disabled — must not throw
      invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);
      assertTrue(otelStatsMap.isEmpty(), "OTel stats map should remain empty after cleanup when disabled");
    }
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
    aggStats.recordMaxIdleTime(STORE_NAME, VERSION_1, 1000);
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
    aggStats.recordWriteComputeTime(STORE_NAME, VERSION_1, VeniceWriteComputeOperation.QUERY, 5.0);
    aggStats.recordDcrLookupTime(STORE_NAME, VERSION_1, VeniceRecordType.DATA, 4.0);
    aggStats.recordDcrMergeTime(STORE_NAME, VERSION_1, VeniceDCROperation.PUT, 3.0);
    aggStats.recordUnexpectedMessageCount(STORE_NAME, VERSION_1);
    aggStats.recordStoreMetadataInconsistentCount(STORE_NAME, VERSION_1);
    aggStats.recordResubscriptionFailureCount(STORE_NAME, VERSION_1);
    aggStats.recordWriteComputeCacheHitCount(STORE_NAME, VERSION_1);
    aggStats.recordChecksumVerificationFailureCount(STORE_NAME, VERSION_1);
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

  // Helper methods to access private fields and methods via reflection

  @SuppressWarnings("unchecked")
  private Map<String, IngestionOtelStats> getOtelStatsMap(AggVersionedIngestionStats stats) throws Exception {
    Field field = AggVersionedIngestionStats.class.getDeclaredField("otelStatsMap");
    field.setAccessible(true);
    return (Map<String, IngestionOtelStats>) field.get(stats);
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
    java.lang.reflect.Method method =
        AggVersionedIngestionStats.class.getDeclaredMethod("onVersionInfoUpdated", String.class, int.class, int.class);
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
