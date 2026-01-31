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
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.lang.reflect.Field;
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

  private MetricsRepository metricsRepository;
  private ReadOnlyStoreRepository storeRepository;
  private VeniceServerConfig serverConfig;
  private AggVersionedIngestionStats aggStats;

  @BeforeMethod
  public void setUp() {
    metricsRepository = new MetricsRepository();
    storeRepository = mock(ReadOnlyStoreRepository.class);
    serverConfig = mock(VeniceServerConfig.class);

    when(serverConfig.getClusterName()).thenReturn(CLUSTER_NAME);
    when(serverConfig.isUnregisterMetricForDeletedStoreEnabled()).thenReturn(true);
    doReturn(Int2ObjectMaps.emptyMap()).when(serverConfig).getKafkaClusterIdToAliasMap();

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(STORE_NAME);
    when(mockStore.getVersions()).thenReturn(Collections.emptyList());
    when(mockStore.getCurrentVersion()).thenReturn(0);
    doReturn(mockStore).when(storeRepository).getStoreOrThrow(anyString());

    aggStats = new AggVersionedIngestionStats(metricsRepository, storeRepository, serverConfig);
  }

  @Test
  public void testSetIngestionTaskCreatesOtelStats() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set ingestion task for a version
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    // Verify OTel stats were created
    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    assertTrue(otelStatsMap.containsKey(STORE_NAME), "OTel stats should be created for the store");
  }

  @Test
  public void testCleanupVersionResourcesRemovesVersionData() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set up ingestion tasks for multiple versions
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_2, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
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

    // Simulate version removal by calling the hook directly
    // In real scenario, this is called from AbstractVeniceAggVersionedStats.updateStatsVersionInfo
    invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);

    // Verify VERSION_1 data was cleaned up
    assertFalse(tasksByVersion.containsKey(VERSION_1), "Task should be removed for VERSION_1 after cleanup");
    assertFalse(
        pushTimeoutByVersion.containsKey(VERSION_1),
        "Push timeout should be removed for VERSION_1 after cleanup");
    assertFalse(idleTimeByVersion.containsKey(VERSION_1), "Idle time should be removed for VERSION_1 after cleanup");

    // Verify VERSION_2 data still exists
    assertTrue(tasksByVersion.containsKey(VERSION_2), "Task should still exist for VERSION_2 after cleanup");
  }

  @Test
  public void testHandleStoreDeletedCleansUpOtelStats() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set up ingestion task
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    assertTrue(otelStatsMap.containsKey(STORE_NAME), "OTel stats should exist before deletion");

    // Delete the store
    aggStats.handleStoreDeleted(STORE_NAME);

    // Verify OTel stats were removed
    assertFalse(otelStatsMap.containsKey(STORE_NAME), "OTel stats should be removed after store deletion");
  }

  @Test
  public void testHandleStoreDeletedWithNoOtelStats() {
    // Delete a store that doesn't have OTel stats (should not throw)
    aggStats.handleStoreDeleted("nonExistentStore");
  }

  @Test
  public void testVersionInfoUpdateTriggersCleanup() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set up ingestion tasks for versions 1, 2, and 3
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_2, mockTask);
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_3, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);

    // Set some state for all versions
    otelStats.setIngestionTaskPushTimeoutGauge(VERSION_1, 1);
    otelStats.setIngestionTaskPushTimeoutGauge(VERSION_2, 1);
    otelStats.setIngestionTaskPushTimeoutGauge(VERSION_3, 1);

    // Verify all versions have data before cleanup
    Map<Integer, Integer> pushTimeoutByVersion = getPushTimeoutByVersion(otelStats);
    assertTrue(pushTimeoutByVersion.containsKey(VERSION_1), "Push timeout should exist for VERSION_1");
    assertTrue(pushTimeoutByVersion.containsKey(VERSION_2), "Push timeout should exist for VERSION_2");
    assertTrue(pushTimeoutByVersion.containsKey(VERSION_3), "Push timeout should exist for VERSION_3");

    // Update version info - simulate that only version 2 and 3 exist now (version 1 removed)
    otelStats.updateVersionInfo(VERSION_2, VERSION_3);

    // Clean up version 1 via the hook
    invokeCleanupVersionResources(aggStats, STORE_NAME, VERSION_1);

    // Verify VERSION_1 data was cleaned up
    assertFalse(pushTimeoutByVersion.containsKey(VERSION_1), "Push timeout should be removed for VERSION_1");

    // Verify VERSION_2 and VERSION_3 data still exists
    assertTrue(pushTimeoutByVersion.containsKey(VERSION_2), "Push timeout should still exist for VERSION_2");
    assertTrue(pushTimeoutByVersion.containsKey(VERSION_3), "Push timeout should still exist for VERSION_3");
  }

  @Test
  public void testOnVersionInfoUpdatedUpdatesOtelStats() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    when(mockTask.isHybridMode()).thenReturn(false);

    // Set up ingestion task
    aggStats.setIngestionTask(STORE_NAME + "_v" + VERSION_1, mockTask);

    Map<String, IngestionOtelStats> otelStatsMap = getOtelStatsMap(aggStats);
    IngestionOtelStats otelStats = otelStatsMap.get(STORE_NAME);

    // Call onVersionInfoUpdated via reflection
    invokeOnVersionInfoUpdated(aggStats, STORE_NAME, VERSION_2, VERSION_3);

    // Verify version info was updated via reflection (package-private method)
    Object versionInfo = getVersionInfo(otelStats);
    assertNotNull(versionInfo, "Version info should not be null");
    assertEquals(getCurrentVersion(versionInfo), VERSION_2);
    assertEquals(getFutureVersion(versionInfo), VERSION_3);
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
}
