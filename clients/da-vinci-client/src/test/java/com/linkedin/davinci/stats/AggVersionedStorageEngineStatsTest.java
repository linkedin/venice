package com.linkedin.davinci.stats;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedStorageEngineStatsTest {
  @Test
  public void testVersionedStats() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());
    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false);
    stats.addStore(mockStore);
    stats.getStats(storeName, 1).getKeyCountEstimate();
    Metric metric = metricsRepository.getMetric(".testStore_total--rocksdb_key_count_estimate.Gauge");
    Assert.assertEquals(metric.value(), 0.0);
  }

  @Test
  public void testDiskSizeDropAlertFiresOnVersionSwap() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Set up storage engines: current version = 40GB, future version = 1MB (huge drop)
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    // Simulate version swap event - this is where the check happens
    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric, "Alert gauge should be registered");
    Assert.assertEquals(alertMetric.value(), 1.0, "Alert should record 1 when future version is much smaller");
  }

  @Test
  public void testDiskSizeDropAlertResetsWhenSizesSimilar() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Set up storage engines: current version = 40GB, future version = 38GB (similar)
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 38L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when sizes are similar");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroWithNoFutureVersion() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(1).when(mockStore).getCurrentVersion();
    // Explicitly return empty versions list — no future version exists
    doReturn(Collections.emptyList()).when(mockStore).getVersions();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Simulate store change with no future version
    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 without future version");
  }

  @Test
  public void testDiskSizeDropAlertFiresWhenFutureSizeIsZero() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Current version has data, future version has 0 bytes — total data loss.
    // Since future version is PUSHED (ingestion complete), this is a real alert.
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert
        .assertEquals(alertMetric.value(), 1.0, "Alert should fire when PUSHED future version has 0 bytes (data loss)");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroWhenCurrentSizeIsZero() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Current version has no data (e.g., first version of a store), future has data
    setStorageEngineSize(stats, storeName, 2, 40L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when current version has no data");
  }

  @Test
  public void testDiskSizeDropAlertSkipsStartedVersion() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    // Future version is STARTED (still ingesting), not PUSHED
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2, VersionStatus.STARTED);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Even though sizes show a huge drop, the STARTED guard should prevent the alert
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should not fire when future version is still STARTED");
  }

  @Test
  public void testDiskSizeDropAlertSensorCleanedUpOnStoreDelete() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, true, 0.5);
    stats.addStore(mockStore);

    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    // Trigger alert to create the sensor
    stats.handleStoreChanged(mockStore);
    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric, "Sensor should exist before deletion");

    // Delete the store
    stats.handleStoreDeleted(storeName);

    // Verify the sensor is removed from MetricsRepository
    Metric removedMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNull(removedMetric, "Sensor should be removed from MetricsRepository after store deletion");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroOnException() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Set current version with a storage engine that throws on getStats()
    StorageEngine throwingEngine = mock(StorageEngine.class);
    StorageEngineStats throwingStats = mock(StorageEngineStats.class);
    when(throwingEngine.getStats()).thenReturn(throwingStats);
    when(throwingStats.getStoreSizeInBytes()).thenThrow(new RuntimeException("RocksDB error"));
    stats.getStats(storeName, 1).setStorageEngine(throwingEngine);

    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when exception occurs");
  }

  @Test
  public void testDiskSizeDropAlertFiresThenResetsOnNextVersionSwap() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);

    // First: version 2 is PUSHED with a huge drop — alert should fire
    Store mockStore1 = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore1).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore1);

    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore1);
    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertEquals(alertMetric.value(), 1.0, "Alert should fire on bad version swap");

    // Second: version swap completes — version 2 is now current, no future version
    Store mockStore2 = mock(Store.class);
    doReturn(storeName).when(mockStore2).getName();
    doReturn(2).when(mockStore2).getCurrentVersion();
    Version onlineVersion = new VersionImpl(storeName, 2, "push-job-2");
    onlineVersion.setStatus(VersionStatus.ONLINE);
    doReturn(Collections.singletonList(onlineVersion)).when(mockStore2).getVersions();
    doReturn(mockStore2).when(metadataRepository).getStoreOrThrow(anyString());

    stats.handleStoreChanged(mockStore2);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should reset to 0 after version swap completes");
  }

  private Store createMockStoreWithVersions(String storeName, int currentVersionNum, int futureVersionNum) {
    return createMockStoreWithVersions(storeName, currentVersionNum, futureVersionNum, VersionStatus.PUSHED);
  }

  private Store createMockStoreWithVersions(
      String storeName,
      int currentVersionNum,
      int futureVersionNum,
      VersionStatus futureStatus) {
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(currentVersionNum).when(mockStore).getCurrentVersion();

    Version currentVersion = new VersionImpl(storeName, currentVersionNum, "push-job-1");
    currentVersion.setStatus(VersionStatus.ONLINE);

    Version futureVersion = new VersionImpl(storeName, futureVersionNum, "push-job-2");
    futureVersion.setStatus(futureStatus);

    doReturn(Arrays.asList(currentVersion, futureVersion)).when(mockStore).getVersions();
    doReturn(currentVersion).when(mockStore).getVersion(currentVersionNum);
    doReturn(futureVersion).when(mockStore).getVersion(futureVersionNum);
    return mockStore;
  }

  private void setStorageEngineSize(
      AggVersionedStorageEngineStats stats,
      String storeName,
      int version,
      long sizeInBytes) {
    StorageEngine mockEngine = mock(StorageEngine.class);
    StorageEngineStats mockEngineStats = mock(StorageEngineStats.class);
    when(mockEngine.getStats()).thenReturn(mockEngineStats);
    when(mockEngineStats.getStoreSizeInBytes()).thenReturn(sizeInBytes);
    stats.getStats(storeName, version).setStorageEngine(mockEngine);
  }
}
