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
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Simulate version swap with no future version
    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 without future version");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroWhenFutureSizeIsZero() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5);
    stats.addStore(mockStore);

    // Current version has data, future version has 0 (still ingesting)
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when future version size is 0");
  }

  private Store createMockStoreWithVersions(String storeName, int currentVersionNum, int futureVersionNum) {
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(currentVersionNum).when(mockStore).getCurrentVersion();

    Version currentVersion = new VersionImpl(storeName, currentVersionNum, "push-job-1");
    currentVersion.setStatus(VersionStatus.ONLINE);

    Version futureVersion = new VersionImpl(storeName, futureVersionNum, "push-job-2");
    futureVersion.setStatus(VersionStatus.PUSHED);

    doReturn(Arrays.asList(currentVersion, futureVersion)).when(mockStore).getVersions();
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
