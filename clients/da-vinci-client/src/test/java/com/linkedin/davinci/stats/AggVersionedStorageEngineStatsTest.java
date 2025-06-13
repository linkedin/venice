package com.linkedin.davinci.stats;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import org.testng.annotations.Test;


public class AggVersionedStorageEngineStatsTest {
  @Test
  public void testVersionedStats() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    doReturn(mock(Sensor.class)).when(metricsRepository).sensor(anyString(), any());
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mock(Store.class)).when(metadataRepository).getStoreOrThrow(anyString());
    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false);
    stats.addStore(storeName);
    AggVersionedStorageEngineStats.StorageEngineStatsReporter reporter =
        new AggVersionedStorageEngineStats.StorageEngineStatsReporter(metricsRepository, storeName, "cluster1");
    reporter.registerStats();
    stats.getStats(storeName, 1).getKeyCountEstimate();
    stats.getStats(storeName, 1).getDuplicateKeyCountEstimate();

  }
}
