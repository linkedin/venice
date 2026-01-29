package com.linkedin.davinci.stats;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedStorageEngineStatsTest {
  @Test
  public void testVersionedStats() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mock(Store.class)).when(metadataRepository).getStoreOrThrow(anyString());
    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false);
    stats.addStore(storeName);
    stats.getStats(storeName, 1).getKeyCountEstimate();
    Metric metric = metricsRepository.getMetric(".testStore_total--rocksdb_key_count_estimate.Gauge");
    Assert.assertEquals(metric.value(), 0.0);
  }
}
