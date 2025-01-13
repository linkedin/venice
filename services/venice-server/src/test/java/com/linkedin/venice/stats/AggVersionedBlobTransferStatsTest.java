package com.linkedin.venice.stats;

import static java.lang.Double.NaN;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedBlobTransferStatsTest {
  @Test
  public void testRecordBlobTransferMetrics() {
    MetricsRepository metricsRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_foo");

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();

    AggVersionedBlobTransferStats stats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig);

    Store mockStore = createStore(storeName);
    List<Store> storeList = new ArrayList<>();
    storeList.add(mockStore);

    doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
    doReturn(storeList).when(mockMetaRepository).getAllStores();

    stats.loadAllStats();
    storeName = mockStore.getName();
    // initial stats
    // Gauge default value is NaN
    Assert
        .assertEquals(reporter.query("." + storeName + "_total--blob_transfer_time.IngestionStatsGauge").value(), NaN);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_file_receive_throughput.IngestionStatsGauge").value(),
        NaN);
    // Count default value is 0.0
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_failed_num_responses.IngestionStatsGauge").value(),
        0.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_successful_num_responses.IngestionStatsGauge").value(),
        0.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_total_num_responses.IngestionStatsGauge").value(),
        0.0);

    // Record response count
    stats.recordBlobTransferResponsesCount(storeName, 1);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_total_num_responses.IngestionStatsGauge").value(),
        1.0);
    // Record response status
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, true);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_successful_num_responses.IngestionStatsGauge").value(),
        1.0);

    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, false);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_failed_num_responses.IngestionStatsGauge").value(),
        1.0);

    // Record file receive throughput
    stats.recordBlobTransferFileReceiveThroughput(storeName, 1, 1000.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_file_receive_throughput.IngestionStatsGauge").value(),
        1000.0);

    // Record blob transfer time
    stats.recordBlobTransferTimeInSec(storeName, 1, 20.0);
    Assert
        .assertEquals(reporter.query("." + storeName + "_total--blob_transfer_time.IngestionStatsGauge").value(), 20.0);
  }

  private Store createStore(String storeName) {
    return new ZKStore(
        storeName,
        "",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }
}
