package com.linkedin.davinci.stats;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggBlobTransferStatsTest {
  @Test
  public void testHostLevelBlobTransferMetrics() {
    TestMockTime mockTime = new TestMockTime();
    MetricsRepository metricsRepo = new MetricsRepository(mockTime);
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_foo");

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();
    doReturn("test-cluster").when(mockVeniceServerConfig).getClusterName();

    // Create AggVersionedBlobTransferStats
    AggVersionedBlobTransferStats aggVersionedStats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig, mockTime);

    // Create AggHostLevelIngestionStats
    AggHostLevelIngestionStats aggHostLevelStats = new AggHostLevelIngestionStats(
        metricsRepo,
        mockVeniceServerConfig,
        new HashMap<>(),
        mockMetaRepository,
        true,
        mockTime);

    // Create AggBlobTransferStats that wraps both
    AggBlobTransferStats aggBlobTransferStats = new AggBlobTransferStats(aggVersionedStats, aggHostLevelStats);

    Store mockStore = createStore(storeName);
    List<Store> storeList = new ArrayList<>();
    storeList.add(mockStore);

    doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
    doReturn(storeList).when(mockMetaRepository).getAllStores();

    aggVersionedStats.loadAllStats();
    storeName = mockStore.getName();

    // Record host-level blob transfer bytes received and sent
    aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, 1, 2048);
    aggBlobTransferStats.recordBlobTransferBytesSent(storeName, 1, 8192);

    // Advance time past the 30-second cache duration to get the rate calculation
    mockTime.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);

    // Expected rates
    double expectedReceivedRate = 2048.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
    double expectedSentRate = 8192.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;

    // Verify version-level metrics
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_bytes_received.IngestionStatsGauge").value(),
        expectedReceivedRate);
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_bytes_sent.IngestionStatsGauge").value(),
        expectedSentRate);

    // Verify host-level metrics
    Assert.assertEquals(reporter.query(".total--blob_transfer_bytes_received.Rate").value(), expectedReceivedRate);
    Assert.assertEquals(reporter.query(".total--blob_transfer_bytes_sent.Rate").value(), expectedSentRate);
  }

  @Test
  public void testHostLevelBlobTransferMetricsAcrossMultipleVersions() {
    TestMockTime mockTime = new TestMockTime();
    MetricsRepository metricsRepo = new MetricsRepository(mockTime);
    MockTehutiReporter reporter = new MockTehutiReporter();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);

    String storeName = Utils.getUniqueString("store_bar");

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();
    doReturn("test-cluster").when(mockVeniceServerConfig).getClusterName();

    // Create AggVersionedBlobTransferStats
    AggVersionedBlobTransferStats aggVersionedStats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig, mockTime);

    // Create AggHostLevelIngestionStats
    AggHostLevelIngestionStats aggHostLevelStats = new AggHostLevelIngestionStats(
        metricsRepo,
        mockVeniceServerConfig,
        new HashMap<>(),
        mockMetaRepository,
        true,
        mockTime);

    // Create AggBlobTransferStats that wraps both
    AggBlobTransferStats aggBlobTransferStats = new AggBlobTransferStats(aggVersionedStats, aggHostLevelStats);

    Store mockStore = createStore(storeName);
    List<Store> storeList = new ArrayList<>();
    storeList.add(mockStore);

    doReturn(mockStore).when(mockMetaRepository).getStoreOrThrow(any());
    doReturn(storeList).when(mockMetaRepository).getAllStores();

    aggVersionedStats.loadAllStats();
    storeName = mockStore.getName();

    // Record metrics for version 1
    aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, 1, 1000);
    aggBlobTransferStats.recordBlobTransferBytesSent(storeName, 1, 2000);

    // Record metrics for version 2
    aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, 2, 3000);
    aggBlobTransferStats.recordBlobTransferBytesSent(storeName, 2, 4000);

    // Advance time past the 30-second cache duration
    mockTime.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);

    // Host-level metrics should aggregate across all versions
    // Total bytes received: 1000 + 3000 = 4000
    // Total bytes sent: 2000 + 4000 = 6000
    double expectedReceivedRate = 4000.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
    double expectedSentRate = 6000.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;

    Assert.assertEquals(reporter.query(".total--blob_transfer_bytes_received.Rate").value(), expectedReceivedRate);
    Assert.assertEquals(reporter.query(".total--blob_transfer_bytes_sent.Rate").value(), expectedSentRate);
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
