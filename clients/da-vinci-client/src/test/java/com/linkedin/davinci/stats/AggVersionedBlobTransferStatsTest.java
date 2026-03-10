package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static java.lang.Double.NaN;
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
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.ArrayList;
import java.util.Collection;
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
    TestMockTime mockTime = new TestMockTime();

    String storeName = Utils.getUniqueString("store_foo");

    metricsRepo.addReporter(reporter);
    ReadOnlyStoreRepository mockMetaRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockVeniceServerConfig).isUnregisterMetricForDeletedStoreEnabled();

    AggVersionedBlobTransferStats stats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockVeniceServerConfig, mockTime);

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
    // Record blob transfer bytes received
    stats.recordBlobTransferBytesReceived(storeName, 1, 1024);
    // Advance time past the 30-second cache duration to get the rate calculation
    mockTime.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    // Expected rate: 1024 bytes / 30 seconds = 34.13 bytes/sec
    double expectedRate = 1024.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_bytes_received.IngestionStatsGauge").value(),
        expectedRate);
    // Record blob transfer bytes sent
    stats.recordBlobTransferBytesSent(storeName, 1, 4096);
    expectedRate = 4096.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
    Assert.assertEquals(
        reporter.query("." + storeName + "_total--blob_transfer_bytes_sent.IngestionStatsGauge").value(),
        expectedRate);

  }

  /**
   * Verifies that Tehuti-only recording methods ({@link AggVersionedBlobTransferStats#recordBlobTransferResponsesCount}
   * and {@link AggVersionedBlobTransferStats#recordBlobTransferFileReceiveThroughput}) do NOT produce OTel metrics,
   * while dual-recording methods do.
   */
  @Test
  public void testTehutiOnlyMethodsDoNotProduceOtelMetrics() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    String metricPrefix = "server";
    try (VeniceMetricsRepository otelRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(metricPrefix)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build())) {

      VeniceServerConfig mockConfig = Mockito.mock(VeniceServerConfig.class);
      doReturn(Int2ObjectMaps.emptyMap()).when(mockConfig).getKafkaClusterIdToAliasMap();
      doReturn(true).when(mockConfig).isUnregisterMetricForDeletedStoreEnabled();
      doReturn("test-cluster").when(mockConfig).getClusterName();

      ReadOnlyStoreRepository mockMetaRepo = mock(ReadOnlyStoreRepository.class);
      String storeName = Utils.getUniqueString("store_foo");
      Store mockStore = createStore(storeName);
      List<Store> storeList = new ArrayList<>();
      storeList.add(mockStore);
      doReturn(mockStore).when(mockMetaRepo).getStoreOrThrow(any());
      doReturn(storeList).when(mockMetaRepo).getAllStores();

      AggVersionedBlobTransferStats stats = new AggVersionedBlobTransferStats(otelRepo, mockMetaRepo, mockConfig);
      stats.loadAllStats();
      storeName = mockStore.getName();

      // Call Tehuti-only methods
      stats.recordBlobTransferResponsesCount(storeName, 1);
      stats.recordBlobTransferFileReceiveThroughput(storeName, 1, 1000.0);

      // Verify no OTel RESPONSE_COUNT metric was produced
      String responseCountMetric = BlobTransferOtelMetricEntity.RESPONSE_COUNT.getMetricEntity().getMetricName();
      Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
      OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(metricsData, responseCountMetric, metricPrefix, null);

      // Now call a dual-recording method and verify OTel IS produced
      stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, true);
      metricsData = inMemoryMetricReader.collectAllMetrics();
      String fullName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + responseCountMetric;
      MetricData data = metricsData.stream().filter(md -> md.getName().equals(fullName)).findFirst().orElse(null);
      Assert.assertNotNull(data, "OTel RESPONSE_COUNT should be present after dual-recording method call");
    }
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
