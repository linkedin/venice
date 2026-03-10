package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.CLUSTER_NAME;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.METRIC_PREFIX;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.buildVersionRoleAttributes;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createMockMetaRepository;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createMockServerConfig;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createStore;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggBlobTransferStatsTest {
  @Test
  public void testHostLevelBlobTransferMetrics() {
    TestMockTime mockTime = new TestMockTime();
    MetricsRepository metricsRepo = new MetricsRepository(mockTime);
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepo.addReporter(reporter);

    String storeName = Utils.getUniqueString("store_foo");
    Store mockStore = createStore(storeName);
    ReadOnlyStoreRepository mockMetaRepository = createMockMetaRepository(mockStore);
    VeniceServerConfig mockServerConfig = createMockServerConfig();

    AggVersionedBlobTransferStats aggVersionedStats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockServerConfig, mockTime);
    AggHostLevelIngestionStats aggHostLevelStats = new AggHostLevelIngestionStats(
        metricsRepo,
        mockServerConfig,
        Collections.emptyMap(),
        mockMetaRepository,
        true,
        mockTime);
    AggBlobTransferStats aggBlobTransferStats = new AggBlobTransferStats(aggVersionedStats, aggHostLevelStats);

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
    metricsRepo.addReporter(reporter);

    String storeName = Utils.getUniqueString("store_bar");
    Store mockStore = createStore(storeName);
    ReadOnlyStoreRepository mockMetaRepository = createMockMetaRepository(mockStore);
    VeniceServerConfig mockServerConfig = createMockServerConfig();

    AggVersionedBlobTransferStats aggVersionedStats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, mockServerConfig, mockTime);
    AggHostLevelIngestionStats aggHostLevelStats = new AggHostLevelIngestionStats(
        metricsRepo,
        mockServerConfig,
        Collections.emptyMap(),
        mockMetaRepository,
        true,
        mockTime);
    AggBlobTransferStats aggBlobTransferStats = new AggBlobTransferStats(aggVersionedStats, aggHostLevelStats);

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

  /**
   * Validates OTel metrics are produced when bytes are recorded through AggBlobTransferStats.
   * Uses VeniceMetricsRepository with InMemoryMetricReader for OTel validation.
   */
  @Test
  public void testOtelMetricsForBytesReceivedAndSent() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository metricsRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build())) {

      String storeName = Utils.getUniqueString("store_otel");
      Store mockStore = createStore(storeName);
      ReadOnlyStoreRepository mockMetaRepo = createMockMetaRepository(mockStore);
      VeniceServerConfig mockServerConfig = createMockServerConfig();

      AggVersionedBlobTransferStats aggVersionedStats =
          new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepo, mockServerConfig);
      AggHostLevelIngestionStats aggHostLevelStats = new AggHostLevelIngestionStats(
          metricsRepo,
          mockServerConfig,
          Collections.emptyMap(),
          mockMetaRepo,
          true,
          new TestMockTime());
      AggBlobTransferStats aggBlobTransferStats = new AggBlobTransferStats(aggVersionedStats, aggHostLevelStats);

      aggVersionedStats.loadAllStats();
      storeName = mockStore.getName();

      // Record bytes for two versions
      aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, 1, 1000);
      aggBlobTransferStats.recordBlobTransferBytesSent(storeName, 1, 2000);
      aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, 2, 3000);
      aggBlobTransferStats.recordBlobTransferBytesSent(storeName, 2, 4000);

      // OTel counters accumulate per VersionRole — both versions are BACKUP (no current/future set)
      String otelBytesReceived = BlobTransferOtelMetricEntity.BYTES_RECEIVED.getMetricEntity().getMetricName();
      String otelBytesSent = BlobTransferOtelMetricEntity.BYTES_SENT.getMetricEntity().getMetricName();
      Attributes backupAttrs = buildVersionRoleAttributes(storeName, CLUSTER_NAME, VersionRole.BACKUP);
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          inMemoryMetricReader,
          1000 + 3000,
          backupAttrs,
          otelBytesReceived,
          METRIC_PREFIX);
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          inMemoryMetricReader,
          2000 + 4000,
          backupAttrs,
          otelBytesSent,
          METRIC_PREFIX);
    }
  }
}
