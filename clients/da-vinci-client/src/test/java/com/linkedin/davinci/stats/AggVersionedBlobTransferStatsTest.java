package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.CLUSTER_NAME;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.METRIC_PREFIX;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.buildResponseCountAttributes;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.buildVersionRoleAttributes;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createMockMetaRepository;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createMockServerConfig;
import static com.linkedin.davinci.stats.BlobTransferStatsTestUtils.createStore;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static java.lang.Double.NaN;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedBlobTransferStatsTest {
  @Test
  public void testRecordBlobTransferMetrics() {
    MetricsRepository metricsRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    TestMockTime mockTime = new TestMockTime();
    metricsRepo.addReporter(reporter);

    String storeName = Utils.getUniqueString("store_foo");
    Store mockStore = createStore(storeName);
    ReadOnlyStoreRepository mockMetaRepository = createMockMetaRepository(mockStore);

    AggVersionedBlobTransferStats stats =
        new AggVersionedBlobTransferStats(metricsRepo, mockMetaRepository, createMockServerConfig(), mockTime);

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
   * Tests that all dual-recording methods produce both Tehuti and OTel metrics.
   * Uses VeniceMetricsRepository with InMemoryMetricReader to validate OTel alongside Tehuti.
   */
  @Test
  public void testDualRecordingMethodsProduceBothTehutiAndOtel() throws Exception {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    TestMockTime mockTime = new TestMockTime();
    // Use a dedicated executor to avoid contention with the shared DEFAULT_ASYNC_GAUGE_EXECUTOR in CI
    try (AsyncGauge.AsyncGaugeExecutor executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
        VeniceMetricsRepository otelRepo = new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
                .setMetricEntities(SERVER_METRIC_ENTITIES)
                .setEmitOtelMetrics(true)
                .setOtelAdditionalMetricsReader(inMemoryMetricReader)
                .setTehutiMetricConfig(new MetricConfig(executor))
                .build())) {

      MockTehutiReporter reporter = new MockTehutiReporter();
      otelRepo.addReporter(reporter);

      String storeName = Utils.getUniqueString("store_foo");
      Store mockStore = createStore(storeName);
      ReadOnlyStoreRepository mockMetaRepo = createMockMetaRepository(mockStore);

      AggVersionedBlobTransferStats stats =
          new AggVersionedBlobTransferStats(otelRepo, mockMetaRepo, createMockServerConfig(), mockTime);
      stats.loadAllStats();
      storeName = mockStore.getName();

      String otelResponseCount = BlobTransferOtelMetricEntity.RESPONSE_COUNT.getMetricEntity().getMetricName();
      String otelTime = BlobTransferOtelMetricEntity.TIME.getMetricEntity().getMetricName();
      String otelBytesReceived = BlobTransferOtelMetricEntity.BYTES_RECEIVED.getMetricEntity().getMetricName();
      String otelBytesSent = BlobTransferOtelMetricEntity.BYTES_SENT.getMetricEntity().getMetricName();

      // --- recordBlobTransferResponsesBasedOnBoostrapStatus (success) ---
      stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, true);
      // Tehuti
      Assert.assertEquals(
          reporter.query("." + storeName + "_total--blob_transfer_successful_num_responses.IngestionStatsGauge")
              .value(),
          1.0);
      // OTel: RESPONSE_COUNT with SUCCESS dimension — version 1 is BACKUP (no current/future set)
      Attributes successAttrs = buildResponseCountAttributes(
          storeName,
          CLUSTER_NAME,
          VersionRole.BACKUP,
          VeniceResponseStatusCategory.SUCCESS);
      OpenTelemetryDataTestUtils
          .validateLongPointDataFromCounter(inMemoryMetricReader, 1, successAttrs, otelResponseCount, METRIC_PREFIX);

      // --- recordBlobTransferResponsesBasedOnBoostrapStatus (failure) ---
      stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, false);
      // Tehuti
      Assert.assertEquals(
          reporter.query("." + storeName + "_total--blob_transfer_failed_num_responses.IngestionStatsGauge").value(),
          1.0);
      // OTel: RESPONSE_COUNT with FAIL dimension
      Attributes failAttrs =
          buildResponseCountAttributes(storeName, CLUSTER_NAME, VersionRole.BACKUP, VeniceResponseStatusCategory.FAIL);
      OpenTelemetryDataTestUtils
          .validateLongPointDataFromCounter(inMemoryMetricReader, 1, failAttrs, otelResponseCount, METRIC_PREFIX);

      // --- recordBlobTransferTimeInSec ---
      stats.recordBlobTransferTimeInSec(storeName, 1, 20.0);
      // Tehuti
      Assert.assertEquals(
          reporter.query("." + storeName + "_total--blob_transfer_time.IngestionStatsGauge").value(),
          20.0);
      // OTel: TIME histogram
      Attributes backupAttrs = buildVersionRoleAttributes(storeName, CLUSTER_NAME, VersionRole.BACKUP);
      OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
          inMemoryMetricReader,
          20.0,
          20.0,
          1,
          20.0,
          backupAttrs,
          otelTime,
          METRIC_PREFIX);

      // --- recordBlobTransferBytesReceived ---
      stats.recordBlobTransferBytesReceived(storeName, 1, 1024);
      // Tehuti
      mockTime.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
      double expectedRate = 1024.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
      Assert.assertEquals(
          reporter.query("." + storeName + "_total--blob_transfer_bytes_received.IngestionStatsGauge").value(),
          expectedRate);
      // OTel: BYTES_RECEIVED counter
      OpenTelemetryDataTestUtils
          .validateLongPointDataFromCounter(inMemoryMetricReader, 1024, backupAttrs, otelBytesReceived, METRIC_PREFIX);

      // --- recordBlobTransferBytesSent ---
      stats.recordBlobTransferBytesSent(storeName, 1, 4096);
      // Tehuti
      expectedRate = 4096.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS;
      Assert.assertEquals(
          reporter.query("." + storeName + "_total--blob_transfer_bytes_sent.IngestionStatsGauge").value(),
          expectedRate);
      // OTel: BYTES_SENT counter
      OpenTelemetryDataTestUtils
          .validateLongPointDataFromCounter(inMemoryMetricReader, 4096, backupAttrs, otelBytesSent, METRIC_PREFIX);
    }
  }

  /**
   * Verifies that Tehuti-only recording methods ({@link AggVersionedBlobTransferStats#recordBlobTransferResponsesCount}
   * and {@link AggVersionedBlobTransferStats#recordBlobTransferFileReceiveThroughput}) do NOT produce OTel metrics,
   * while dual-recording methods do.
   */
  @Test
  public void testTehutiOnlyMethodsDoNotProduceOtelMetrics() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository otelRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build())) {

      String storeName = Utils.getUniqueString("store_foo");
      Store mockStore = createStore(storeName);
      ReadOnlyStoreRepository mockMetaRepo = createMockMetaRepository(mockStore);

      AggVersionedBlobTransferStats stats =
          new AggVersionedBlobTransferStats(otelRepo, mockMetaRepo, createMockServerConfig());
      stats.loadAllStats();
      storeName = mockStore.getName();

      // Call Tehuti-only methods
      stats.recordBlobTransferResponsesCount(storeName, 1);
      stats.recordBlobTransferFileReceiveThroughput(storeName, 1, 1000.0);

      // Verify no OTel RESPONSE_COUNT metric was produced
      String responseCountMetric = BlobTransferOtelMetricEntity.RESPONSE_COUNT.getMetricEntity().getMetricName();
      Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
      OpenTelemetryDataTestUtils
          .assertNoLongSumDataForAttributes(metricsData, responseCountMetric, METRIC_PREFIX, null);

      // Now call a dual-recording method and verify OTel IS produced
      stats.recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, 1, true);
      metricsData = inMemoryMetricReader.collectAllMetrics();
      String fullName = DEFAULT_METRIC_PREFIX + METRIC_PREFIX + "." + responseCountMetric;
      MetricData data = metricsData.stream().filter(md -> md.getName().equals(fullName)).findFirst().orElse(null);
      Assert.assertNotNull(data, "OTel RESPONSE_COUNT should be present after dual-recording method call");
    }
  }
}
