package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.*;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.RejectionReason.NO_REPLICAS_AVAILABLE;
import static com.linkedin.venice.stats.dimensions.RejectionReason.THROTTLED_BY_LOAD_CONTROLLER;
import static com.linkedin.venice.stats.dimensions.RequestFanoutType.ORIGINAL;
import static com.linkedin.venice.stats.dimensions.RequestFanoutType.RETRY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntity;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class FastClientStatsTest {
  private FastClientStats createStats(InMemoryMetricReader inMemoryMetricReader) {
    String storeName = "test_store";
    Set<MetricEntity> allMetricEntities = new HashSet<>(CLIENT_METRIC_ENTITIES);
    for (FastClientMetricEntity entity: FastClientMetricEntity.values()) {
      allMetricEntities.add(entity.getMetricEntity());
    }
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(FAST_CLIENT, allMetricEntities, true, inMemoryMetricReader);
    return FastClientStats.getClientStats(metricsRepository, "", storeName, SINGLE_GET);
  }

  @Test
  public void testRetryRequestWin() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    stats.recordRetryRequestWin();

    // Check Tehuti metric
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    assertTrue(metrics.get(".test_store--retry_request_win.OccurrenceRate").value() > 0.0);

    // Check OTel metric
    Attributes expectedAttr = getBaseAttributes("test_store");
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttr,
        RETRY_REQUEST_WIN_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testMetadataStalenessHighWatermark() throws Exception {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    long deltaMs = 123L;
    stats.updateCacheTimestamp(System.currentTimeMillis() - deltaMs);

    // Check Tehuti metric - AsyncGauge uses the metric name as stat name
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    double value = metrics.get(".test_store--metadata_staleness_high_watermark_ms.Gauge").value();
    assertTrue(value >= deltaMs, "metadata staleness should be at least the delta set");

    // Check OTel metric - METADATA_STALENESS_DURATION is an ASYNC_GAUGE (gauge)
    Attributes expectedAttr =
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "test_store").build();

    // Since the gauge value is time-dependent, we'll just validate that a gauge metric exists
    // with the correct attributes and has a reasonable value (>= our delta)
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    boolean foundGaugeMetric = metricsData.stream()
        .filter(
            metricData -> metricData.getName()
                .equals("venice.fast_client." + METADATA_STALENESS_DURATION.getMetricEntity().getMetricName()))
        .flatMap(metricData -> metricData.getLongGaugeData().getPoints().stream())
        .anyMatch(point -> point.getAttributes().equals(expectedAttr) && point.getValue() >= deltaMs);

    assertTrue(foundGaugeMetric, "Should find metadata staleness gauge metric with value >= " + deltaMs);
  }

  @Test
  public void testOriginalFanoutSize() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    int fanoutSize = 5;
    stats.recordFanoutSize(fanoutSize);

    // Check Tehuti metrics
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();

    // Check if the specific metrics exist before accessing them
    Metric avgMetric = metrics.get(".test_store--fanout_size.Avg");
    Metric maxMetric = metrics.get(".test_store--fanout_size.Max");

    if (avgMetric == null) {
      throw new AssertionError(
          "Expected metric '.test_store--fanout_size.Avg' not found. Available metrics: " + metrics.keySet());
    }
    if (maxMetric == null) {
      throw new AssertionError(
          "Expected metric '.test_store--fanout_size.Max' not found. Available metrics: " + metrics.keySet());
    }

    assertEquals((int) avgMetric.value(), fanoutSize);
    assertEquals((int) maxMetric.value(), fanoutSize);

    // Check OTel metric
    Attributes expectedAttr = getAttributesWithFanoutType("test_store", ORIGINAL);
    validateHistogramPointData(
        inMemoryMetricReader,
        fanoutSize,
        fanoutSize,
        1,
        fanoutSize,
        expectedAttr,
        REQUEST_FANOUT_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testRetryFanoutSize() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    int fanoutSize = 3;
    stats.recordRetryFanoutSize(fanoutSize);

    // Check Tehuti metrics
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    assertEquals((int) metrics.get(".test_store--retry_fanout_size.Avg").value(), fanoutSize);
    assertEquals((int) metrics.get(".test_store--retry_fanout_size.Max").value(), fanoutSize);

    // Check OTel metric
    Attributes expectedAttr = getAttributesWithFanoutType("test_store", RETRY);
    validateHistogramPointData(
        inMemoryMetricReader,
        fanoutSize,
        fanoutSize,
        1,
        fanoutSize,
        expectedAttr,
        REQUEST_FANOUT_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testRejectionRatio() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    double ratio = 0.42;
    stats.recordRejectionRatio(ratio);

    // Check Tehuti metrics
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    assertEquals(metrics.get(".test_store--rejection_ratio.Avg").value(), ratio, 1e-9);
    assertEquals(metrics.get(".test_store--rejection_ratio.Max").value(), ratio, 1e-9);

    // Check OTel metric - REQUEST_REJECTION_RATIO is now MIN_MAX_COUNT_SUM_AGGREGATIONS (histogram)
    Attributes expectedAttr = getAttributesWithRejectionReason("test_store", THROTTLED_BY_LOAD_CONTROLLER);
    validateHistogramPointData(
        inMemoryMetricReader,
        ratio,
        ratio,
        1,
        ratio,
        expectedAttr,
        REQUEST_REJECTION_RATIO.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testNoAvailableReplicaRequestCount() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    stats.recordNoAvailableReplicaRequest();

    // Check Tehuti metric
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    assertTrue(metrics.get(".test_store--no_available_replica_request_count.OccurrenceRate").value() > 0.0);

    // Check OTel metric
    Attributes expectedAttr = getAttributesWithRejectionReason("test_store", NO_REPLICAS_AVAILABLE);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttr,
        REQUEST_REJECTION_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testRejectedRequestCountByLoadController() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    FastClientStats stats = createStats(inMemoryMetricReader);

    stats.recordRejectedRequestByLoadController();

    // Check Tehuti metric
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    assertTrue(metrics.get(".test_store--rejected_request_count_by_load_controller.OccurrenceRate").value() > 0.0);

    // Check OTel metric
    Attributes expectedAttr = getAttributesWithRejectionReason("test_store", THROTTLED_BY_LOAD_CONTROLLER);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttr,
        REQUEST_REJECTION_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  private Attributes getBaseAttributes(String storeName) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .build();
  }

  private Attributes getAttributesWithFanoutType(
      String storeName,
      com.linkedin.venice.stats.dimensions.RequestFanoutType fanoutType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(VENICE_REQUEST_FANOUT_TYPE.getDimensionNameInDefaultFormat(), fanoutType.getDimensionValue())
        .build();
  }

  private Attributes getAttributesWithRejectionReason(
      String storeName,
      com.linkedin.venice.stats.dimensions.RejectionReason rejectionReason) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(VENICE_REQUEST_REJECTION_REASON.getDimensionNameInDefaultFormat(), rejectionReason.getDimensionValue())
        .build();
  }

  @Test
  public void testFastClientTehutiMetricNameEnum() {
    Map<FastClientStats.FastClientTehutiMetricName, String> expectedNames = new HashMap<>();
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.LONG_TAIL_RETRY_REQUEST, "long_tail_retry_request");
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.ERROR_RETRY_REQUEST, "error_retry_request");
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.RETRY_REQUEST_WIN, "retry_request_win");
    expectedNames.put(
        FastClientStats.FastClientTehutiMetricName.METADATA_STALENESS_HIGH_WATERMARK_MS,
        "metadata_staleness_high_watermark_ms");
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.FANOUT_SIZE, "fanout_size");
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.RETRY_FANOUT_SIZE, "retry_fanout_size");
    expectedNames.put(
        FastClientStats.FastClientTehutiMetricName.NO_AVAILABLE_REPLICA_REQUEST_COUNT,
        "no_available_replica_request_count");
    expectedNames.put(
        FastClientStats.FastClientTehutiMetricName.REJECTED_REQUEST_COUNT_BY_LOAD_CONTROLLER,
        "rejected_request_count_by_load_controller");
    expectedNames.put(FastClientStats.FastClientTehutiMetricName.REJECTION_RATIO, "rejection_ratio");

    assertEquals(
        FastClientStats.FastClientTehutiMetricName.values().length,
        expectedNames.size(),
        "New FastClientTehutiMetricName values were added but not included in this test");

    for (FastClientStats.FastClientTehutiMetricName enumValue: FastClientStats.FastClientTehutiMetricName.values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }
}
