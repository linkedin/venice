package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.stats.RouterHttpRequestStats.RouterTehutiMetricNameEnum.HEALTHY_REQUEST;
import static com.linkedin.venice.router.stats.RouterHttpRequestStats.RouterTehutiMetricNameEnum.KEY_SIZE_IN_BYTE;
import static com.linkedin.venice.router.stats.RouterHttpRequestStats.RouterTehutiMetricNameEnum.REQUEST_SIZE;
import static com.linkedin.venice.router.stats.RouterHttpRequestStats.RouterTehutiMetricNameEnum.RESPONSE_SIZE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import org.testng.annotations.Test;


public class RouterHttpRequestStatsTest {
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void routerMetricsTest(boolean useVeniceMetricRepository, boolean isOtelEnabled) {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    MetricsRepository metricsRepository;
    if (useVeniceMetricRepository) {
      Collection<MetricEntity> metricEntities = new ArrayList<>();
      metricEntities.add(
          new MetricEntity(
              "test_metric",
              MetricType.HISTOGRAM,
              MetricUnit.MILLISECOND,
              "Test description",
              Utils.setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)));
      metricsRepository = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository(
          isOtelEnabled,
          isOtelEnabled ? PASCAL_CASE : VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat(),
          metricEntities);
    } else {
      metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    }
    metricsRepository.addReporter(new MockTehutiReporter());

    RouterHttpRequestStats routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        true,
        null);

    if (useVeniceMetricRepository && isOtelEnabled) {
      assertTrue(routerHttpRequestStats.emitOpenTelemetryMetrics(), "Otel should be enabled");
      VeniceOpenTelemetryMetricsRepository otelRepository = routerHttpRequestStats.getOtelRepository();
      assertNotNull(otelRepository);
      Attributes baseAttributes = routerHttpRequestStats.getBaseAttributes();
      assertNotNull(baseAttributes);
      baseAttributes.forEach((key, value) -> {
        if (key.getKey().equals(VENICE_STORE_NAME.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, storeName);
        } else if (key.getKey().equals(VENICE_REQUEST_METHOD.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, RequestType.SINGLE_GET.name().toLowerCase());
        } else if (key.getKey().equals(VENICE_CLUSTER_NAME.getDimensionName(PASCAL_CASE))) {
          assertEquals(value, clusterName);
        }
      });
      Map<VeniceMetricsDimensions, String> baseDimensionsMap = routerHttpRequestStats.getBaseDimensionsMap();
      assertTrue(baseDimensionsMap.containsKey(VENICE_STORE_NAME));
      assertTrue(baseDimensionsMap.containsKey(VENICE_REQUEST_METHOD));
      assertTrue(baseDimensionsMap.containsKey(VENICE_CLUSTER_NAME));
      assertEquals(baseDimensionsMap.size(), 3);
    } else {
      assertFalse(routerHttpRequestStats.emitOpenTelemetryMetrics(), "Otel should not be enabled");
      assertNull(routerHttpRequestStats.getOtelRepository());
    }

    routerHttpRequestStats.recordHealthyRequest(1.0, HttpResponseStatus.OK, 1);
    assertEquals(
        metricsRepository.getMetric("." + storeName + "--" + HEALTHY_REQUEST.getMetricName() + ".Count").value(),
        1.0);

    routerHttpRequestStats.recordRequestSize(512.0);
    // Verify that the request size is recorded correctly
    assertEquals(
        metricsRepository.getMetric("." + storeName + "--" + REQUEST_SIZE.getMetricName() + ".Avg").value(),
        512.0);

    // Verify that the response size is recorded
    routerHttpRequestStats.recordResponseSize(1024.0);
    assertEquals(
        metricsRepository.getMetric("." + storeName + "--" + RESPONSE_SIZE.getMetricName() + ".Avg").value(),
        1024.0);

  }

  @Test
  public void testKeyValueProfilingEnabled() {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    metricsRepository.addReporter(new MockTehutiReporter());

    RouterHttpRequestStats routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        true, // keyValueProfilingEnabled = true
        null);

    // Record key size metric
    routerHttpRequestStats.recordKeySizeInByte(128);

    // Test response size metric (should always work)
    routerHttpRequestStats.recordResponseSize(1024.0);
    String responseSizeMetricName = "." + storeName + "--" + RESPONSE_SIZE.getMetricName() + ".Avg";
    assertNotNull(metricsRepository.getMetric(responseSizeMetricName));
    assertEquals(metricsRepository.getMetric(responseSizeMetricName).value(), 1024.0);
  }

  @Test
  public void testKeyValueProfilingDisabled() {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    metricsRepository.addReporter(new MockTehutiReporter());

    RouterHttpRequestStats routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        false, // keyValueProfilingEnabled = false
        null);

    // Record key size metric
    routerHttpRequestStats.recordKeySizeInByte(128);

    // Key size metric should not exist when profiling is disabled
    String keySizeMetricName = "." + storeName + "--" + KEY_SIZE_IN_BYTE.getMetricName() + ".Avg";
    assertNull(metricsRepository.getMetric(keySizeMetricName));

    // Response size should still work
    routerHttpRequestStats.recordResponseSize(1024.0);
    String responseSizeMetricName = "." + storeName + "--" + RESPONSE_SIZE.getMetricName() + ".Avg";
    assertNotNull(metricsRepository.getMetric(responseSizeMetricName));
  }

  @Test
  public void testEmitRequestSizeMetrics() {
    runCallSizeMetricTest(MessageType.REQUEST, 512, RouterHttpRequestStats::recordRequestSize);
  }

  @Test
  public void testEmitResponseSizeMetrics() {
    runCallSizeMetricTest(MessageType.RESPONSE, 1024, RouterHttpRequestStats::recordResponseSize);
  }

  private void runCallSizeMetricTest(
      MessageType messageType,
      int size,
      BiConsumer<RouterHttpRequestStats, Integer> recorder) {

    String storeName = "test-store";
    String clusterName = "test-cluster";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepositoryForRouter(RouterServer.ROUTER_SERVICE_METRIC_ENTITIES, true, inMemoryMetricReader);
    RouterHttpRequestStats routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        false,
        null);

    // Record
    recorder.accept(routerHttpRequestStats, size);

    // Validate
    validateOtelMetrics(
        inMemoryMetricReader,
        storeName,
        clusterName,
        RequestType.SINGLE_GET,
        messageType,
        "",
        (double) size,
        "call_size");
  }

  private void validateOtelMetrics(
      InMemoryMetricReader inMemoryMetricReader,
      String storeName,
      String clusterName,
      RequestType requestType,
      MessageType messageType,
      String otelPrefix,
      double expectedDataSize,
      String expectedMetricName) {
    Attributes attributes = getAttributes(storeName, clusterName, requestType, messageType);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertEquals(
        1,
        metricsData.size(),
        String.format("There should be exactly %d metric data points collected for the test case", metricsData.size()));
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedDataSize,
        expectedDataSize,
        1,
        expectedDataSize,
        attributes,
        expectedMetricName,
        otelPrefix);

  }

  private Attributes getAttributes(String storeName, String clusterName, RequestType requestType, MessageType type) {
    AttributesBuilder builder = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), requestType.name().toLowerCase())
        .put(VENICE_MESSAGE_TYPE.getDimensionNameInDefaultFormat(), type.getDimensionValue());
    return builder.build();

  }

  public static VeniceMetricsRepository getVeniceMetricsRepositoryForRouter(
      Collection<MetricEntity> metricEntities,
      boolean emitOtelMetrics,
      MetricReader additionalMetricReader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(metricEntities)
            .setEmitOtelMetrics(emitOtelMetrics)
            .setOtelAdditionalMetricsReader(additionalMetricReader)
            .build());
  }

  @Test
  public void testBodyAggregationLatencySensorRecords() {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    RouterHttpRequestStats stats = new RouterHttpRequestStats(
        metricsRepository,
        storeName,
        clusterName,
        RequestType.MULTI_GET_STREAMING,
        mock(ScatterGatherStats.class),
        false,
        null);

    for (int i = 1; i <= 100; i++) {
      stats.recordBodyAggregationLatency(i);
    }

    String prefix = "multiget_streaming_";
    double p50 =
        reporter.query("." + storeName + "--" + prefix + "body_aggregation_latency" + ".50thPercentile").value();
    double p99 =
        reporter.query("." + storeName + "--" + prefix + "body_aggregation_latency" + ".99thPercentile").value();
    assertEquals((int) p50, 50);
    assertEquals((int) p99, 99);
  }
}
