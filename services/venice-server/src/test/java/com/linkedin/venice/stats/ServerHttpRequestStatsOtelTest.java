package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_KEY_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_VALUE_SIZE;
import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ServerHttpRequestStatsOtelTest {
  private static final String METRIC_PREFIX = "server";
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final HttpResponseStatusEnum STATUS = HttpResponseStatusEnum.OK;
  private static final HttpResponseStatusCodeCategory STATUS_CATEGORY = HttpResponseStatusCodeCategory.SUCCESS;
  private static final VeniceResponseStatusCategory VENICE_STATUS = VeniceResponseStatusCategory.SUCCESS;

  @DataProvider(name = "multiKeyRequestTypes", parallel = true)
  public static Object[][] multiKeyRequestTypes() {
    return new Object[][] { { RequestType.MULTI_GET }, { RequestType.COMPUTE } };
  }

  @DataProvider(name = "noOtelExportScenarios", parallel = true)
  public static Object[][] noOtelExportScenarios() {
    return new Object[][] { { RequestType.MULTI_GET, false, STORE_NAME }, { RequestType.COMPUTE, false, STORE_NAME },
        { RequestType.MULTI_GET, true, STORE_NAME_FOR_TOTAL_STAT },
        { RequestType.COMPUTE, true, STORE_NAME_FOR_TOTAL_STAT } };
  }

  @Test(dataProvider = "multiKeyRequestTypes")
  public void testMultiKeySizeMetricsExportToOtelOnly(RequestType requestType) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repository = createRepository(true, reader)) {
      ServerHttpRequestStats stats = createStats(repository, STORE_NAME, requestType);

      recordSizes(stats);

      assertSizeOtelMetrics(reader, requestType, STORE_NAME);
      assertMultiKeyTehutiMetricsAbsent(repository, requestType, STORE_NAME);
    }
  }

  @Test
  public void testSingleGetSizeMetricsExportToOtelAndTehuti() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repository = createRepository(true, reader)) {
      ServerHttpRequestStats stats = createStats(repository, STORE_NAME, RequestType.SINGLE_GET);

      recordSizes(stats);

      assertSizeOtelMetrics(reader, RequestType.SINGLE_GET, STORE_NAME);
      assertTehutiMetricValue(repository, "request_key_size", "Avg", 15.0);
      assertTehutiMetricValue(repository, "request_key_size", "Max", 20.0);
      assertTehutiMetricValue(repository, "request_value_size", "Avg", 150.0);
      assertTehutiMetricValue(repository, "request_value_size", "Max", 200.0);
    }
  }

  @Test(dataProvider = "noOtelExportScenarios")
  public void testMultiKeySizeMetricsDoNotExportWhenOtelIsUnavailable(
      RequestType requestType,
      boolean emitOtelMetrics,
      String storeName) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository repository = createRepository(emitOtelMetrics, reader)) {
      ServerHttpRequestStats stats = createStats(repository, storeName, requestType);

      recordSizes(stats);

      assertNoSizeOtelMetrics(reader.collectAllMetrics());
      assertMultiKeyTehutiMetricsAbsent(repository, requestType, storeName);
    }
  }

  @Test(dataProvider = "multiKeyRequestTypes")
  public void testMultiKeySizeMetricsAreSafeWithPlainRepository(RequestType requestType) {
    MetricsRepository repository = new MetricsRepository();
    try {
      ServerHttpRequestStats stats = createStats(repository, STORE_NAME, requestType);

      recordSizes(stats);

      assertMultiKeyTehutiMetricsAbsent(repository, requestType, STORE_NAME);
    } finally {
      repository.close();
    }
  }

  private static VeniceMetricsRepository createRepository(boolean emitOtelMetrics, InMemoryMetricReader reader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(emitOtelMetrics)
            .setOtelAdditionalMetricsReader(reader)
            .build());
  }

  private static ServerHttpRequestStats createStats(
      MetricsRepository repository,
      String storeName,
      RequestType requestType) {
    return new ServerHttpRequestStats(repository, storeName, CLUSTER_NAME, requestType, null, false);
  }

  private static void recordSizes(ServerHttpRequestStats stats) {
    stats.recordKeySizeInByte(10);
    stats.recordKeySizeInByte(20);
    stats.recordValueSizeInByte(STATUS, STATUS_CATEGORY, VENICE_STATUS, 100);
    stats.recordValueSizeInByte(STATUS, STATUS_CATEGORY, VENICE_STATUS, 200);
  }

  private static void assertSizeOtelMetrics(InMemoryMetricReader reader, RequestType requestType, String storeName) {
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        reader,
        10,
        20,
        2,
        30,
        buildKeySizeAttributes(requestType, storeName),
        READ_REQUEST_KEY_SIZE.getMetricEntity().getMetricName(),
        METRIC_PREFIX);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        reader,
        100,
        200,
        2,
        300,
        buildValueSizeAttributes(requestType, storeName),
        READ_RESPONSE_VALUE_SIZE.getMetricEntity().getMetricName(),
        METRIC_PREFIX);
  }

  private static Attributes buildKeySizeAttributes(RequestType requestType, String storeName) {
    return new OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(CLUSTER_NAME)
        .setRequestType(requestType)
        .build();
  }

  private static Attributes buildValueSizeAttributes(RequestType requestType, String storeName) {
    return new OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(CLUSTER_NAME)
        .setRequestType(requestType)
        .setHttpStatus(STATUS)
        .setVeniceStatusCategory(VENICE_STATUS)
        .build();
  }

  private static void assertMultiKeyTehutiMetricsAbsent(
      MetricsRepository repository,
      RequestType requestType,
      String storeName) {
    for (String metricName: new String[] { "request_key_size", "request_value_size" }) {
      for (String statName: new String[] { "Avg", "Max" }) {
        assertNull(repository.getMetric(tehutiMetricName(requestType, storeName, metricName, statName)));
      }
    }
  }

  private static void assertTehutiMetricValue(
      MetricsRepository repository,
      String metricName,
      String statName,
      double expectedValue) {
    assertEquals(
        repository.getMetric(tehutiMetricName(RequestType.SINGLE_GET, STORE_NAME, metricName, statName)).value(),
        expectedValue);
  }

  private static String tehutiMetricName(
      RequestType requestType,
      String storeName,
      String metricName,
      String statName) {
    return "." + storeName + "--" + requestType.getMetricPrefix() + metricName + "." + statName;
  }

  private static void assertNoSizeOtelMetrics(Collection<MetricData> metrics) {
    assertOtelMetricAbsent(metrics, READ_REQUEST_KEY_SIZE.getMetricEntity().getMetricName());
    assertOtelMetricAbsent(metrics, READ_RESPONSE_VALUE_SIZE.getMetricEntity().getMetricName());
  }

  private static void assertOtelMetricAbsent(Collection<MetricData> metrics, String metricName) {
    String fullMetricName = DEFAULT_METRIC_PREFIX + METRIC_PREFIX + "." + metricName;
    assertFalse(metrics.stream().anyMatch(metric -> metric.getName().equals(fullMetricName)));
  }
}
