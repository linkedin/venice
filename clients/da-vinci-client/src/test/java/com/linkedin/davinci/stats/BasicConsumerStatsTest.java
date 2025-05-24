package com.linkedin.davinci.stats;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.CHUNKED_RECORD_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.CURRENT_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.HEART_BEAT_DELAY;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.POLL_CALL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.VERSION_SWAP_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_FAIL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_SUCCESS_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MAXIMUM_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MAX_PARTITION_LAG;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MINIMUM_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_FAIL_CALL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_SUCCESS_CALL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_FAIL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_SUCCESS_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.getHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.getLongPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointData;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BasicConsumerStatsTest {
  BasicConsumerStats consumerStats;
  MetricsRepository metricsRepository;
  InMemoryMetricReader inMemoryMetricReader;
  String tehutiMetricPrefix;
  String otelMetricPrefix;
  Attributes baseAttributes;
  String storeName;

  @BeforeMethod
  public void setUp() {
    ClientType clientType = CHANGE_DATA_CAPTURE_CLIENT;
    tehutiMetricPrefix = "." + clientType;
    otelMetricPrefix = CHANGE_DATA_CAPTURE_CLIENT.getMetricsPrefix();
    storeName = "test_store";
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = getVeniceMetricsRepository(clientType, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    consumerStats = new BasicConsumerStats(metricsRepository, clientType.toString(), storeName);
    baseAttributes = consumerStats.getBaseAttributes();
  }

  @Test
  public void testEmitCurrentConsumingVersionMetrics() {
    double minVersion = 1.0;
    double maxVersion = 2.0;

    consumerStats.emitCurrentConsumingVersionMetrics((int) minVersion, (int) maxVersion);

    validateTehutiMetrics(tehutiMetricPrefix + "--" + MINIMUM_CONSUMING_VERSION.getMetricName() + ".Gauge", minVersion);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + MAXIMUM_CONSUMING_VERSION.getMetricName() + ".Gauge", maxVersion);

    validateMinMaxSumAggregationsOtelMetrics(
        storeName,
        CURRENT_CONSUMING_VERSION.getMetricEntity().getMetricName(),
        minVersion,
        maxVersion,
        2,
        minVersion + maxVersion);
  }

  @Test
  public void testEmitHeartBeatDelayMetrics() {
    double delay = 100;
    consumerStats.emitHeartBeatDelayMetrics((long) delay);

    validateTehutiMetrics(tehutiMetricPrefix + "--" + MAX_PARTITION_LAG.getMetricName() + ".Max", delay);

    validateMinMaxSumAggregationsOtelMetrics(
        storeName,
        HEART_BEAT_DELAY.getMetricEntity().getMetricName(),
        delay,
        delay,
        1,
        delay);
  }

  @Test
  public void testEmitPollSuccessCallCountMetrics() {
    consumerStats.emitPollCallCountMetrics(SUCCESS);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + POLL_SUCCESS_CALL_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, POLL_CALL_COUNT.getMetricEntity().getMetricName(), 1, SUCCESS);
  }

  @Test
  public void testEmitPollFailCallCountMetrics() {
    consumerStats.emitPollCallCountMetrics(FAIL);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + POLL_FAIL_CALL_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, POLL_CALL_COUNT.getMetricEntity().getMetricName(), 1, FAIL);
  }

  @Test
  public void testEmitVersionSwapSuccessCountMetrics() {
    consumerStats.emitVersionSwapCountMetrics(SUCCESS);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + VERSION_SWAP_SUCCESS_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, VERSION_SWAP_COUNT.getMetricEntity().getMetricName(), 1, SUCCESS);
  }

  @Test
  public void testEmitVersionSwapFailCountMetrics() {
    consumerStats.emitVersionSwapCountMetrics(FAIL);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + VERSION_SWAP_FAIL_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, VERSION_SWAP_COUNT.getMetricEntity().getMetricName(), 1, FAIL);
  }

  @Test
  public void testEmitChunkedRecordSuccessCountMetrics() {
    consumerStats.emitChunkedRecordCountMetrics(SUCCESS);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + CHUNKED_RECORD_SUCCESS_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, CHUNKED_RECORD_COUNT.getMetricEntity().getMetricName(), 1, SUCCESS);
  }

  @Test
  public void testEmitChunkedRecordFailCountMetrics() {
    consumerStats.emitChunkedRecordCountMetrics(FAIL);
    validateTehutiMetrics(tehutiMetricPrefix + "--" + CHUNKED_RECORD_FAIL_COUNT.getMetricName() + ".Avg", 1);
    validateLongCounterOtelMetrics(storeName, CHUNKED_RECORD_COUNT.getMetricEntity().getMetricName(), 1, FAIL);
  }

  private void validateTehutiMetrics(String metricName, double expectedValue) {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertEquals(metrics.get(metricName).value(), expectedValue);
  }

  private void validateMinMaxSumAggregationsOtelMetrics(
      String storeName,
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum) {

    Attributes expectedAttributes = getExpectedBaseAttributes(storeName);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    HistogramPointData minMaxCountSumPointData = getHistogramPointData(metricsData, metricName, otelMetricPrefix);

    validateHistogramPointData(
        minMaxCountSumPointData,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes);
  }

  private void validateLongCounterOtelMetrics(
      String storeName,
      String metricName,
      double expectedValue,
      VeniceResponseStatusCategory responseStatusCategory) {

    Attributes expectedAttributes = getExpectedAttributes(storeName, responseStatusCategory);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longCounterData = getLongPointData(metricsData, metricName, otelMetricPrefix);
    validateLongPointData(longCounterData, (long) expectedValue, expectedAttributes);
  }

  private Attributes getExpectedBaseAttributes(String storeName) {
    AttributesBuilder builder =
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName);

    return builder.build();
  }

  private Attributes getExpectedAttributes(String storeName, VeniceResponseStatusCategory responseStatusCategory) {
    AttributesBuilder builder = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            responseStatusCategory.getDimensionValue());

    return builder.build();
  }
}
