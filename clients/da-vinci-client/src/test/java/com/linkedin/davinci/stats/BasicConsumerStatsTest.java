package com.linkedin.davinci.stats;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.CHUNKED_RECORD_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.CURRENT_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.HEART_BEAT_DELAY;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.POLL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerMetricEntity.VERSION_SWAP_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_FAIL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_SUCCESS_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MAXIMUM_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MAX_PARTITION_LAG;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.MINIMUM_CONSUMING_VERSION;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_FAIL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_SUCCESS_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_FAIL_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_SUCCESS_COUNT;
import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
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
    metricsRepository = getVeniceMetricsRepository(clientType, CONSUMER_METRIC_ENTITIES, true, inMemoryMetricReader);
    consumerStats = new BasicConsumerStats(metricsRepository, clientType.toString(), storeName);
    baseAttributes = consumerStats.getBaseAttributes();
  }

  @Test
  public void testEmitCurrentConsumingVersionMetrics() {
    double minVersion = 1.0;
    double maxVersion = 2.0;
    consumerStats.emitCurrentConsumingVersionMetrics((int) minVersion, (int) maxVersion);

    validateTehutiMetric(tehutiMetricPrefix + "--" + MINIMUM_CONSUMING_VERSION.getMetricName() + ".Gauge", minVersion);
    validateTehutiMetric(tehutiMetricPrefix + "--" + MAXIMUM_CONSUMING_VERSION.getMetricName() + ".Gauge", maxVersion);

    validateMinMaxSumAggregationsOtelMetric(
        storeName,
        CURRENT_CONSUMING_VERSION.getMetricEntity().getMetricName(),
        minVersion,
        maxVersion,
        2,
        minVersion + maxVersion,
        null);
  }

  @Test
  public void testEmitHeartBeatDelayMetrics() {
    double delay = 100;
    consumerStats.emitHeartBeatDelayMetrics((long) delay);

    validateTehutiMetric(tehutiMetricPrefix + "--" + MAX_PARTITION_LAG.getMetricName() + ".Max", delay);
    validateMinMaxSumAggregationsOtelMetric(
        storeName,
        HEART_BEAT_DELAY.getMetricEntity().getMetricName(),
        delay,
        delay,
        1,
        delay,
        null);
  }

  @Test
  public void testEmitPollSuccessCountMetrics() {
    VeniceResponseStatusCategory responseStatusCategory = SUCCESS;
    consumerStats.emitPollCountMetrics(responseStatusCategory);

    validateTehutiRateMetric(tehutiMetricPrefix + "--" + POLL_SUCCESS_COUNT.getMetricName() + ".Rate");
    validateLongCounterOtelMetric(storeName, POLL_COUNT.getMetricEntity().getMetricName(), 1, responseStatusCategory);
  }

  @Test
  public void testEmitPollFailCallCountMetrics() {
    VeniceResponseStatusCategory responseStatusCategory = FAIL;
    consumerStats.emitPollCountMetrics(responseStatusCategory);

    validateTehutiRateMetric(tehutiMetricPrefix + "--" + POLL_FAIL_COUNT.getMetricName() + ".Rate");
    validateLongCounterOtelMetric(storeName, POLL_COUNT.getMetricEntity().getMetricName(), 1, responseStatusCategory);
  }

  @Test
  public void testEmitVersionSwapCountMetrics() {
    int defaultNum = 0;
    int expectedNum = 1;

    // Default metrics
    validateTehutiMetric(tehutiMetricPrefix + "--" + VERSION_SWAP_SUCCESS_COUNT.getMetricName() + ".Total", defaultNum);
    validateTehutiMetric(tehutiMetricPrefix + "--" + VERSION_SWAP_FAIL_COUNT.getMetricName() + ".Total", defaultNum);
    validateLongCounterOtelMetric(storeName, VERSION_SWAP_COUNT.getMetricEntity().getMetricName(), defaultNum, SUCCESS);
    validateLongCounterOtelMetric(storeName, VERSION_SWAP_COUNT.getMetricEntity().getMetricName(), defaultNum, FAIL);

    consumerStats.emitVersionSwapCountMetrics(SUCCESS);
    consumerStats.emitVersionSwapCountMetrics(FAIL);

    // Success metrics
    validateTehutiMetric(
        tehutiMetricPrefix + "--" + VERSION_SWAP_SUCCESS_COUNT.getMetricName() + ".Total",
        expectedNum);
    validateLongCounterOtelMetric(
        storeName,
        VERSION_SWAP_COUNT.getMetricEntity().getMetricName(),
        expectedNum,
        SUCCESS);

    // Fail metrics
    validateTehutiMetric(tehutiMetricPrefix + "--" + VERSION_SWAP_FAIL_COUNT.getMetricName() + ".Total", expectedNum);
    validateLongCounterOtelMetric(storeName, VERSION_SWAP_COUNT.getMetricEntity().getMetricName(), expectedNum, FAIL);
  }

  @Test
  public void testEmitChunkedRecordSuccessCountMetrics() {
    VeniceResponseStatusCategory responseStatusCategory = SUCCESS;
    consumerStats.emitChunkedRecordCountMetrics(responseStatusCategory);

    validateTehutiRateMetric(tehutiMetricPrefix + "--" + CHUNKED_RECORD_SUCCESS_COUNT.getMetricName() + ".Rate");
    validateLongCounterOtelMetric(
        storeName,
        CHUNKED_RECORD_COUNT.getMetricEntity().getMetricName(),
        1,
        responseStatusCategory);
  }

  @Test
  public void testEmitChunkedRecordFailCountMetrics() {
    VeniceResponseStatusCategory responseStatusCategory = FAIL;
    consumerStats.emitChunkedRecordCountMetrics(responseStatusCategory);

    validateTehutiRateMetric(tehutiMetricPrefix + "--" + CHUNKED_RECORD_FAIL_COUNT.getMetricName() + ".Rate");
    validateLongCounterOtelMetric(
        storeName,
        CHUNKED_RECORD_COUNT.getMetricEntity().getMetricName(),
        1,
        responseStatusCategory);
  }

  private void validateTehutiMetric(String metricName, double expectedValue) {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertEquals(metrics.get(metricName).value(), expectedValue);
  }

  private void validateTehutiRateMetric(String metricName) {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.get(metricName).value() > 0);
  }

  private void validateMinMaxSumAggregationsOtelMetric(
      String storeName,
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      VeniceResponseStatusCategory responseStatusCategory) {

    Attributes expectedAttributes;

    if (responseStatusCategory == null) {
      expectedAttributes = getExpectedBaseAttributes(storeName);
    } else {
      expectedAttributes = getExpectedAttributes(storeName, responseStatusCategory);
    }

    validateHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        otelMetricPrefix);
  }

  private void validateLongCounterOtelMetric(
      String storeName,
      String metricName,
      double expectedValue,
      VeniceResponseStatusCategory responseStatusCategory) {
    Attributes expectedAttributes = getExpectedAttributes(storeName, responseStatusCategory);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        (long) expectedValue,
        expectedAttributes,
        metricName,
        otelMetricPrefix);
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

  @Test
  public void testBasicConsumerTehutiMetricNameEnum() {
    Map<BasicConsumerStats.BasicConsumerTehutiMetricName, String> expectedNames = new HashMap<>();
    expectedNames.put(BasicConsumerStats.BasicConsumerTehutiMetricName.MAX_PARTITION_LAG, "max_partition_lag");
    expectedNames.put(BasicConsumerStats.BasicConsumerTehutiMetricName.RECORDS_CONSUMED, "records_consumed");
    expectedNames
        .put(BasicConsumerStats.BasicConsumerTehutiMetricName.MINIMUM_CONSUMING_VERSION, "minimum_consuming_version");
    expectedNames
        .put(BasicConsumerStats.BasicConsumerTehutiMetricName.MAXIMUM_CONSUMING_VERSION, "maximum_consuming_version");
    expectedNames.put(BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_SUCCESS_COUNT, "poll_success_count");
    expectedNames.put(BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_FAIL_COUNT, "poll_fail_count");
    expectedNames
        .put(BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_SUCCESS_COUNT, "version_swap_success_count");
    expectedNames
        .put(BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_FAIL_COUNT, "version_swap_fail_count");
    expectedNames.put(
        BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_SUCCESS_COUNT,
        "chunked_record_success_count");
    expectedNames
        .put(BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_FAIL_COUNT, "chunked_record_fail_count");

    assertEquals(
        BasicConsumerStats.BasicConsumerTehutiMetricName.values().length,
        expectedNames.size(),
        "New BasicConsumerTehutiMetricName values were added but not included in this test");

    for (BasicConsumerStats.BasicConsumerTehutiMetricName enumValue: BasicConsumerStats.BasicConsumerTehutiMetricName
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }
}
