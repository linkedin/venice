package com.linkedin.venice.utils;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_KEY_COUNT_BUCKET;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.testng.annotations.Test;


@Test
public abstract class OpenTelemetryDataTestUtils {
  public static class OpenTelemetryAttributesBuilder {
    private String storeName;
    private RequestType requestType;
    private String clusterName;
    private String routeName;
    private HttpResponseStatusEnum httpStatus;
    private VeniceResponseStatusCategory veniceStatusCategory;
    private RequestRetryType retryType;
    private VeniceRequestKeyCountBucket keyCountBucket;

    /**
     * Set the store name dimension.
     */
    public OpenTelemetryAttributesBuilder setStoreName(String storeName) {
      this.storeName = storeName;
      return this;
    }

    /**
     * Set the request type dimension.
     */
    public OpenTelemetryAttributesBuilder setRequestType(RequestType requestType) {
      this.requestType = requestType;
      return this;
    }

    /**
     * Set the cluster name dimension.
     */
    public OpenTelemetryAttributesBuilder setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    /**
     * Set the route name dimension.
     */
    public OpenTelemetryAttributesBuilder setRouteName(String routeName) {
      this.routeName = routeName;
      return this;
    }

    /**
     * Set the http status dimension.
     */
    public OpenTelemetryAttributesBuilder setHttpStatus(HttpResponseStatusEnum httpStatus) {
      this.httpStatus = httpStatus;
      return this;
    }

    /**
     * Set the Venice response status category dimension.
     */
    public OpenTelemetryAttributesBuilder setVeniceStatusCategory(VeniceResponseStatusCategory veniceStatusCategory) {
      this.veniceStatusCategory = veniceStatusCategory;
      return this;
    }

    /**
     * Set the request retry type dimension.
     */
    public OpenTelemetryAttributesBuilder setRetryType(RequestRetryType retryType) {
      this.retryType = retryType;
      return this;
    }

    /**
     * Set the request key-count-bucket dimension (OTel-only, used on CallTime metrics).
     */
    public OpenTelemetryAttributesBuilder setKeyCountBucket(VeniceRequestKeyCountBucket keyCountBucket) {
      this.keyCountBucket = keyCountBucket;
      return this;
    }

    /**
     * Build: setup base dimensions and attributes, and determine if OTel metrics should be emitted.
     * @return OpenTelemetryMetricsSetupInfo containing this information
     */
    public Attributes build() {
      AttributesBuilder builder = Attributes.builder();

      // Add store name if provided
      if (storeName != null) {
        builder.put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName);
      }

      // Add request type if provided
      if (requestType != null) {
        builder.put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), requestType.getDimensionValue());
      }

      // Add cluster name if provided
      if (clusterName != null) {
        builder.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName);
      }

      // Add route name if provided
      if (routeName != null) {
        builder.put(VeniceMetricsDimensions.VENICE_ROUTE_NAME.getDimensionNameInDefaultFormat(), routeName);
      }

      // Add http status if provided
      if (httpStatus != null) {
        builder.put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), httpStatus.getDimensionValue());
        builder.put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            getVeniceHttpResponseStatusCodeCategory(Integer.parseInt(httpStatus.getDimensionValue()))
                .getDimensionValue());
      }

      // Add venice status category if provided
      if (veniceStatusCategory != null) {
        builder.put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            veniceStatusCategory.getDimensionValue());
      }

      // Add retry type if provided
      if (retryType != null) {
        builder.put(VENICE_REQUEST_RETRY_TYPE.getDimensionNameInDefaultFormat(), retryType.getDimensionValue());
      }

      // Add key count bucket if provided
      if (keyCountBucket != null) {
        builder
            .put(VENICE_REQUEST_KEY_COUNT_BUCKET.getDimensionNameInDefaultFormat(), keyCountBucket.getDimensionValue());
      }

      return builder.build();
    }
  }

  public static LongPointData getLongPointDataFromSum(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix,
      Attributes expectedAttributes) {
    MetricData data = metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null);
    assertNotNull(data, "MetricData should not be null");

    return data.getLongSumData()
        .getPoints()
        .stream()
        .filter(p -> p.getAttributes().equals(expectedAttributes))
        .findFirst()
        .orElse(null);
  }

  public static LongPointData getLongPointDataFromGauge(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix,
      Attributes expectedAttributes) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getLongGaugeData()
        .getPoints()
        .stream()
        .filter(p -> p.getAttributes().equals(expectedAttributes))
        .findFirst()
        .orElse(null);
  }

  public static ExponentialHistogramPointData getExponentialHistogramPointData(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix,
      Attributes expectedAttributes) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getExponentialHistogramData()
        .getPoints()
        .stream()
        .filter(p -> p.getAttributes().equals(expectedAttributes))
        .findFirst()
        .orElse(null);
  }

  public static HistogramPointData getHistogramPointData(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix,
      Attributes expectedAttributes) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getHistogramData()
        .getPoints()
        .stream()
        .filter(p -> p.getAttributes().equals(expectedAttributes))
        .findFirst()
        .orElse(null);
  }

  public static void validateLongPointDataFromCounter(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longPointData = getLongPointDataFromSum(metricsData, metricName, metricPrefix, expectedAttributes);
    assertNotNull(longPointData, "LongPointData should not be null");
    assertEquals(longPointData.getValue(), expectedValue, "LongPointData value should be " + expectedValue);
    assertEquals(longPointData.getAttributes(), expectedAttributes, "LongPointData attributes should match");
  }

  /**
   * Validate counter value is at least the expected minimum. Use this in integration tests
   * where the exact count is non-deterministic (e.g., TopicCleanupService may delete
   * additional topics beyond those the test explicitly tracks).
   */
  public static void validateLongPointDataFromCounterAtLeast(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedMinValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longPointData = getLongPointDataFromSum(metricsData, metricName, metricPrefix, expectedAttributes);
    assertNotNull(longPointData, "LongPointData should not be null");
    assertTrue(
        longPointData.getValue() >= expectedMinValue,
        "LongPointData value should be >= " + expectedMinValue + " but was " + longPointData.getValue());
    assertEquals(longPointData.getAttributes(), expectedAttributes, "LongPointData attributes should match");
  }

  public static void validateLongPointDataFromGauge(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longPointData = getLongPointDataFromGauge(metricsData, metricName, metricPrefix, expectedAttributes);
    assertNotNull(longPointData, "LongPointData should not be null");
    assertEquals(longPointData.getValue(), expectedValue, "LongPointData value should be " + expectedValue);
    assertEquals(longPointData.getAttributes(), expectedAttributes, "LongPointData attributes should match");
  }

  public static void validateDoublePointDataFromGauge(
      InMemoryMetricReader inMemoryMetricReader,
      double expectedValue,
      double tolerance,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    String fullMetricName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + metricName;
    DoublePointData doublePointData = metricsData.stream()
        .filter(metricData -> metricData.getName().equals(fullMetricName))
        .findFirst()
        .orElseThrow(() -> new AssertionError("MetricData not found for: " + fullMetricName))
        .getDoubleGaugeData()
        .getPoints()
        .stream()
        .filter(p -> p.getAttributes().equals(expectedAttributes))
        .findFirst()
        .orElse(null);
    assertNotNull(doublePointData, "DoublePointData should not be null");
    assertEquals(
        doublePointData.getValue(),
        expectedValue,
        tolerance,
        "DoublePointData value should be " + expectedValue);
    assertEquals(doublePointData.getAttributes(), expectedAttributes, "DoublePointData attributes should match");
  }

  public static void validateExponentialHistogramPointData(
      InMemoryMetricReader inMemoryMetricReader,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());
    ExponentialHistogramPointData histogramPointData =
        getExponentialHistogramPointData(metricsData, metricName, metricPrefix, expectedAttributes);

    assertNotNull(histogramPointData, "ExponentialHistogramPointData should not be null");
    assertEquals(histogramPointData.getMin(), expectedMin, "Histogram min value should be " + expectedMin);
    assertEquals(histogramPointData.getMax(), expectedMax, "Histogram max value should be " + expectedMax);
    assertEquals(histogramPointData.getCount(), expectedCount, "Histogram count should be " + expectedCount);
    assertEquals(histogramPointData.getSum(), expectedSum, "Histogram sum should be " + expectedSum);
    long expectedNumPositiveBuckets = (expectedMin > 0 || expectedMax > 0) ? expectedCount : 0;
    assertEquals(
        histogramPointData.getPositiveBuckets().getTotalCount(),
        expectedNumPositiveBuckets,
        "Histogram positive buckets total count should be " + expectedNumPositiveBuckets);
    assertEquals(histogramPointData.getAttributes(), expectedAttributes, "Histogram attributes should match");
  }

  /**
   * Validate ExponentialHistogramPointData for latency metrics where min, max, and sum are not known but are > 0.
   */
  public static void validateExponentialHistogramPointDataForLatency(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedCount,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());
    ExponentialHistogramPointData histogramPointData =
        getExponentialHistogramPointData(metricsData, metricName, metricPrefix, expectedAttributes);

    assertNotNull(histogramPointData, "ExponentialHistogramPointData should not be null");
    assertTrue(histogramPointData.getMin() > 0, "Histogram min value should be > 0");
    assertTrue(histogramPointData.getMax() > 0, "Histogram max value should be > 0");
    assertEquals(histogramPointData.getCount(), expectedCount, "Histogram count should be " + expectedCount);
    assertTrue(histogramPointData.getSum() > 0, "Histogram sum should be > 0");
    assertEquals(
        histogramPointData.getPositiveBuckets().getTotalCount(),
        expectedCount,
        "Histogram positive buckets total count should be " + expectedCount);
    assertEquals(histogramPointData.getAttributes(), expectedAttributes, "Histogram attributes should match");
  }

  /**
   * Validate ExponentialHistogramPointData has at least expectedMinCount entries with sum > 0.
   * Use this in integration tests where the exact count is non-deterministic.
   */
  public static void validateExponentialHistogramPointDataAtLeast(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedMinCount,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());
    ExponentialHistogramPointData histogramPointData =
        getExponentialHistogramPointData(metricsData, metricName, metricPrefix, expectedAttributes);

    assertNotNull(histogramPointData, "ExponentialHistogramPointData should not be null");
    assertTrue(
        histogramPointData.getCount() >= expectedMinCount,
        "Histogram count should be >= " + expectedMinCount + " but was " + histogramPointData.getCount());
    assertTrue(histogramPointData.getSum() > 0, "Histogram sum should be > 0");
    assertEquals(histogramPointData.getAttributes(), expectedAttributes, "Histogram attributes should match");
  }

  public static void validateHistogramPointData(
      InMemoryMetricReader inMemoryMetricReader,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());
    HistogramPointData histogramPointData =
        getHistogramPointData(metricsData, metricName, metricPrefix, expectedAttributes);

    assertNotNull(histogramPointData, "HistogramPointData should not be null");
    assertEquals(histogramPointData.getMin(), expectedMin, "Histogram min value should be " + expectedMin);
    assertEquals(histogramPointData.getMax(), expectedMax, "Histogram max value should be " + expectedMax);
    assertEquals(histogramPointData.getCount(), expectedCount, "Histogram count should be " + expectedCount);
    assertEquals(histogramPointData.getSum(), expectedSum, "Histogram sum should be " + expectedSum);
    assertEquals(histogramPointData.getAttributes(), expectedAttributes, "Histogram attributes should match");
  }

  /**
   * Validate observable counter value for a specific attribute combination.
   * Observable counters report as Sum data in OpenTelemetry.
   */
  public static void validateObservableCounterValue(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longPointData = getLongPointDataFromSum(metricsData, metricName, metricPrefix, expectedAttributes);
    assertNotNull(longPointData, "LongPointData for observable counter should not be null");
    assertEquals(longPointData.getValue(), expectedValue, "Observable counter value should be " + expectedValue);
    assertEquals(longPointData.getAttributes(), expectedAttributes, "Observable counter attributes should match");
  }

  /**
   * Validate that at least one Sum (counter) data point exists with value >= minValue,
   * across all attribute combinations. Use this in integration tests where the exact
   * attributes are non-deterministic (e.g., replica type depends on leader election).
   *
   * <p>Note: When validating multiple metrics, prefer collecting once via
   * {@code reader.collectAllMetrics()} and calling the {@code Collection<MetricData>}
   * overload to avoid draining async counter adders between calls.
   */
  public static void validateAnySumDataPointAtLeast(
      InMemoryMetricReader inMemoryMetricReader,
      long minValue,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    validateAnySumDataPointAtLeast(metricsData, minValue, metricName, metricPrefix);
  }

  public static void validateAnySumDataPointAtLeast(
      Collection<MetricData> metricsData,
      long minValue,
      String metricName,
      String metricPrefix) {
    assertFalse(metricsData.isEmpty());

    String fullMetricName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + metricName;
    MetricData data =
        metricsData.stream().filter(metricData -> metricData.getName().equals(fullMetricName)).findFirst().orElse(null);
    assertNotNull(data, "MetricData for " + fullMetricName + " should not be null");

    boolean found = data.getLongSumData().getPoints().stream().anyMatch(p -> p.getValue() >= minValue);
    assertTrue(found, fullMetricName + " should have at least one data point with value >= " + minValue);
  }

  /**
   * Validate that at least one Gauge data point exists with value >= minValue,
   * across all attribute combinations. Use this in integration tests where the exact
   * attributes are non-deterministic.
   *
   * <p>Note: When validating multiple metrics, prefer collecting once via
   * {@code reader.collectAllMetrics()} and calling the {@code Collection<MetricData>}
   * overload to avoid draining async counter adders between calls.
   */
  public static void validateAnyGaugeDataPointAtLeast(
      InMemoryMetricReader inMemoryMetricReader,
      long minValue,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    validateAnyGaugeDataPointAtLeast(metricsData, minValue, metricName, metricPrefix);
  }

  public static void validateAnyGaugeDataPointAtLeast(
      Collection<MetricData> metricsData,
      long minValue,
      String metricName,
      String metricPrefix) {
    assertFalse(metricsData.isEmpty());

    String fullMetricName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + metricName;
    MetricData data =
        metricsData.stream().filter(metricData -> metricData.getName().equals(fullMetricName)).findFirst().orElse(null);
    assertNotNull(data, "MetricData for " + fullMetricName + " should not be null");

    boolean found = data.getLongGaugeData().getPoints().stream().anyMatch(p -> p.getValue() >= minValue);
    assertTrue(found, fullMetricName + " should have at least one data point with value >= " + minValue);
  }

  public static void validateAnyDoubleGaugeDataPointAtLeast(
      InMemoryMetricReader inMemoryMetricReader,
      double minValue,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    String fullMetricName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + metricName;
    MetricData data =
        metricsData.stream().filter(metricData -> metricData.getName().equals(fullMetricName)).findFirst().orElse(null);
    assertNotNull(data, "MetricData for " + fullMetricName + " should not be null");

    boolean found = data.getDoubleGaugeData().getPoints().stream().anyMatch(p -> p.getValue() >= minValue);
    assertTrue(found, fullMetricName + " should have at least one double data point with value >= " + minValue);
  }

  /**
   * Assert that no Sum (counter) data point exists for the given metric name and attributes.
   * If the metric is not present at all, this passes (no data = no counter data).
   * If attributes is null, asserts that no data points exist at all for the metric.
   */
  public static void assertNoLongSumDataForAttributes(
      Collection<MetricData> metricsData,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes) {
    String fullMetricName = DEFAULT_METRIC_PREFIX + metricPrefix + "." + metricName;
    MetricData data =
        metricsData.stream().filter(metricData -> metricData.getName().equals(fullMetricName)).findFirst().orElse(null);
    if (data == null) {
      return; // metric not present at all — no data recorded
    }
    LongPointData point;
    if (expectedAttributes != null) {
      point = data.getLongSumData()
          .getPoints()
          .stream()
          .filter(p -> p.getAttributes().equals(expectedAttributes))
          .findFirst()
          .orElse(null);
    } else {
      point = data.getLongSumData().getPoints().stream().findFirst().orElse(null);
    }
    assertNull(point, "Expected no counter data for " + fullMetricName);
  }

  /**
   * Validates that a <b>monotonic</b> async counter ({@code ASYNC_COUNTER_FOR_HIGH_PERF_CASES})
   * produces correct strictly-positive deltas under DELTA temporality across multiple collection
   * intervals. All {@code valuesPerPeriod} must be strictly positive.
   *
   * <p>This is the key regression test for the {@code sumThenReset()} to {@code sum()} fix. With
   * DELTA temporality, the OTel SDK computes {@code currentObservation - lastObservation}. If the
   * callback reports deltas ({@code sumThenReset()}), the SDK computes delta-of-delta which goes
   * negative when traffic decreases. With cumulative observations ({@code sum()}), the SDK
   * correctly computes the real delta for each period.
   *
   * <p>For {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES}, use
   * {@link #validateAsyncUpDownCounterDeltaMultiCollection} instead.
   */
  public static void validateAsyncCounterDeltaMultiCollection(
      InMemoryMetricReader deltaReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod) {
    validateDeltaMultiCollection(
        deltaReader,
        metricName,
        metricPrefix,
        expectedAttributes,
        recordFn,
        valuesPerPeriod,
        true);
  }

  /**
   * Validates that an {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES} metric produces correct
   * deltas under DELTA temporality. Unlike monotonic counters, deltas can be negative.
   */
  public static void validateAsyncUpDownCounterDeltaMultiCollection(
      InMemoryMetricReader deltaReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod) {
    validateDeltaMultiCollection(
        deltaReader,
        metricName,
        metricPrefix,
        expectedAttributes,
        recordFn,
        valuesPerPeriod,
        false);
  }

  /**
   * Validates that a <b>monotonic</b> async counter ({@code ASYNC_COUNTER_FOR_HIGH_PERF_CASES})
   * produces monotonically non-decreasing cumulative values across multiple collection intervals.
   *
   * <p>For {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES}, use
   * {@link #validateAsyncUpDownCounterCumulativeMultiCollection} instead.
   */
  public static void validateAsyncCounterCumulativeMultiCollection(
      InMemoryMetricReader cumulativeReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod) {
    validateCumulativeMultiCollection(
        cumulativeReader,
        metricName,
        metricPrefix,
        expectedAttributes,
        recordFn,
        valuesPerPeriod,
        true);
  }

  /**
   * Validates that an {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES} metric produces correct
   * cumulative values. Unlike monotonic counters, the cumulative value can decrease.
   */
  public static void validateAsyncUpDownCounterCumulativeMultiCollection(
      InMemoryMetricReader cumulativeReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod) {
    validateCumulativeMultiCollection(
        cumulativeReader,
        metricName,
        metricPrefix,
        expectedAttributes,
        recordFn,
        valuesPerPeriod,
        false);
  }

  /**
   * Convenience overload for <b>monotonic</b> async counters ({@code ASYNC_COUNTER_FOR_HIGH_PERF_CASES})
   * that handles reader and repository lifecycle. Validates both DELTA and CUMULATIVE temporality.
   *
   * <p>For {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES}, use
   * {@link #validateAsyncUpDownCounterMultiCollection} instead.
   */
  public static void validateAsyncCounterMultiCollection(
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      String metricName,
      Attributes expectedAttributes,
      Function<VeniceMetricsRepository, LongConsumer> statsFactory,
      long[] valuesPerPeriod) {
    validateBothTemporalities(
        metricPrefix,
        metricEntities,
        metricName,
        expectedAttributes,
        statsFactory,
        valuesPerPeriod,
        true);
  }

  /**
   * Convenience overload for {@code ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES} that handles
   * reader and repository lifecycle. Validates both DELTA and CUMULATIVE temporality.
   * Values can be positive or negative.
   */
  public static void validateAsyncUpDownCounterMultiCollection(
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      String metricName,
      Attributes expectedAttributes,
      Function<VeniceMetricsRepository, LongConsumer> statsFactory,
      long[] valuesPerPeriod) {
    validateBothTemporalities(
        metricPrefix,
        metricEntities,
        metricName,
        expectedAttributes,
        statsFactory,
        valuesPerPeriod,
        false);
  }

  /** Validates DELTA temporality across multiple collections. When {@code monotonic}, asserts each delta is strictly positive. */
  private static void validateDeltaMultiCollection(
      InMemoryMetricReader deltaReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod,
      boolean monotonic) {
    for (int i = 0; i < valuesPerPeriod.length; i++) {
      recordFn.accept(valuesPerPeriod[i]);
      Collection<MetricData> metrics = deltaReader.collectAllMetrics();
      LongPointData point = getLongPointDataFromSum(metrics, metricName, metricPrefix, expectedAttributes);
      assertNotNull(point, "Period " + (i + 1) + " should have data for " + metricName);
      if (monotonic) {
        assertTrue(
            point.getValue() >= 0,
            "Monotonic counter delta must be non-negative in period " + (i + 1) + ", but got: " + point.getValue()
                + " for " + metricName);
      }
      assertEquals(
          point.getValue(),
          valuesPerPeriod[i],
          "Period " + (i + 1) + " delta should be " + valuesPerPeriod[i] + " for " + metricName);
    }
  }

  /** Validates CUMULATIVE temporality across multiple collections. When {@code monotonic}, asserts values are non-decreasing. */
  private static void validateCumulativeMultiCollection(
      InMemoryMetricReader cumulativeReader,
      String metricName,
      String metricPrefix,
      Attributes expectedAttributes,
      LongConsumer recordFn,
      long[] valuesPerPeriod,
      boolean monotonic) {
    long previousValue = 0;
    long expectedCumulative = 0;
    for (int i = 0; i < valuesPerPeriod.length; i++) {
      recordFn.accept(valuesPerPeriod[i]);
      expectedCumulative += valuesPerPeriod[i];
      Collection<MetricData> metrics = cumulativeReader.collectAllMetrics();
      LongPointData point = getLongPointDataFromSum(metrics, metricName, metricPrefix, expectedAttributes);
      assertNotNull(point, "Period " + (i + 1) + " should have data for " + metricName);
      if (monotonic) {
        assertTrue(
            point.getValue() >= previousValue,
            "Cumulative counter must be non-decreasing in period " + (i + 1) + ": was " + previousValue + ", now "
                + point.getValue() + " for " + metricName);
      }
      assertEquals(
          point.getValue(),
          expectedCumulative,
          "Cumulative value after period " + (i + 1) + " should be " + expectedCumulative + " for " + metricName);
      previousValue = point.getValue();
    }
  }

  /** Runs both DELTA and CUMULATIVE validation with reader/repo lifecycle management. */
  private static void validateBothTemporalities(
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      String metricName,
      Attributes expectedAttributes,
      Function<VeniceMetricsRepository, LongConsumer> statsFactory,
      long[] valuesPerPeriod,
      boolean monotonic) {
    InMemoryMetricReader deltaReader = InMemoryMetricReader.createDelta();
    VeniceMetricsRepository deltaRepo = createOtelRepo(metricPrefix, metricEntities, deltaReader);
    try {
      LongConsumer recordFn = statsFactory.apply(deltaRepo);
      validateDeltaMultiCollection(
          deltaReader,
          metricName,
          metricPrefix,
          expectedAttributes,
          recordFn,
          valuesPerPeriod,
          monotonic);
    } finally {
      deltaRepo.close();
    }

    InMemoryMetricReader cumulativeReader = InMemoryMetricReader.create();
    VeniceMetricsRepository cumulativeRepo = createOtelRepo(metricPrefix, metricEntities, cumulativeReader);
    try {
      LongConsumer recordFn = statsFactory.apply(cumulativeRepo);
      validateCumulativeMultiCollection(
          cumulativeReader,
          metricName,
          metricPrefix,
          expectedAttributes,
          recordFn,
          valuesPerPeriod,
          monotonic);
    } finally {
      cumulativeRepo.close();
    }
  }

  /** Creates a VeniceMetricsRepository with OTel enabled and the given reader. */
  private static VeniceMetricsRepository createOtelRepo(
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      InMemoryMetricReader reader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(metricPrefix)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
  }
}
