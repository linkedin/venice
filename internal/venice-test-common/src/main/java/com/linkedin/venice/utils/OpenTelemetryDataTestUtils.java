package com.linkedin.venice.utils;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
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
}
