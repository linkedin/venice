package com.linkedin.venice.utils;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.testng.annotations.Test;


@Test
public abstract class OpenTelemetryDataPointTestUtils {
  public static LongPointData getLongPointDataFromSum(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getLongSumData()
        .getPoints()
        .stream()
        .findFirst()
        .orElse(null);
  }

  public static DoublePointData getDoublePointDataFromGauge(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getDoubleGaugeData()
        .getPoints()
        .stream()
        .findFirst()
        .orElse(null);
  }

  public static ExponentialHistogramPointData getExponentialHistogramPointData(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getExponentialHistogramData()
        .getPoints()
        .stream()
        .findFirst()
        .orElse(null);
  }

  public static HistogramPointData getHistogramPointData(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals(DEFAULT_METRIC_PREFIX + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getHistogramData()
        .getPoints()
        .stream()
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

    LongPointData longPointData = getLongPointDataFromSum(metricsData, metricName, metricPrefix);
    assertNotNull(longPointData, "LongPointData should not be null");
    assertEquals(longPointData.getValue(), expectedValue, "LongPointData value should be " + expectedValue);
    assertEquals(longPointData.getAttributes(), expectedAttributes, "LongPointData attributes should match");
  }

  public static void validateDoublePointDataFromGauge(
      InMemoryMetricReader inMemoryMetricReader,
      double expectedValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    DoublePointData doublePointData = getDoublePointDataFromGauge(metricsData, metricName, metricPrefix);
    assertNotNull(doublePointData, "DoublePointData should not be null");
    assertEquals(doublePointData.getValue(), expectedValue, "DoublePointData value should be " + expectedValue);
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
        getExponentialHistogramPointData(metricsData, metricName, metricPrefix);

    assertNotNull(histogramPointData, "ExponentialHistogramPointData should not be null");
    assertEquals(histogramPointData.getMin(), expectedMin, "Histogram min value should be " + expectedMin);
    assertEquals(histogramPointData.getMax(), expectedMax, "Histogram max value should be " + expectedMax);
    assertEquals(histogramPointData.getCount(), expectedCount, "Histogram count should be " + expectedCount);
    assertEquals(histogramPointData.getSum(), expectedSum, "Histogram sum should be " + expectedSum);
    assertEquals(
        histogramPointData.getPositiveBuckets().getTotalCount(),
        expectedCount,
        "Histogram positive buckets total count should be " + expectedCount);
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
    HistogramPointData histogramPointData = getHistogramPointData(metricsData, metricName, metricPrefix);

    assertNotNull(histogramPointData, "HistogramPointData should not be null");
    assertEquals(histogramPointData.getMin(), expectedMin, "Histogram min value should be " + expectedMin);
    assertEquals(histogramPointData.getMax(), expectedMax, "Histogram max value should be " + expectedMax);
    assertEquals(histogramPointData.getCount(), expectedCount, "Histogram count should be " + expectedCount);
    assertEquals(histogramPointData.getSum(), expectedSum, "Histogram sum should be " + expectedSum);
    assertEquals(histogramPointData.getAttributes(), expectedAttributes, "Histogram attributes should match");
  }
}
