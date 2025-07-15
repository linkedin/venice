package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.testng.annotations.Test;


@Test
public abstract class OpenTelemetryDataPointTestUtils {
  public static LongPointData getLongPointData(Collection<MetricData> metricsData, String metricName, String prefix) {
    return metricsData.stream().filter(metricData -> {
      System.out.println(
          "HERE HERE HERE: metricName = " + metricData.getName() + ", metricName = " + "venice." + prefix + "."
              + metricName);
      return metricData.getName().equals("venice." + prefix + "." + metricName);
    }).findFirst().orElse(null).getLongSumData().getPoints().stream().findFirst().orElse(null);
  }

  public static ExponentialHistogramPointData getExponentialHistogramPointData(
      Collection<MetricData> metricsData,
      String metricName,
      String prefix) {
    return metricsData.stream()
        .filter(metricData -> metricData.getName().equals("venice." + prefix + "." + metricName))
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
        .filter(metricData -> metricData.getName().equals("venice." + prefix + "." + metricName))
        .findFirst()
        .orElse(null)
        .getHistogramData()
        .getPoints()
        .stream()
        .findFirst()
        .orElse(null);
  }

  public static void validateLongPointData(
      InMemoryMetricReader inMemoryMetricReader,
      long expectedValue,
      Attributes expectedAttributes,
      String metricName,
      String metricPrefix) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metricsData.isEmpty());

    LongPointData longPointData = getLongPointData(metricsData, metricName, metricPrefix);
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
