package com.linkedin.venice.tehuti;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAccumulator;


/**
 * Utility functions to help test metrics
 */
public class MetricsUtils {
  public static double getMax(String metricName, List<? extends MetricsAware> metricsAwareWrapperList) {
    return getMetricValue(metricName, metricsAwareWrapperList, new DoubleAccumulator(Math::max, Double.MIN_VALUE));
  }

  public static double getMin(String metricName, List<? extends MetricsAware> metricsAwareWrapperList) {
    return getMetricValue(metricName, metricsAwareWrapperList, new DoubleAccumulator(Math::min, Double.MAX_VALUE));
  }

  public static double getSum(String metricName, List<? extends MetricsAware> metricsAwareWrapperList) {
    return getMetricValue(metricName, metricsAwareWrapperList, new DoubleAccumulator(Double::sum, 0.0));
  }

  public static double getAvg(String metricName, List<? extends MetricsAware> metricsAwareWrapperList) {
    AtomicInteger count = new AtomicInteger(0);
    return getMetricValue(metricName, metricsAwareWrapperList, new DoubleAccumulator((left, right) -> {
      int previousCount = count.getAndIncrement();
      return (left * previousCount + right) / (previousCount + 1);
    }, 0.0));
  }

  public static List<Double> getAvgMax(String baseMetricName, List<? extends MetricsAware> metricsAwareWrapperList) {
    return Arrays.asList(
        getAvg(baseMetricName + ".Avg", metricsAwareWrapperList),
        getMax(baseMetricName + ".Max", metricsAwareWrapperList));
  }

  public static void validateMetricRange(List<Double> metricValues, double min, double max) throws Exception {
    for (double value: metricValues) {
      if (value < min || value > max) {
        throw new Exception(String.format("Metric value %f is not in range [%f, %f]", value, min, max));
      }
    }
  }

  public static double getMetricValue(
      String metricName,
      List<? extends MetricsAware> metricsAwareWrapperList,
      DoubleAccumulator accumulator) {
    for (MetricsAware metricsAware: metricsAwareWrapperList) {
      MetricsRepository metricsRepository = metricsAware.getMetricsRepository();
      Map<String, ? extends Metric> metrics = metricsRepository.metrics();
      if (metrics.containsKey(metricName)) {
        accumulator.accumulate(metrics.get(metricName).value());
      }
    }
    return accumulator.doubleValue();
  }
}
