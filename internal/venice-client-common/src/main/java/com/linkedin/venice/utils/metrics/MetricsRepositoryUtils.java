package com.linkedin.venice.utils.metrics;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.TimeUnit;


/**
 * Utility functions to help create common metrics repository.
 */
public class MetricsRepositoryUtils {
  public static MetricsRepository createMultiThreadedMetricsRepository() {
    return new MetricsRepository(
        new MetricConfig(
            new AsyncGauge.AsyncGaugeExecutor.Builder().setInitialMetricsMeasurementTimeoutInMs(100).build()));
  }

  public static MetricsRepository createSingleThreadedMetricsRepository() {
    return createSingleThreadedMetricsRepository(TimeUnit.MINUTES.toMillis(1), 100);
  }

  public static MetricsRepository createSingleThreadedMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs) {
    return new MetricsRepository(
        new MetricConfig(
            new AsyncGauge.AsyncGaugeExecutor.Builder().setMetricMeasurementThreadCount(1)
                .setSlowMetricMeasurementThreadCount(1)
                .setInitialMetricsMeasurementTimeoutInMs(initialMetricsMeasurementTimeoutMs)
                .setMaxMetricsMeasurementTimeoutInMs(maxMetricsMeasurementTimeoutMs)
                .build()));
  }
}
