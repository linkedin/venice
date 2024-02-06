package com.linkedin.venice.utils.metrics;

import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.AsyncGaugeConfig;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Utility functions to help create common metrics repository.
 */
public class MetricsRepositoryUtils {
  public static MetricsRepository createMultiThreadedMetricsRepository(String threadName) {
    return new MetricsRepository(
        new MetricConfig(
            new AsyncGaugeConfig(
                Executors.newFixedThreadPool(10, new DaemonThreadFactory(threadName)),
                TimeUnit.MINUTES.toMillis(1),
                100)));
  }

  public static MetricsRepository createSingleThreadedMetricsRepository(String threadName) {
    return createSingleThreadedMetricsRepository(threadName, TimeUnit.MINUTES.toMillis(1), 100);
  }

  public static MetricsRepository createSingleThreadedMetricsRepository(
      String threadName,
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs) {
    return new MetricsRepository(
        new MetricConfig(
            new AsyncGaugeConfig(
                Executors.newSingleThreadExecutor(new DaemonThreadFactory(threadName)),
                maxMetricsMeasurementTimeoutMs,
                initialMetricsMeasurementTimeoutMs)));
  }
}
