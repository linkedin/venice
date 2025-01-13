package com.linkedin.venice.utils.metrics;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.metrics.MetricEntity;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Collection;
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
    return new MetricsRepository(getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs));
  }

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository() {
    return createSingleThreadedVeniceMetricsRepository(
        TimeUnit.MINUTES.toMillis(1),
        100,
        false,
        VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat(),
        null);
  }

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository(
      boolean isOtelEnabled,
      VeniceOpenTelemetryMetricNamingFormat otelFormat,
      Collection<MetricEntity> metricEntities) {
    return createSingleThreadedVeniceMetricsRepository(
        TimeUnit.MINUTES.toMillis(1),
        100,
        isOtelEnabled,
        otelFormat,
        metricEntities);
  }

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs,
      boolean isOtelEnabled,
      VeniceOpenTelemetryMetricNamingFormat otelFormat,
      Collection<MetricEntity> metricEntities) {

    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setEmitOtelMetrics(isOtelEnabled)
            .setMetricEntities(metricEntities)
            .setMetricNamingFormat(otelFormat)
            .setTehutiMetricConfig(getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs))
            .build());
  }

  public static MetricConfig getMetricConfig(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs) {
    return new MetricConfig(
        new AsyncGauge.AsyncGaugeExecutor.Builder().setMetricMeasurementThreadCount(1)
            .setSlowMetricMeasurementThreadCount(1)
            .setInitialMetricsMeasurementTimeoutInMs(initialMetricsMeasurementTimeoutMs)
            .setMaxMetricsMeasurementTimeoutInMs(maxMetricsMeasurementTimeoutMs)
            .build());
  }
}
