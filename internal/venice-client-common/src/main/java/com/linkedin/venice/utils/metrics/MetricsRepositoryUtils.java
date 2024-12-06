package com.linkedin.venice.utils.metrics;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.ArrayList;
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

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository() {
    return createSingleThreadedVeniceMetricsRepository(
        TimeUnit.MINUTES.toMillis(1),
        100,
        false,
        VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat());
  }

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository(
      boolean isOtelEnabled,
      VeniceOpenTelemetryMetricNamingFormat otelFormat) {
    return createSingleThreadedVeniceMetricsRepository(TimeUnit.MINUTES.toMillis(1), 100, isOtelEnabled, otelFormat);
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

  public static MetricsRepository createSingleThreadedMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs) {
    return new MetricsRepository(getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs));
  }

  public static MetricsRepository createSingleThreadedVeniceMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs,
      boolean isOtelEnabled,
      VeniceOpenTelemetryMetricNamingFormat otelFormat) {
    Collection<MetricEntity> metricEntities = new ArrayList<>();
    metricEntities
        .add(new MetricEntity("test_metric", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Test description"));

    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setEmitOtelMetrics(isOtelEnabled)
            .setMetricEntities(metricEntities)
            .setMetricNamingFormat(otelFormat)
            .setTehutiMetricConfig(getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs))
            .build());
  }
}
