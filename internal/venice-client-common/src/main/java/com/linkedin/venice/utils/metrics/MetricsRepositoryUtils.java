package com.linkedin.venice.utils.metrics;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.metrics.MetricEntity;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.utils.Time;
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

  public static MetricsRepository createSingleThreadedMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs) {
    return new MetricsRepository(getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs));
  }

  /**
   * Variant that lets the caller inject a {@link Time} into the underlying
   * {@link MetricsRepository}. Tehuti's {@code Rate} (and other sampled stats) read time on
   * every {@code measure()} call to compute the window elapsed; using a {@link Time} mock
   * makes those reads deterministic across consecutive {@code value()} calls in a test.
   */
  public static MetricsRepository createSingleThreadedMetricsRepository(Time time) {
    return createSingleThreadedMetricsRepository(TimeUnit.MINUTES.toMillis(1), 100, time);
  }

  public static MetricsRepository createSingleThreadedMetricsRepository(
      long maxMetricsMeasurementTimeoutMs,
      long initialMetricsMeasurementTimeoutMs,
      Time time) {
    /*
     * Tehuti's MetricsRepository(MetricConfig, List, Time) constructor stores the reporters list
     * by reference and later mutates it via addReporter(...). Pass a mutable ArrayList so callers
     * can register reporters; an immutable Collections.emptyList() throws UnsupportedOperationException.
     */
    return new MetricsRepository(
        getMetricConfig(maxMetricsMeasurementTimeoutMs, initialMetricsMeasurementTimeoutMs),
        new ArrayList<>(),
        time);
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

  public static MetricConfig createDefaultSingleThreadedMetricConfig() {
    return getMetricConfig(TimeUnit.MINUTES.toMillis(1), 100);
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
