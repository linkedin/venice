package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import io.tehuti.metrics.MetricsRepository;


public class ServerNettyThreadStats {
  private static final String THREAD_POOL_NAME = "netty_worker_thread_pool";

  private final MetricEntityStateBase activeRequestThreadCount;

  public ServerNettyThreadStats(MetricsRepository metricsRepository) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setThreadPoolName(THREAD_POOL_NAME).build();
    activeRequestThreadCount = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  public void recordActiveRequestThreadCount(int count) {
    activeRequestThreadCount.record(count);
  }
}
