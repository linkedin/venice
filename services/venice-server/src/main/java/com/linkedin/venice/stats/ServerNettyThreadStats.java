package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;


public class ServerNettyThreadStats extends AbstractVeniceStats {
  static final String THREAD_POOL_NAME = "netty_worker_thread_pool";

  private final MetricEntityStateBase activeRequestThreadCount;

  public ServerNettyThreadStats(MetricsRepository metricsRepository) {
    super(metricsRepository, THREAD_POOL_NAME);
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setThreadPoolName(THREAD_POOL_NAME).build();
    activeRequestThreadCount = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        TehutiMetricName.ACTIVE_REQUEST_THREAD_COUNT,
        Arrays.asList(new Avg(), new Max()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  public void recordActiveRequestThreadCount(int count) {
    activeRequestThreadCount.record(count);
  }

  enum TehutiMetricName implements TehutiMetricNameEnum {
    ACTIVE_REQUEST_THREAD_COUNT
  }
}
