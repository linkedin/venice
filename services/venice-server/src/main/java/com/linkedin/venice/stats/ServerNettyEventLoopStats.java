package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.function.LongSupplier;


public class ServerNettyEventLoopStats extends AbstractVeniceStats {
  private volatile long pendingTasks;
  private volatile int maxPendingTasks;
  private final MetricEntityStateBase schedulingDelay;

  public ServerNettyEventLoopStats(MetricsRepository metricsRepository, int workerCount) {
    super(metricsRepository, ServerNettyThreadStats.THREAD_POOL_NAME);
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setThreadPoolName(ServerNettyThreadStats.THREAD_POOL_NAME)
            .build();
    registerGauge(
        otelData,
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT,
        "max_thread_number",
        () -> workerCount);
    registerGauge(
        otelData,
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT,
        "queued_task_count_gauge",
        () -> pendingTasks);
    registerGauge(
        otelData,
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_MAX_TASK_COUNT,
        "max_queued_task_count_gauge",
        () -> maxPendingTasks);
    schedulingDelay = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_EVENT_LOOP_SCHEDULING_DELAY.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        TehutiMetricName.EVENT_LOOP_SCHEDULING_DELAY,
        Arrays.asList(TehutiUtils.getPercentileStatWithAvgAndMax(getName(), "event_loop_scheduling_delay")),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  private void registerGauge(
      OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData,
      ThreadPoolOtelMetricEntity metric,
      String name,
      LongSupplier value) {
    registerSensor(new LambdaStat((ignored, ignored2) -> value.getAsLong(), name));
    AsyncMetricEntityStateBase.create(
        metric.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes(),
        value);
  }

  public void recordPendingTasks(long total, int max) {
    pendingTasks = total;
    maxPendingTasks = max;
  }

  public void recordSchedulingDelay(double delayMs) {
    schedulingDelay.record(delayMs);
  }

  enum TehutiMetricName implements TehutiMetricNameEnum {
    EVENT_LOOP_SCHEDULING_DELAY
  }
}
