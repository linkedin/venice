package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Stats used to collect the usage of a thread pool including: 1. active thread number, 2. max thread number and 3.
 * queued task number.
 */
public class ThreadPoolStats extends AbstractVeniceStats {
  private final ThreadPoolExecutor threadPoolExecutor;

  private final MetricEntityStateBase queuedTasksCountMetric;

  public ThreadPoolStats(MetricsRepository metricsRepository, ThreadPoolExecutor threadPoolExecutor, String name) {
    super(metricsRepository, name);
    this.threadPoolExecutor = threadPoolExecutor;

    // Tehuti LambdaStat registrations (async gauges for Tehuti)
    registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getActiveCount(), "active_thread_number"));
    registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getMaximumPoolSize(), "max_thread_number"));
    registerSensor(
        new LambdaStat((ignored, ignored2) -> this.threadPoolExecutor.getQueue().size(), "queued_task_count_gauge"));

    // OTel setup
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setThreadPoolName(name).build();

    // OTel async gauges for thread pool metrics
    AsyncMetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes(),
        () -> this.threadPoolExecutor.getActiveCount());

    AsyncMetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes(),
        () -> this.threadPoolExecutor.getMaximumPoolSize());

    AsyncMetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes(),
        () -> this.threadPoolExecutor.getQueue().size());

    /**
     * If only registered as Gauge, the metric would show the queue size at the time of the metric collection, which is not
     * very useful. It can provide a better view of the queue size if we record the average and max queue size within
     * the metric reporting time window which is usually 1 minute.
     * As a result, we need the users of the thread pool to explicitly call the record function to record the queue size
     * during each new task submission.
     */
    queuedTasksCountMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_DISTRIBUTION.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.QUEUED_TASK_COUNT,
        Arrays.asList(new Avg(), new Max()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  public void recordQueuedTasksCount(int queuedTasksNumber) {
    // TODO: remove the argument in the function; you don't need it
    queuedTasksCountMetric.record(this.threadPoolExecutor.getQueue().size());
  }

  enum ThreadPoolTehutiMetricNameEnum implements TehutiMetricNameEnum {
    QUEUED_TASK_COUNT
  }
}
