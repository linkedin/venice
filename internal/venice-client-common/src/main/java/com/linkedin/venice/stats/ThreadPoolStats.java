package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Stats used to collect the usage of a thread pool including: 1. active thread number, 2. max thread number and 3.
 * queued task number.
 */
public class ThreadPoolStats extends AbstractVeniceStats {
  private final ThreadPoolExecutor threadPoolExecutor;

  private final MetricEntityStateBase activeThreadsCountMetric;
  private final MetricEntityStateBase queuedTasksCountMetric;
  private final MetricEntityStateBase rejectedTasksCountMetric;
  private final MetricEntityStateBase callerRunsCountMetric;
  private final MetricEntityStateBase callerRunsTimeMetric;

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
    registerSensor(new LambdaStat((ignored, ignored2) -> getQueueCapacity(), "queue_capacity"));

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
    AsyncMetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_CAPACITY.getMetricEntity(),
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes(),
        this::getQueueCapacity);

    activeThreadsCountMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_DISTRIBUTION.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.ACTIVE_THREAD_COUNT,
        Arrays.asList(new Avg(), new Max()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());

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
    rejectedTasksCountMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_REJECTED_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.REJECTED_TASK_COUNT,
        Collections.singletonList(new Count()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
    callerRunsCountMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_CALLER_RUN_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.CALLER_RUN_COUNT,
        Collections.singletonList(new Count()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
    callerRunsTimeMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_CALLER_RUN_TIME.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.CALLER_RUN_TIME,
        Arrays.asList(new Avg(), new Max()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  private long getQueueCapacity() {
    return (long) threadPoolExecutor.getQueue().size() + threadPoolExecutor.getQueue().remainingCapacity();
  }

  /**
   * Records the current queue size as a distribution data point for the task distribution metric.
   * Callers should invoke this on each task submission to capture avg/max queue depth
   * within the metric reporting window.
   */
  public void recordQueuedTasksCount() {
    queuedTasksCountMetric.record(this.threadPoolExecutor.getQueue().size());
  }

  public void recordRejectedTask() {
    rejectedTasksCountMetric.record(1);
  }

  public void recordCallerRun() {
    callerRunsCountMetric.record(1);
  }

  public void recordCallerRunTime(double elapsedTimeMs) {
    callerRunsTimeMetric.record(elapsedTimeMs);
  }

  /**
   * Samples active threads on request arrival to capture avg/max usage between metric collections.
   * The executor's active count is approximate and request-triggered samples may miss peaks.
   */
  public void recordActiveThreadsCount() {
    activeThreadsCountMetric.record(this.threadPoolExecutor.getActiveCount());
  }

  enum ThreadPoolTehutiMetricNameEnum implements TehutiMetricNameEnum {
    ACTIVE_THREAD_COUNT, QUEUED_TASK_COUNT, REJECTED_TASK_COUNT, CALLER_RUN_COUNT, CALLER_RUN_TIME
  }
}
