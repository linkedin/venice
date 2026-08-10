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
 * Stats used to collect the usage of a thread pool including: 1. active thread number, 2. max thread number, 3.
 * queued task number and 4. a request-triggered avg/max distribution of active thread count and queued task count.
 */
public class ThreadPoolStats extends AbstractVeniceStats {
  private final ThreadPoolExecutor threadPoolExecutor;

  private final MetricEntityStateBase queuedTasksCountMetric;

  private final MetricEntityStateBase activeThreadCountMetric;

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

    /**
     * The periodic async gauge above reports the active thread count only at metric collection time, which can
     * miss short-lived bursts of activity between collection intervals. To get a better signal on utilization, we
     * additionally allow callers to explicitly record the active thread count whenever a new request is submitted
     * to the thread pool. Recording on every request submission (rather than relying purely on the collection-time
     * gauge) gives us more data points to compute avg/max active thread count within the metric reporting window.
     */
    activeThreadCountMetric = MetricEntityStateBase.create(
        ThreadPoolOtelMetricEntity.THREAD_POOL_ACTIVE_THREAD_DISTRIBUTION.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        ThreadPoolTehutiMetricNameEnum.ACTIVE_THREAD_COUNT,
        Arrays.asList(new Avg(), new Max()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  /**
   * Records the current queue size as a distribution data point for the task distribution metric.
   * Callers should invoke this on each task submission to capture avg/max queue depth
   * within the metric reporting window.
   */
  public void recordQueuedTasksCount() {
    queuedTasksCountMetric.record(this.threadPoolExecutor.getQueue().size());
  }

  /**
   * Records the current active thread count as a distribution data point for the active thread count metric.
   * Callers should invoke this once per incoming request, at the point where the request's work is submitted to
   * this thread pool, to capture avg/max active thread utilization within the metric reporting window. This is a
   * best-effort, request-triggered sample: it does not rely on periodic metric collection, so it can surface
   * utilization spikes that a collection-time gauge would otherwise miss, at the cost of not being a fully
   * accurate point-in-time measurement.
   */
  public void recordActiveThreadCount() {
    activeThreadCountMetric.record(this.threadPoolExecutor.getActiveCount());
  }

  enum ThreadPoolTehutiMetricNameEnum implements TehutiMetricNameEnum {
    QUEUED_TASK_COUNT, ACTIVE_THREAD_COUNT
  }
}
