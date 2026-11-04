package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Collections;
import java.util.List;
import java.util.Set;


/**
 * OTel metric entity definitions for thread pool monitoring. Empty explicit advice opts time-sampled
 * metrics out of exponential views; the listener registers capacity-based advice before instrument creation.
 */
public enum ThreadPoolOtelMetricEntity implements ModuleMetricEntityInterface {
  THREAD_POOL_THREAD_ACTIVE_COUNT(
      "thread_pool.thread.active_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER, "Active threads in the thread pool",
      Collections.singleton(VENICE_THREAD_POOL_NAME)
  ),
  THREAD_POOL_THREAD_ACTIVE_DISTRIBUTION(
      "thread_pool.thread.active_distribution", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Distribution of active thread count sampled on request arrival", Collections.singleton(VENICE_THREAD_POOL_NAME)
  ),
  THREAD_POOL_THREAD_MAX_COUNT(
      "thread_pool.thread.max_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER, "Maximum thread pool size",
      Collections.singleton(VENICE_THREAD_POOL_NAME)
  ),
  THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION(
      "thread_pool.thread.active_request_distribution", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Request-sampled number of threads executing the synchronous HTTP request pipeline",
      Collections.singleton(VENICE_THREAD_POOL_NAME)
  ),
  THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION(
      "thread_pool.thread.active_time_distribution", MetricType.HISTOGRAM, MetricUnit.NUMBER,
      "Fixed-interval samples of active read or compute threads, including idle periods",
      Collections.singleton(VENICE_THREAD_POOL_NAME), Collections.emptyList()
  ),
  THREAD_POOL_THREAD_ACTIVE_REQUEST_TIME_DISTRIBUTION(
      "thread_pool.thread.active_request_time_distribution", MetricType.HISTOGRAM, MetricUnit.NUMBER,
      "Fixed-interval samples of threads executing the synchronous HTTP pipeline, including idle periods",
      Collections.singleton(VENICE_THREAD_POOL_NAME), Collections.emptyList()
  ),
  THREAD_POOL_QUEUE_TASK_COUNT(
      "thread_pool.queue.task_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Tasks currently queued in the thread pool", Collections.singleton(VENICE_THREAD_POOL_NAME)
  ),
  THREAD_POOL_QUEUE_TASK_DISTRIBUTION(
      "thread_pool.queue.task_distribution", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Distribution of queued task count over time", Collections.singleton(VENICE_THREAD_POOL_NAME)
  );

  private final MetricEntity metricEntity;

  ThreadPoolOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this(metricName, metricType, unit, description, dimensionsList, null);
  }

  ThreadPoolOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList,
      List<Double> boundaries) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList, boundaries);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
