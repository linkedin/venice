package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ThreadPoolOtelMetricEntityTest {
  private static Map<ThreadPoolOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ThreadPoolOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT,
        new MetricEntityExpectation(
            "thread_pool.thread.active_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Active threads in the thread pool",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_DISTRIBUTION,
        new MetricEntityExpectation(
            "thread_pool.thread.active_distribution",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Distribution of active thread count sampled on request arrival",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT,
        new MetricEntityExpectation(
            "thread_pool.thread.max_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Maximum thread pool size",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT,
        new MetricEntityExpectation(
            "thread_pool.queue.task_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Tasks currently queued in the thread pool",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_DISTRIBUTION,
        new MetricEntityExpectation(
            "thread_pool.queue.task_distribution",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Distribution of queued task count over time",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_CAPACITY,
        new MetricEntityExpectation(
            "thread_pool.queue.capacity",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Approximate queue capacity from queued tasks and remaining capacity",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_REJECTED_COUNT,
        new MetricEntityExpectation(
            "thread_pool.task.rejected_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Tasks rejected from worker execution, including caller-run and shutdown rejections",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_CALLER_RUN_COUNT,
        new MetricEntityExpectation(
            "thread_pool.task.caller_run_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Tasks executed on the submitting thread by the caller-runs policy",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    map.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_TASK_CALLER_RUN_TIME,
        new MetricEntityExpectation(
            "thread_pool.task.caller_run_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Time spent executing tasks on the submitting thread",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ThreadPoolOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
