package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThreadPoolStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_POOL_NAME = "test-pool";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ThreadPoolExecutor mockThreadPool;
  private BlockingQueue<Runnable> mockQueue;
  private ThreadPoolStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();

    Collection<MetricEntity> metricEntities =
        ModuleMetricEntityInterface.getUniqueMetricEntities(ThreadPoolOtelMetricEntity.class);

    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    mockThreadPool = Mockito.mock(ThreadPoolExecutor.class);
    mockQueue = Mockito.mock(BlockingQueue.class);
    Mockito.doReturn(mockQueue).when(mockThreadPool).getQueue();
    Mockito.doReturn(5).when(mockThreadPool).getActiveCount();
    Mockito.doReturn(10).when(mockThreadPool).getMaximumPoolSize();
    Mockito.doReturn(3).when(mockQueue).size();

    stats = new ThreadPoolStats(metricsRepository, mockThreadPool, TEST_POOL_NAME);
  }

  @Test
  public void testAsyncGaugeActiveThreadCount() {
    // Async gauges are collected during metric read
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT.getMetricEntity().getMetricName(), 5);
    // Tehuti SyncGauge should also report the same value
    validateTehutiMetric("active_thread_number", "Gauge", 5.0);
  }

  @Test
  public void testAsyncGaugeMaxThreadCount() {
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT.getMetricEntity().getMetricName(), 10);
    validateTehutiMetric("max_thread_number", "Gauge", 10.0);
  }

  @Test
  public void testAsyncGaugeQueueTaskCount() {
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT.getMetricEntity().getMetricName(), 3);
    validateTehutiMetric("queued_task_count_gauge", "Gauge", 3.0);
  }

  @Test
  public void testAsyncGaugeReflectsChangedThreadPoolState() {
    // Verify initial values
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT.getMetricEntity().getMetricName(), 5);
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT.getMetricEntity().getMetricName(), 10);
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT.getMetricEntity().getMetricName(), 3);

    // Change mock values after construction
    Mockito.doReturn(8).when(mockThreadPool).getActiveCount();
    Mockito.doReturn(20).when(mockThreadPool).getMaximumPoolSize();
    Mockito.doReturn(12).when(mockQueue).size();

    // Async gauges should pick up the new values dynamically
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT.getMetricEntity().getMetricName(), 8);
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT.getMetricEntity().getMetricName(), 20);
    validateAsyncGauge(ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT.getMetricEntity().getMetricName(), 12);
  }

  @Test
  public void testRecordQueuedTasksCount() {
    Mockito.doReturn(7).when(mockQueue).size();
    stats.recordQueuedTasksCount();

    // OTel histogram
    validateHistogram(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_DISTRIBUTION.getMetricEntity().getMetricName(),
        7.0,
        7.0,
        1,
        7.0,
        threadPoolAttributes());

    // Tehuti
    validateTehutiMetric("queued_task_count", "Avg", 7.0);
    validateTehutiMetric("queued_task_count", "Max", 7.0);
  }

  @Test
  public void testRecordMultipleQueuedTasksCounts() {
    Mockito.doReturn(5).when(mockQueue).size();
    stats.recordQueuedTasksCount();
    Mockito.doReturn(15).when(mockQueue).size();
    stats.recordQueuedTasksCount();

    // OTel histogram: min=5, max=15, count=2, sum=20
    validateHistogram(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_DISTRIBUTION.getMetricEntity().getMetricName(),
        5.0,
        15.0,
        2,
        20.0,
        threadPoolAttributes());
  }

  @Test
  public void testBlankThreadPoolNameSanitizedToUnknown() {
    verifyThreadPoolNameDimension("   ", "unknown");
  }

  @Test
  public void testThreadPoolNameWithWhitespaceIsTrimmed() {
    verifyThreadPoolNameDimension(" my-pool ", "my-pool");
  }

  private void verifyThreadPoolNameDimension(String inputName, String expectedDimensionValue) {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    Collection<MetricEntity> metricEntities =
        ModuleMetricEntityInterface.getUniqueMetricEntities(ThreadPoolOtelMetricEntity.class);
    VeniceMetricsRepository repo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());

    ThreadPoolExecutor pool = Mockito.mock(ThreadPoolExecutor.class);
    BlockingQueue<Runnable> queue = Mockito.mock(BlockingQueue.class);
    Mockito.doReturn(queue).when(pool).getQueue();
    Mockito.doReturn(2).when(pool).getActiveCount();
    Mockito.doReturn(0).when(queue).size();

    new ThreadPoolStats(repo, pool, inputName);

    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), expectedDimensionValue)
        .build();
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        reader,
        2,
        expectedAttributes,
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    verifyNoNpeWithRepository(disabledRepo, "disabled-pool");
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    verifyNoNpeWithRepository(new MetricsRepository(), "plain-pool");
  }

  @Test
  public void testThreadPoolTehutiMetricNameEnum() {
    Map<ThreadPoolStats.ThreadPoolTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.QUEUED_TASK_COUNT, "queued_task_count");

    assertEquals(
        ThreadPoolStats.ThreadPoolTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New ThreadPoolTehutiMetricNameEnum values were added but not included in this test");

    for (ThreadPoolStats.ThreadPoolTehutiMetricNameEnum enumValue: ThreadPoolStats.ThreadPoolTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  /**
   * Validates all {@link ThreadPoolOtelMetricEntity} entries match expected definitions.
   * Registration in SERVICE_METRIC_ENTITIES is verified by RouterMetricEntityTest and ServerMetricEntityTest.
   */
  @Test
  public void testThreadPoolOtelMetricEntity() {
    Map<ThreadPoolOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_COUNT,
        new MetricEntity(
            "thread_pool.thread.active_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Active threads in the thread pool",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    expectedMetrics.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_MAX_COUNT,
        new MetricEntity(
            "thread_pool.thread.max_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Maximum thread pool size",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    expectedMetrics.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_COUNT,
        new MetricEntity(
            "thread_pool.queue.task_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Tasks currently queued in the thread pool",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));
    expectedMetrics.put(
        ThreadPoolOtelMetricEntity.THREAD_POOL_QUEUE_TASK_DISTRIBUTION,
        new MetricEntity(
            "thread_pool.queue.task_distribution",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Distribution of queued task count over time",
            Collections.singleton(VENICE_THREAD_POOL_NAME)));

    assertEquals(
        ThreadPoolOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New ThreadPoolOtelMetricEntity values were added but not included in this test");

    for (ThreadPoolOtelMetricEntity metric: ThreadPoolOtelMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }
  }

  private Attributes threadPoolAttributes() {
    return Attributes.builder().put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), TEST_POOL_NAME).build();
  }

  private void validateAsyncGauge(String metricName, long expectedValue) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        threadPoolAttributes(),
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateHistogram(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateTehutiMetric(String sensorName, String statSuffix, double expectedValue) {
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName("." + TEST_POOL_NAME, sensorName) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }

  private void verifyNoNpeWithRepository(MetricsRepository repo, String poolName) {
    ThreadPoolExecutor pool = Mockito.mock(ThreadPoolExecutor.class);
    BlockingQueue<Runnable> queue = Mockito.mock(BlockingQueue.class);
    Mockito.doReturn(queue).when(pool).getQueue();
    Mockito.doReturn(0).when(queue).size();

    ThreadPoolStats localStats = new ThreadPoolStats(repo, pool, poolName);
    localStats.recordQueuedTasksCount();
  }
}
