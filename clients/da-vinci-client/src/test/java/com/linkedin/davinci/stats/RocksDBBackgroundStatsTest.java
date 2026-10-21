package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_COMPLETED_COUNT;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_INPUT_BYTES;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_OUTPUT_BYTES;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_POOL_CAPACITY;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_POOL_QUEUE_DEPTH;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_RUNNING_JOBS;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.COMPACTION_TIME;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_COMPLETED_COUNT;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_POOL_CAPACITY;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_POOL_QUEUE_DEPTH;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_RAW_BYTES;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_RUNNING_JOBS;
import static com.linkedin.davinci.stats.RocksDBBackgroundOtelMetricEntity.FLUSH_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.Env;
import org.rocksdb.Priority;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RocksDBBackgroundStatsTest {
  private InMemoryMetricReader reader;
  private VeniceMetricsRepository repository;
  private final Attributes attributes =
      Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), "test-cluster").build();

  @BeforeMethod
  public void setUp() {
    reader = InMemoryMetricReader.create();
    repository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
  }

  @AfterMethod
  public void tearDown() {
    repository.close();
  }

  @Test
  public void testEnvironmentGaugesRegisteredOnceAndReadNativeValues() {
    Env env = mock(Env.class);
    when(env.getBackgroundThreads(Priority.HIGH)).thenReturn(3);
    when(env.getBackgroundThreads(Priority.LOW)).thenReturn(7);
    when(env.getThreadPoolQueueLen(Priority.HIGH)).thenReturn(11);
    when(env.getThreadPoolQueueLen(Priority.LOW)).thenReturn(13);
    RocksDBMemoryStats memory = new RocksDBMemoryStats(repository, "rocksdb", false, "test-cluster");
    RocksDBBackgroundStats stats = memory.getOrCreateBackgroundStats(env);
    assertSame(memory.getOrCreateBackgroundStats(env), stats);
    gauge(FLUSH_POOL_CAPACITY, 3);
    gauge(COMPACTION_POOL_CAPACITY, 7);
    gauge(FLUSH_POOL_QUEUE_DEPTH, 11);
    gauge(COMPACTION_POOL_QUEUE_DEPTH, 13);
    when(env.getBackgroundThreads(Priority.HIGH)).thenReturn(5);
    gauge(FLUSH_POOL_CAPACITY, 5);
    // Existing running-count gauges drive empty-state distribution samples without a scheduler.
    reader.collectAllMetrics();
    for (RocksDBBackgroundOtelMetricEntity entity: new RocksDBBackgroundOtelMetricEntity[] { FLUSH_RUNNING_JOBS,
        COMPACTION_RUNNING_JOBS }) {
      HistogramPointData point = OpenTelemetryDataTestUtils
          .getHistogramPointData(reader.collectAllMetrics(), entity.getMetricName(), "server", attributes);
      assertNotNull(point);
      assertTrue(point.getCount() > 0);
      assertEquals(point.getSum(), 0.0);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsRegisteringDifferentEnvironment() {
    RocksDBMemoryStats memory = new RocksDBMemoryStats(repository, "rocksdb", false, "test-cluster");
    memory.getOrCreateBackgroundStats(mock(Env.class));
    memory.getOrCreateBackgroundStats(mock(Env.class));
  }

  @Test
  public void testRunningJobTransitionsAreBoundedCollectionSamples() {
    AtomicLong clock = new AtomicLong(0);
    RocksDBBackgroundStats stats =
        new RocksDBBackgroundStats(repository, "rocksdb", "test-cluster", mock(Env.class), clock::get);
    stats.sampleRunningJobs(true, 0);
    stats.sampleRunningJobs(true, 99);
    stats.sampleRunningJobs(false, 2);
    clock.set(1_000_000_000L);
    stats.sampleRunningJobs(true, 3);
    stats.sampleRunningJobs(false, 4);
    clock.set(2_000_000_000L);
    stats.sampleRunningJobs(true, 0);
    histogram(FLUSH_RUNNING_JOBS, 0, 3, 3, 3);
    histogram(COMPACTION_RUNNING_JOBS, 2, 4, 2, 6);
    assertEquals(tehuti(FLUSH_RUNNING_JOBS, "Avg"), 1.0);
    assertEquals(tehuti(FLUSH_RUNNING_JOBS, "Max"), 3.0);
  }

  @DataProvider(name = "propertyReadFailure")
  public Object[][] propertyReadFailure() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "propertyReadFailure")
  public void testPropertyCollectionSamplesAggregatedJobsButNotPartialReads(boolean failure) {
    InMemoryMetricReader propertyReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository propertyRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .emitTehutiMetrics(false)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(propertyReader)
            .build())) {
      RocksDBMemoryStats memory = new RocksDBMemoryStats(propertyRepository, "rocksdb", false, "test-cluster");
      RocksDBStoragePartition first = mock(RocksDBStoragePartition.class);
      RocksDBStoragePartition second = mock(RocksDBStoragePartition.class);
      when(first.getRocksDBStatValue("rocksdb.num-running-flushes")).thenReturn(2L);
      when(second.getRocksDBStatValue("rocksdb.num-running-flushes")).thenReturn(3L);
      when(first.getRocksDBStatValue("rocksdb.num-running-compactions")).thenReturn(1L);
      when(second.getRocksDBStatValue("rocksdb.num-running-compactions")).thenReturn(2L);
      if (failure) {
        when(second.getRocksDBStatValue("rocksdb.num-running-flushes")).thenThrow(new VeniceException("test failure"));
      }
      memory.registerPartition("first", first);
      memory.registerPartition("second", second);
      memory.getOrCreateBackgroundStats(mock(Env.class));
      propertyReader.collectAllMetrics();
      if (failure) {
        assertTrue(
            propertyReader.collectAllMetrics()
                .stream()
                .noneMatch(metric -> metric.getName().endsWith("." + FLUSH_RUNNING_JOBS.getMetricName())));
      } else {
        OpenTelemetryDataTestUtils.validateHistogramPointData(
            propertyReader,
            5,
            5,
            1,
            5,
            attributes,
            FLUSH_RUNNING_JOBS.getMetricName(),
            "server");
      }
      OpenTelemetryDataTestUtils.validateHistogramPointData(
          propertyReader,
          3,
          3,
          1,
          3,
          attributes,
          COMPACTION_RUNNING_JOBS.getMetricName(),
          "server");
    }
  }

  @Test
  public void testCompletionCountersDurationsAndThroughputBytes() {
    RocksDBBackgroundStats stats = new RocksDBBackgroundStats(repository, "rocksdb", "test-cluster", mock(Env.class));
    stats.recordFlushCompleted();
    stats.recordFlushDuration(2_500_000);
    stats.recordFlushBytes(100);
    stats.recordCompactionCompleted();
    stats.recordCompactionStats(1_250, 200, 150);
    counter(FLUSH_COMPLETED_COUNT, 1);
    counter(COMPACTION_COMPLETED_COUNT, 1);
    counter(FLUSH_RAW_BYTES, 100);
    counter(COMPACTION_INPUT_BYTES, 200);
    counter(COMPACTION_OUTPUT_BYTES, 150);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        reader,
        2.5,
        2.5,
        1,
        2.5,
        attributes,
        FLUSH_TIME.getMetricName(),
        "server");
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        reader,
        1.25,
        1.25,
        1,
        1.25,
        attributes,
        COMPACTION_TIME.getMetricName(),
        "server");
    assertEquals(tehuti(FLUSH_TIME, "Avg"), 2.5);
    assertEquals(tehuti(COMPACTION_TIME, "Max"), 1.25);
    assertEquals(tehuti(FLUSH_RAW_BYTES, "Total"), 100.0);
    assertNotNull(repository.getMetric(".rocksdb--rocksdb.flush.completed_raw_bytes.Rate"));
  }

  @Test
  public void testNoFabricatedCompletionsOrDurations() {
    new RocksDBBackgroundStats(repository, "rocksdb", "test-cluster", mock(Env.class));
    assertTrue(
        reader.collectAllMetrics()
            .stream()
            .noneMatch(
                metric -> metric.getName().endsWith("." + FLUSH_TIME.getMetricName())
                    || metric.getName().endsWith("." + COMPACTION_TIME.getMetricName())
                    || metric.getName().endsWith("." + FLUSH_COMPLETED_COUNT.getMetricName())));
  }

  @Test
  public void testMetricShapesAndRegistration() {
    for (RocksDBBackgroundOtelMetricEntity entity: RocksDBBackgroundOtelMetricEntity.values()) {
      assertTrue(SERVER_METRIC_ENTITIES.contains(entity.getMetricEntity()));
      assertEquals(entity.getMetricEntity().getDimensionsList(), Collections.singleton(VENICE_CLUSTER_NAME));
      assertEquals(entity.getMetricName(), entity.getMetricEntity().getMetricName());
    }
    assertShape(FLUSH_POOL_CAPACITY, "rocksdb.flush.pool.capacity", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER);
    assertShape(
        COMPACTION_POOL_CAPACITY,
        "rocksdb.compaction.pool.capacity",
        MetricType.ASYNC_GAUGE,
        MetricUnit.NUMBER);
    assertShape(FLUSH_POOL_QUEUE_DEPTH, "rocksdb.flush.pool.queue_depth", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER);
    assertShape(
        COMPACTION_POOL_QUEUE_DEPTH,
        "rocksdb.compaction.pool.queue_depth",
        MetricType.ASYNC_GAUGE,
        MetricUnit.NUMBER);
    assertShape(
        FLUSH_RUNNING_JOBS,
        "rocksdb.flush.running_jobs",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER);
    assertShape(
        COMPACTION_RUNNING_JOBS,
        "rocksdb.compaction.running_jobs",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER);
    assertShape(FLUSH_COMPLETED_COUNT, "rocksdb.flush.completed_count", MetricType.COUNTER, MetricUnit.NUMBER);
    assertShape(
        COMPACTION_COMPLETED_COUNT,
        "rocksdb.compaction.completed_count",
        MetricType.COUNTER,
        MetricUnit.NUMBER);
    assertShape(FLUSH_TIME, "rocksdb.flush.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND);
    assertShape(COMPACTION_TIME, "rocksdb.compaction.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND);
    assertShape(FLUSH_RAW_BYTES, "rocksdb.flush.completed_raw_bytes", MetricType.COUNTER, MetricUnit.BYTES);
    assertShape(
        COMPACTION_INPUT_BYTES,
        "rocksdb.compaction.completed_input_bytes",
        MetricType.COUNTER,
        MetricUnit.BYTES);
    assertShape(
        COMPACTION_OUTPUT_BYTES,
        "rocksdb.compaction.completed_output_bytes",
        MetricType.COUNTER,
        MetricUnit.BYTES);
  }

  private void assertShape(RocksDBBackgroundOtelMetricEntity entity, String name, MetricType type, MetricUnit unit) {
    assertEquals(entity.getMetricName(), name);
    assertEquals(entity.getMetricEntity().getMetricType(), type);
    assertEquals(entity.getMetricEntity().getUnit(), unit);
  }

  private void gauge(RocksDBBackgroundOtelMetricEntity entity, long value) {
    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(reader, value, attributes, entity.getMetricName(), "server");
  }

  private void counter(RocksDBBackgroundOtelMetricEntity entity, long value) {
    OpenTelemetryDataTestUtils
        .validateLongPointDataFromCounter(reader, value, attributes, entity.getMetricName(), "server");
  }

  private void histogram(RocksDBBackgroundOtelMetricEntity entity, double min, double max, int count, double sum) {
    OpenTelemetryDataTestUtils
        .validateHistogramPointData(reader, min, max, count, sum, attributes, entity.getMetricName(), "server");
  }

  private double tehuti(RocksDBBackgroundOtelMetricEntity entity, String stat) {
    return repository.getMetric(".rocksdb--" + entity.getMetricName() + "." + stat).value();
  }
}
