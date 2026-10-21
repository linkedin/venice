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

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.rocksdb.Env;
import org.rocksdb.Priority;


/**
 * Registered once per shared environment by {@link RocksDBMemoryStats}. No native resource is owned here.
 * Running-job distributions reuse property collection, limited to one sample per second per job kind.
 */
public class RocksDBBackgroundStats extends AbstractVeniceStats {
  private final Env env;
  private final LongSupplier nanoTime;
  private final Map<RocksDBBackgroundOtelMetricEntity, MetricEntityStateBase> metrics =
      new EnumMap<>(RocksDBBackgroundOtelMetricEntity.class);
  private final OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otel;
  private long lastFlushSample;
  private long lastCompactionSample;
  private boolean flushSampled;
  private boolean compactionSampled;

  RocksDBBackgroundStats(MetricsRepository repository, String name, String clusterName, Env env) {
    this(repository, name, clusterName, env, System::nanoTime);
  }

  RocksDBBackgroundStats(
      MetricsRepository repository,
      String name,
      String clusterName,
      Env env,
      LongSupplier nanoTime) {
    super(repository, name);
    this.env = env;
    this.nanoTime = nanoTime;
    otel = OpenTelemetryMetricsSetup.builder(repository).setClusterName(clusterName).build();
    gauge(FLUSH_POOL_CAPACITY, () -> env.getBackgroundThreads(Priority.HIGH));
    gauge(COMPACTION_POOL_CAPACITY, () -> env.getBackgroundThreads(Priority.LOW));
    gauge(FLUSH_POOL_QUEUE_DEPTH, () -> env.getThreadPoolQueueLen(Priority.HIGH));
    gauge(COMPACTION_POOL_QUEUE_DEPTH, () -> env.getThreadPoolQueueLen(Priority.LOW));
    distribution(FLUSH_RUNNING_JOBS);
    distribution(COMPACTION_RUNNING_JOBS);
    distribution(FLUSH_TIME);
    distribution(COMPACTION_TIME);
    metric(FLUSH_COMPLETED_COUNT, new Count(), new OccurrenceRate());
    metric(COMPACTION_COMPLETED_COUNT, new Count(), new OccurrenceRate());
    metric(FLUSH_RAW_BYTES, new Total(), new Rate());
    metric(COMPACTION_INPUT_BYTES, new Total(), new Rate());
    metric(COMPACTION_OUTPUT_BYTES, new Total(), new Rate());
  }

  boolean usesEnv(Env env) {
    return this.env == env;
  }

  private void gauge(RocksDBBackgroundOtelMetricEntity entity, LongSupplier supplier) {
    AsyncMetricEntityStateBase.create(
        entity.getMetricEntity(),
        otel.getOtelRepository(),
        this::registerSensorIfAbsent,
        entity,
        Collections.singletonList(new AsyncGauge((config, now) -> supplier.getAsLong(), entity.getMetricName())),
        otel.getBaseDimensionsMap(),
        otel.getBaseAttributes(),
        supplier);
  }

  private void distribution(RocksDBBackgroundOtelMetricEntity entity) {
    metric(entity, new Avg(), new Max());
  }

  private void metric(RocksDBBackgroundOtelMetricEntity entity, MeasurableStat... stats) {
    metrics.put(
        entity,
        MetricEntityStateBase.create(
            entity.getMetricEntity(),
            otel.getOtelRepository(),
            this::registerSensorIfAbsent,
            entity,
            Arrays.asList(stats),
            otel.getBaseDimensionsMap(),
            otel.getBaseAttributes()));
  }

  synchronized void sampleRunningJobs(boolean flush, long jobs) {
    long now = nanoTime.getAsLong();
    boolean sampled = flush ? flushSampled : compactionSampled;
    long last = flush ? lastFlushSample : lastCompactionSample;
    if (sampled && now - last < TimeUnit.SECONDS.toNanos(1)) {
      return;
    }
    metrics.get(flush ? FLUSH_RUNNING_JOBS : COMPACTION_RUNNING_JOBS).record(jobs);
    if (flush) {
      lastFlushSample = now;
      flushSampled = true;
    } else {
      lastCompactionSample = now;
      compactionSampled = true;
    }
  }

  public void recordFlushCompleted() {
    metrics.get(FLUSH_COMPLETED_COUNT).record(1);
  }

  public void recordFlushDuration(long elapsedNanos) {
    metrics.get(FLUSH_TIME).record(elapsedNanos / 1_000_000.0);
  }

  public void recordFlushBytes(long rawBytes) {
    metrics.get(FLUSH_RAW_BYTES).record(rawBytes);
  }

  public void recordCompactionCompleted() {
    metrics.get(COMPACTION_COMPLETED_COUNT).record(1);
  }

  public void recordCompactionStats(long elapsedMicros, long inputBytes, long outputBytes) {
    metrics.get(COMPACTION_TIME).record(elapsedMicros / 1_000.0);
    metrics.get(COMPACTION_INPUT_BYTES).record(inputBytes);
    metrics.get(COMPACTION_OUTPUT_BYTES).record(outputBytes);
  }
}
