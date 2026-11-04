package com.linkedin.venice.listener;

import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Listener-owned time samples, independent of request arrival and metric export cadence.
 * Fixed-delay sampling approximates time utilization and may miss activity shorter than the sampling interval.
 */
final class ServerThreadPoolSampler implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(ServerThreadPoolSampler.class);
  private static final String READ_POOL_NAME = "storage_execution_thread_pool";
  private static final String COMPUTE_POOL_NAME = "storage_compute_thread_pool";
  private static final String NETTY_POOL_NAME = "netty_worker_thread_pool";
  static final long SAMPLE_INTERVAL_MS = 100;
  private final MetricEntityStateBase readMetric;
  private final MetricEntityStateBase computeMetric;
  private final MetricEntityStateBase nettyMetric;
  private final IntSupplier readCount;
  private final IntSupplier computeCount;
  private final IntSupplier nettyCount;
  private ScheduledFuture<?> future;
  private boolean closed;

  ServerThreadPoolSampler(
      MetricsRepository repository,
      ThreadPoolExecutor readExecutor,
      ThreadPoolExecutor computeExecutor,
      ServerNettyThreadStatsHandler nettyHandler,
      int nettyWorkerCount) {
    VeniceOpenTelemetryMetricsRepository otelRepository =
        OpenTelemetryMetricsSetup.builder(repository).setThreadPoolName(READ_POOL_NAME).build().getOtelRepository();
    if (otelRepository != null && otelRepository.emitOpenTelemetryMetrics()) {
      otelRepository.configureIntegerHistogramBuckets(
          ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION.getMetricEntity(),
          Math.max(readExecutor.getMaximumPoolSize(), computeExecutor.getMaximumPoolSize()));
      otelRepository.configureIntegerHistogramBuckets(
          ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_TIME_DISTRIBUTION.getMetricEntity(),
          nettyWorkerCount);
    }
    readMetric = createMetric(
        repository,
        READ_POOL_NAME,
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION);
    computeMetric = createMetric(
        repository,
        COMPUTE_POOL_NAME,
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION);
    nettyMetric = createMetric(
        repository,
        NETTY_POOL_NAME,
        ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_TIME_DISTRIBUTION);
    readCount = readExecutor::getActiveCount;
    computeCount = computeExecutor::getActiveCount;
    nettyCount = nettyHandler::getActiveRequestThreadCount;
  }

  private static MetricEntityStateBase createMetric(
      MetricsRepository repository,
      String poolName,
      ThreadPoolOtelMetricEntity entity) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo setup =
        OpenTelemetryMetricsSetup.builder(repository).setThreadPoolName(poolName).build();
    return MetricEntityStateBase.create(
        entity.getMetricEntity(),
        setup.getOtelRepository(),
        setup.getBaseDimensionsMap(),
        setup.getBaseAttributes());
  }

  synchronized void start(ScheduledExecutorService scheduler) {
    if (!closed && future == null && (readMetric.getOtelMetric() != null || computeMetric.getOtelMetric() != null
        || nettyMetric.getOtelMetric() != null)) {
      future = scheduler.scheduleWithFixedDelay(this::sample, 0, SAMPLE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
  }

  synchronized void sample() {
    if (!closed) {
      record(readMetric, readCount, READ_POOL_NAME);
      record(computeMetric, computeCount, COMPUTE_POOL_NAME);
      record(nettyMetric, nettyCount, NETTY_POOL_NAME);
    }
  }

  private static void record(MetricEntityStateBase metric, IntSupplier count, String poolName) {
    if (metric.getOtelMetric() != null) {
      try {
        metric.record(count.getAsInt());
      } catch (Exception e) {
        if (!RedundantExceptionFilter.getRedundantExceptionFilter()
            .isRedundantException("ServerThreadPoolSampler." + poolName, e)) {
          LOGGER.warn("Failed to sample server thread pool concurrency for {}", poolName, e);
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (future != null) {
      future.cancel(false);
      future = null;
    }
  }
}
