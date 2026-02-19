package com.linkedin.venice.producer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Metrics for Venice Producer operations.
 *
 * <h3>Latency Metrics</h3>
 * <ul>
 *   <li><b>queue_wait_latency</b>: Time waiting in worker queue (submit to worker start)</li>
 *   <li><b>preprocessing_latency</b>: Worker start to preprocess end (serialization + schema lookup)</li>
 *   <li><b>produce_to_durable_buffer_latency</b>: Preprocess end to broker ack</li>
 *   <li><b>caller_to_pubsub_producer_buffer_latency</b>: Caller submit to PubSub producer buffer</li>
 *   <li><b>end_to_end_latency</b>: Caller submit to future completion (full round-trip)</li>
 * </ul>
 *
 * <h3>Operation Counters</h3>
 * <ul>
 *   <li><b>write_operation</b>: Total write operations (put + delete + update)</li>
 *   <li><b>put_operation</b>, <b>delete_operation</b>, <b>update_operation</b>: Per-type counters</li>
 *   <li><b>success_write_operation</b>, <b>failed_write_operation</b>: Outcome counters</li>
 *   <li><b>pending_write_operation</b>: Current in-flight operations (gauge)</li>
 * </ul>
 *
 * <h3>Queue Size Gauges</h3>
 * <ul>
 *   <li><b>total_worker_queue_size</b>: Sum of all worker queue depths</li>
 *   <li><b>callback_queue_size</b>: Callback executor queue depth</li>
 * </ul>
 */
public class VeniceProducerMetrics extends AbstractVeniceStats {
  private final boolean enableMetrics;

  // === EXISTING SENSORS ===
  private Sensor operationSensor = null;
  private Sensor putOperationSensor = null;
  private Sensor deleteOperationSensor = null;
  private Sensor updateOperationSensor = null;
  private Sensor successOperationSensor = null;
  private Sensor failedOperationSensor = null;
  private Sensor produceLatencySensor = null;
  private Sensor preprocessingLatencySensor = null;
  private Sensor queueWaitLatencySensor = null;
  private Sensor callerToPubSubBufferLatencySensor = null;
  private Sensor endToEndLatencySensor = null; // Total time: submission to future completion
  private Sensor pendingOperationSensor = null;
  private final AtomicInteger pendingOperationCounter = new AtomicInteger(0);

  // Executor reference for queue size metrics (set after executor is created)
  private volatile PartitionedProducerExecutor executor;

  public VeniceProducerMetrics(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    if (metricsRepository != null) {
      enableMetrics = true;
      operationSensor = registerSensor("write_operation", new OccurrenceRate());
      putOperationSensor = registerSensor("put_operation", new OccurrenceRate());
      deleteOperationSensor = registerSensor("delete_operation", new OccurrenceRate());
      updateOperationSensor = registerSensor("update_operation", new OccurrenceRate());

      successOperationSensor = registerSensor("success_write_operation", new OccurrenceRate());
      failedOperationSensor = registerSensor("failed_write_operation", new OccurrenceRate());
      String produceLatencySensorName = "produce_to_durable_buffer_latency";
      produceLatencySensor = registerSensor(
          produceLatencySensorName,
          TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + produceLatencySensorName));
      String preprocessingLatencySensorName = "preprocessing_latency";
      preprocessingLatencySensor = registerSensor(
          preprocessingLatencySensorName,
          TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + preprocessingLatencySensorName));
      String queueWaitLatencySensorName = "queue_wait_latency";
      queueWaitLatencySensor = registerSensor(
          queueWaitLatencySensorName,
          TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + queueWaitLatencySensorName));
      String callerToPubSubBufferLatencySensorName = "caller_to_pubsub_producer_buffer_latency";
      callerToPubSubBufferLatencySensor = registerSensor(
          callerToPubSubBufferLatencySensorName,
          TehutiUtils
              .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + callerToPubSubBufferLatencySensorName));

      pendingOperationSensor = registerSensor("pending_write_operation", new Gauge());

      // NEW: End-to-end latency (submission to completion)
      String endToEndLatencySensorName = "end_to_end_latency";
      endToEndLatencySensor = registerSensor(
          endToEndLatencySensorName,
          TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + endToEndLatencySensorName));
    } else {
      enableMetrics = false;
    }
  }

  /**
   * Called after executor is created to register queue size gauges.
   *
   * @param executor the partitioned producer executor
   */
  public void registerQueueMetrics(PartitionedProducerExecutor executor) {
    if (!enableMetrics || executor == null || this.executor != null) {
      return;
    }
    this.executor = executor;

    // Register queue size gauges - the sensors are kept alive by the metrics repository
    // Total worker queue size (sum of all worker queues)
    registerSensor(new AsyncGauge((config, now) -> this.executor.getTotalWorkerQueueSize(), "total_worker_queue_size"));

    // Callback queue size
    registerSensor(new AsyncGauge((config, now) -> this.executor.getCallbackQueueSize(), "callback_queue_size"));
  }

  private void recordRequest() {
    if (enableMetrics) {
      operationSensor.record();
      pendingOperationSensor.record(pendingOperationCounter.incrementAndGet());
    }
  }

  public void recordPutRequest() {
    if (enableMetrics) {
      recordRequest();
      putOperationSensor.record();
    }
  }

  public void recordDeleteRequest() {
    if (enableMetrics) {
      recordRequest();
      deleteOperationSensor.record();
    }
  }

  public void recordUpdateRequest() {
    if (enableMetrics) {
      recordRequest();
      updateOperationSensor.record();
    }
  }

  public void recordSuccessfulRequestWithLatency(double latencyMs) {
    if (enableMetrics) {
      successOperationSensor.record();
      produceLatencySensor.record(latencyMs);
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }

  public void recordFailedRequest() {
    if (enableMetrics) {
      failedOperationSensor.record();
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }

  public void recordPreprocessingLatency(double latencyMs) {
    if (enableMetrics) {
      preprocessingLatencySensor.record(latencyMs);
    }
  }

  /**
   * Record the end-to-end latency (from submission to future completion).
   *
   * @param latencyMs end-to-end latency in milliseconds
   */
  public void recordEndToEndLatency(double latencyMs) {
    if (enableMetrics) {
      endToEndLatencySensor.record(latencyMs);
    }
  }

  /**
   * Records the queue wait latency - the time a task spent waiting in the worker queue
   * before being picked up by a worker thread.
   *
   * @param latencyMs latency in milliseconds
   */
  public void recordQueueWaitLatency(double latencyMs) {
    if (enableMetrics) {
      queueWaitLatencySensor.record(latencyMs);
    }
  }

  /**
   * Records the caller to PubSub producer buffer latency - the total time from caller
   * submission to when the write is queued to the PubSub producer's buffer. This includes
   * queue wait time, preprocessing, and the synchronous time to submit to VeniceWriter.
   *
   * @param latencyMs latency in milliseconds (supports sub-millisecond precision as a double)
   */
  public void recordCallerToPubSubBufferLatency(double latencyMs) {
    if (enableMetrics) {
      callerToPubSubBufferLatencySensor.record(latencyMs);
    }
  }
}
