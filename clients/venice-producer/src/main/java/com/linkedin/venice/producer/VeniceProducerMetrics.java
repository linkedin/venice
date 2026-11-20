package com.linkedin.venice.producer;

import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceProducerOperation;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Metrics for Venice Producer operations.
 *
 * <p>Metrics are emitted to Tehuti and, when the {@link MetricsRepository} is an OpenTelemetry enabled
 * {@link com.linkedin.venice.stats.VeniceMetricsRepository}, also to OpenTelemetry. The Tehuti sensor names are
 * unchanged. The OpenTelemetry metrics are defined in {@link VeniceProducerMetricEntity} and are emitted with the
 * metric prefix of the given repository, so the repository should be dedicated to the producer and created for
 * {@link com.linkedin.venice.stats.ClientType#ONLINE_PRODUCER} with {@link #PRODUCER_METRIC_ENTITIES}.</p>
 *
 * <h3>Latency Metrics</h3>
 * <ul>
 *   <li><b>queue_wait_latency</b>: Time waiting in worker queue (submit to worker start).
 *       OTel: {@code queue_wait_time}</li>
 *   <li><b>preprocessing_latency</b>: Worker start to preprocess end (serialization + schema lookup).
 *       OTel: {@code preprocessing_time}</li>
 *   <li><b>produce_to_durable_buffer_latency</b>: Preprocess end to broker ack.
 *       OTel: {@code produce_to_durable_buffer_time}</li>
 *   <li><b>caller_to_pubsub_producer_buffer_latency</b>: Caller submit to PubSub producer buffer.
 *       OTel: {@code caller_to_pubsub_producer_buffer_time}</li>
 *   <li><b>end_to_end_latency</b>: Caller submit to future completion (full round-trip).
 *       OTel: {@code call_time} with the response status category dimension</li>
 * </ul>
 *
 * <h3>Operation Counters</h3>
 * <ul>
 *   <li><b>write_operation</b>: Total write operations (put + delete + update) at submission. Tehuti only</li>
 *   <li><b>put_operation</b>, <b>delete_operation</b>, <b>update_operation</b>: Per-type counters at submission.
 *       Tehuti only; OTel {@code call_count} carries the operation type but counts completed writes, so submitted
 *       writes are {@code call_count} plus {@code pending_call_count}</li>
 *   <li><b>success_write_operation</b>, <b>failed_write_operation</b>: Outcome counters.
 *       OTel: {@code call_count} with the response status category dimension</li>
 *   <li><b>pending_write_operation</b>: Current in-flight operations (gauge). OTel: {@code pending_call_count}</li>
 * </ul>
 *
 * <h3>Queue Size Gauges</h3>
 * Tehuti only. In OTel they are covered by the {@link ThreadPoolOtelMetricEntity} metrics of the producer worker and
 * callback thread pools, which {@link PartitionedProducerExecutor} registers through
 * {@link com.linkedin.venice.stats.ThreadPoolStats}.
 * <ul>
 *   <li><b>total_worker_queue_size</b>: Sum of all worker queue depths</li>
 *   <li><b>callback_queue_size</b>: Callback executor queue depth</li>
 * </ul>
 */
public class VeniceProducerMetrics extends AbstractVeniceStats implements Closeable {
  /** All the OpenTelemetry metric entities emitted by the producer, including its thread pool stats */
  public static final Collection<MetricEntity> PRODUCER_METRIC_ENTITIES =
      getUniqueMetricEntities(Arrays.asList(VeniceProducerMetricEntity.class, ThreadPoolOtelMetricEntity.class));

  private final boolean enableMetrics;

  // Tehuti only sensors: OTel CALL_COUNT carries the operation type and outcome as dimensions
  private Sensor operationSensor = null;
  private Sensor putOperationSensor = null;
  private Sensor deleteOperationSensor = null;
  private Sensor updateOperationSensor = null;
  private Sensor pendingOperationSensor = null;
  private final AtomicInteger pendingOperationCounter = new AtomicInteger(0);

  // Tehuti and OTel metrics
  private MetricEntityStateTwoEnums<VeniceProducerOperation, VeniceResponseStatusCategory> successOperationMetric =
      null;
  private MetricEntityStateTwoEnums<VeniceProducerOperation, VeniceResponseStatusCategory> failedOperationMetric = null;
  private MetricEntityStateOneEnum<VeniceProducerOperation> produceLatencyMetric = null;
  private MetricEntityStateOneEnum<VeniceProducerOperation> preprocessingLatencyMetric = null;
  private MetricEntityStateOneEnum<VeniceProducerOperation> queueWaitLatencyMetric = null;
  private MetricEntityStateOneEnum<VeniceProducerOperation> callerToPubSubBufferLatencyMetric = null;
  // Total time: submission to future completion
  private MetricEntityStateTwoEnums<VeniceProducerOperation, VeniceResponseStatusCategory> endToEndLatencyMetric = null;
  private AsyncMetricEntityStateBase pendingCallCountMetric = null;

  private VeniceOpenTelemetryMetricsRepository otelRepository = null;
  private Map<VeniceMetricsDimensions, String> baseDimensionsMap = null;
  private Attributes baseAttributes = null;

  // Executor reference for queue size metrics (set after executor is created)
  private volatile PartitionedProducerExecutor executor;

  public VeniceProducerMetrics(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    if (metricsRepository != null) {
      enableMetrics = true;

      OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
          OpenTelemetryMetricsSetup.builder(metricsRepository)
              // set all base dimensions for this stats class and build
              .setStoreName(storeName)
              .build();
      otelRepository = otelData.getOtelRepository();
      baseDimensionsMap = otelData.getBaseDimensionsMap();
      baseAttributes = otelData.getBaseAttributes();

      operationSensor =
          registerSensor(VeniceProducerTehutiMetricName.WRITE_OPERATION.getMetricName(), new OccurrenceRate());
      putOperationSensor =
          registerSensor(VeniceProducerTehutiMetricName.PUT_OPERATION.getMetricName(), new OccurrenceRate());
      deleteOperationSensor =
          registerSensor(VeniceProducerTehutiMetricName.DELETE_OPERATION.getMetricName(), new OccurrenceRate());
      updateOperationSensor =
          registerSensor(VeniceProducerTehutiMetricName.UPDATE_OPERATION.getMetricName(), new OccurrenceRate());

      successOperationMetric = MetricEntityStateTwoEnums.create(
          VeniceProducerMetricEntity.CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          VeniceProducerTehutiMetricName.SUCCESS_WRITE_OPERATION,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          VeniceProducerOperation.class,
          VeniceResponseStatusCategory.class);
      failedOperationMetric = MetricEntityStateTwoEnums.create(
          VeniceProducerMetricEntity.CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          VeniceProducerTehutiMetricName.FAILED_WRITE_OPERATION,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          VeniceProducerOperation.class,
          VeniceResponseStatusCategory.class);

      produceLatencyMetric = createLatencyMetric(
          VeniceProducerMetricEntity.PRODUCE_TO_DURABLE_BUFFER_TIME,
          VeniceProducerTehutiMetricName.PRODUCE_TO_DURABLE_BUFFER_LATENCY);
      preprocessingLatencyMetric = createLatencyMetric(
          VeniceProducerMetricEntity.PREPROCESSING_TIME,
          VeniceProducerTehutiMetricName.PREPROCESSING_LATENCY);
      queueWaitLatencyMetric = createLatencyMetric(
          VeniceProducerMetricEntity.QUEUE_WAIT_TIME,
          VeniceProducerTehutiMetricName.QUEUE_WAIT_LATENCY);
      callerToPubSubBufferLatencyMetric = createLatencyMetric(
          VeniceProducerMetricEntity.CALLER_TO_PUBSUB_PRODUCER_BUFFER_TIME,
          VeniceProducerTehutiMetricName.CALLER_TO_PUBSUB_PRODUCER_BUFFER_LATENCY);

      pendingOperationSensor =
          registerSensor(VeniceProducerTehutiMetricName.PENDING_WRITE_OPERATION.getMetricName(), new Gauge());
      pendingCallCountMetric = AsyncMetricEntityStateBase.create(
          VeniceProducerMetricEntity.PENDING_CALL_COUNT.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          baseAttributes,
          () -> pendingOperationCounter.get());

      endToEndLatencyMetric = MetricEntityStateTwoEnums.create(
          VeniceProducerMetricEntity.CALL_TIME.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          VeniceProducerTehutiMetricName.END_TO_END_LATENCY,
          getLatencyTehutiStats(VeniceProducerTehutiMetricName.END_TO_END_LATENCY),
          baseDimensionsMap,
          VeniceProducerOperation.class,
          VeniceResponseStatusCategory.class);
    } else {
      enableMetrics = false;
    }
  }

  /**
   * Creates a latency metric which emits the same Tehuti percentiles as before along with an OTel histogram.
   */
  private MetricEntityStateOneEnum<VeniceProducerOperation> createLatencyMetric(
      VeniceProducerMetricEntity metricEntity,
      VeniceProducerTehutiMetricName tehutiMetricName) {
    return MetricEntityStateOneEnum.create(
        metricEntity.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        tehutiMetricName,
        getLatencyTehutiStats(tehutiMetricName),
        baseDimensionsMap,
        VeniceProducerOperation.class);
  }

  private List<MeasurableStat> getLatencyTehutiStats(VeniceProducerTehutiMetricName tehutiMetricName) {
    return Collections.singletonList(
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + tehutiMetricName.getMetricName()));
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
    // Tehuti only: OTel gets the queue sizes from the thread pool metrics of the producer executor
    // Total worker queue size (sum of all worker queues)
    registerSensor(
        new AsyncGauge(
            (config, now) -> this.executor.getTotalWorkerQueueSize(),
            VeniceProducerTehutiMetricName.TOTAL_WORKER_QUEUE_SIZE.getMetricName()));

    // Callback queue size
    registerSensor(
        new AsyncGauge(
            (config, now) -> this.executor.getCallbackQueueSize(),
            VeniceProducerTehutiMetricName.CALLBACK_QUEUE_SIZE.getMetricName()));
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

  public void recordSuccessfulRequestWithLatency(VeniceProducerOperation operation, double latencyMs) {
    if (enableMetrics) {
      successOperationMetric.record(1, operation, VeniceResponseStatusCategory.SUCCESS);
      produceLatencyMetric.record(latencyMs, operation);
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }

  public void recordFailedRequest(VeniceProducerOperation operation) {
    if (enableMetrics) {
      failedOperationMetric.record(1, operation, VeniceResponseStatusCategory.FAIL);
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }

  public void recordPreprocessingLatency(VeniceProducerOperation operation, double latencyMs) {
    if (enableMetrics) {
      preprocessingLatencyMetric.record(latencyMs, operation);
    }
  }

  /**
   * Record the end-to-end latency (from submission to future completion).
   *
   * @param operation the write operation type
   * @param latencyMs end-to-end latency in milliseconds
   */
  public void recordEndToEndLatency(VeniceProducerOperation operation, double latencyMs) {
    if (enableMetrics) {
      endToEndLatencyMetric.record(latencyMs, operation, VeniceResponseStatusCategory.SUCCESS);
    }
  }

  /**
   * Records the queue wait latency - the time a task spent waiting in the worker queue
   * before being picked up by a worker thread.
   *
   * @param operation the write operation type
   * @param latencyMs latency in milliseconds
   */
  public void recordQueueWaitLatency(VeniceProducerOperation operation, double latencyMs) {
    if (enableMetrics) {
      queueWaitLatencyMetric.record(latencyMs, operation);
    }
  }

  /**
   * Records the caller to PubSub producer buffer latency - the total time from caller
   * submission to when the write is queued to the PubSub producer's buffer. This includes
   * queue wait time, preprocessing, and the synchronous time to submit to VeniceWriter.
   *
   * @param operation the write operation type
   * @param latencyMs latency in milliseconds (supports sub-millisecond precision as a double)
   */
  public void recordCallerToPubSubBufferLatency(VeniceProducerOperation operation, double latencyMs) {
    if (enableMetrics) {
      callerToPubSubBufferLatencyMetric.record(latencyMs, operation);
    }
  }

  /**
   * Stops the OTel callbacks that read this producer's state, so a producer recreated for the same store on the same
   * repository reports its own values. Tehuti sensors are kept as before.
   */
  @Override
  public void close() {
    if (otelRepository != null && pendingCallCountMetric != null) {
      otelRepository.closeObservableInstrument(
          VeniceProducerMetricEntity.PENDING_CALL_COUNT.getMetricEntity(),
          pendingCallCountMetric.getOtelMetric());
    }
  }

  /** Visible for testing */
  Attributes getBaseAttributes() {
    return baseAttributes;
  }

  /**
   * Tehuti sensor names of the producer metrics. These are the same names as before the OTel migration.
   */
  public enum VeniceProducerTehutiMetricName implements TehutiMetricNameEnum {
    WRITE_OPERATION, PUT_OPERATION, DELETE_OPERATION, UPDATE_OPERATION, SUCCESS_WRITE_OPERATION, FAILED_WRITE_OPERATION,
    PRODUCE_TO_DURABLE_BUFFER_LATENCY, PREPROCESSING_LATENCY, QUEUE_WAIT_LATENCY,
    CALLER_TO_PUBSUB_PRODUCER_BUFFER_LATENCY, PENDING_WRITE_OPERATION, END_TO_END_LATENCY, TOTAL_WORKER_QUEUE_SIZE,
    CALLBACK_QUEUE_SIZE
  }
}
