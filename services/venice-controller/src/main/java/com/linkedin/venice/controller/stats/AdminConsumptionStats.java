package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


public class AdminConsumptionStats extends AbstractVeniceStats {
  private final MetricEntityStateBase failureCountMetric;
  private final MetricEntityStateBase retriableFailureCountMetric;
  private final MetricEntityStateBase divFailureCountMetric;
  /**
   * The number of admin messages with future protocol version that deserialized with future schema from system store.
   */
  private final MetricEntityStateBase futureSchemaCountMetric;

  /**
   * Time taken for the message from parent to be replicated to the child controller's admin topic.
   */
  private final MetricEntityStateOneEnum<AdminMessageType> produceToBrokerTimeMetric;
  /**
   * The time difference between the message made available to the local topic and delegated to the in memory topic by
   * the {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask}.
   */
  private final MetricEntityStateOneEnum<AdminMessageType> brokerToQueueTimeMetric;
  /**
   * Time between delegation and first processing attempt (queue_to_start).
   */
  private final MetricEntityStateOneEnum<AdminMessageType> queueToStartProcessingTimeMetric;
  /**
   * The time difference between the first attempt to process the message and when the message is fully processed. This
   * includes the latency caused by failures/retries.
   */
  private final MetricEntityStateOneEnum<AdminMessageType> processLatencyMetric;
  /**
   * Similar to {@code processLatencyMetric} but specifically for add version admin messages.
   */
  private final MetricEntityStateOneEnum<AdminMessageType> addVersionProcessLatencyMetric;

  /**
   * Tracks the duration of each batch processing cycle for admin messages. 
   */
  private final MetricEntityStateBase cycleTimeMetric;

  /**
   * A gauge reporting the total number of pending admin messages remaining in the internal queue at the end of each
   * consumption cycle. Pending messages could be caused by blocked admin operations or insufficient resources.
   */
  private double pendingAdminMessagesCountGauge;
  /**
   * A gauge reporting the number of stores with pending messages at the end of each consumption cycle.
   */
  private double storesWithPendingAdminMessagesCountGauge;
  /**
   * adminConsumptionOffsetLag = End offset of the admin topic in the source Kafka cluster - the latest consumed offset
   */
  private long adminConsumptionOffsetLag;
  /**
   * maxAdminConsumptionOffsetLag = End offset of the admin topic in the source Kafka cluster - the latest persisted offset
   * If there is a failed admin message for a specific store, with store level isolation, admin messages for other stores
   * will be processed; however, the checkpoint offset will freeze until there is no more failed admin message. In general,
   * the maxAdminConsumptionOffsetLag is equal to adminConsumptionOffsetLag, unless there is a failed admin message.
   */
  private long maxAdminConsumptionOffsetLag;

  /**
   * Total end to end latency from the time when the message was first generated in the parent controller to when it's
   * fully processed in the child controller.
   */
  private final Sensor adminMessageTotalLatencySensor;

  public AdminConsumptionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + "-admin_consumption_task");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    failureCountMetric = MetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.FAILED_ADMIN_MESSAGES,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        baseAttributes);

    retriableFailureCountMetric = MetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_RETRIABLE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.FAILED_RETRIABLE_ADMIN_MESSAGES,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        baseAttributes);

    divFailureCountMetric = MetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_DIV_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DIV_ERROR_REPORT_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        baseAttributes);

    futureSchemaCountMetric = MetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FUTURE_SCHEMA_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGES_WITH_FUTURE_PROTOCOL_VERSION_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        baseAttributes);

    produceToBrokerTimeMetric = MetricEntityStateOneEnum.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_REPLICATION_TO_LOCAL_BROKER_TIME
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_MM_LATENCY_MS,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        AdminMessageType.class);

    brokerToQueueTimeMetric = MetricEntityStateOneEnum.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_BROKER_TO_PROCESSING_QUEUE_TIME
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DELEGATE_LATENCY_MS,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        AdminMessageType.class);

    queueToStartProcessingTimeMetric = MetricEntityStateOneEnum.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_QUEUE_TO_START_PROCESSING_TIME
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_START_PROCESSING_LATENCY_MS,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        AdminMessageType.class);

    processLatencyMetric = MetricEntityStateOneEnum.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        AdminMessageType.class);

    addVersionProcessLatencyMetric = MetricEntityStateOneEnum.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_ADD_VERSION_PROCESS_LATENCY_MS,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        AdminMessageType.class);

    cycleTimeMetric = MetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_BATCH_PROCESSING_CYCLE_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_CYCLE_DURATION_MS,
        Arrays.asList(new Avg(), new Min(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    AsyncMetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PENDING_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.PENDING_ADMIN_MESSAGES_COUNT,
        Arrays.asList(
            new AsyncGauge(
                (ignored, ignored2) -> pendingAdminMessagesCountGauge,
                AdminConsumptionTehutiMetricNameEnum.PENDING_ADMIN_MESSAGES_COUNT.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> (long) pendingAdminMessagesCountGauge);

    AsyncMetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_STORE_PENDING_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.STORES_WITH_PENDING_ADMIN_MESSAGES_COUNT,
        Arrays.asList(
            new AsyncGauge(
                (ignored, ignored2) -> storesWithPendingAdminMessagesCountGauge,
                AdminConsumptionTehutiMetricNameEnum.STORES_WITH_PENDING_ADMIN_MESSAGES_COUNT.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> (long) storesWithPendingAdminMessagesCountGauge);

    AsyncMetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_OFFSET_LAG.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_OFFSET_LAG,
        Arrays.asList(
            new AsyncGauge(
                (ignored, ignored2) -> this.adminConsumptionOffsetLag,
                AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_OFFSET_LAG.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> this.adminConsumptionOffsetLag);

    AsyncMetricEntityStateBase.create(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_CHECKPOINT_OFFSET_LAG.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AdminConsumptionTehutiMetricNameEnum.MAX_ADMIN_CONSUMPTION_OFFSET_LAG,
        Arrays.asList(
            new AsyncGauge(
                (ignored, ignored2) -> this.maxAdminConsumptionOffsetLag,
                AdminConsumptionTehutiMetricNameEnum.MAX_ADMIN_CONSUMPTION_OFFSET_LAG.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> this.maxAdminConsumptionOffsetLag);

    // Tehuti-only
    adminMessageTotalLatencySensor = registerSensor("admin_message_total_latency_ms", new Avg(), new Max());
  }

  /**
   * Record the number of failed admin messages in the past one minute;
   * if controller keeps retrying the admin messages, this metric will keep growing;
   * this metric will be reset to 0 once the blocked admin message is processed.
   */
  public void recordFailedAdminConsumption() {
    failureCountMetric.record(1);
  }

  public void recordFailedRetriableAdminConsumption() {
    retriableFailureCountMetric.record(1);
  }

  public void recordAdminTopicDIVErrorReportCount() {
    divFailureCountMetric.record(1);
  }

  public void recordAdminConsumptionCycleDurationMs(double value) {
    cycleTimeMetric.record(value);
  }

  public void recordPendingAdminMessagesCount(double value) {
    this.pendingAdminMessagesCountGauge = value;
  }

  public void recordStoresWithPendingAdminMessagesCount(double value) {
    this.storesWithPendingAdminMessagesCountGauge = value;
  }

  public void recordAdminMessageMMLatency(double value, AdminMessageType adminMessageType) {
    produceToBrokerTimeMetric.record(value, adminMessageType);
  }

  public void recordAdminMessageDelegateLatency(double value, AdminMessageType adminMessageType) {
    brokerToQueueTimeMetric.record(value, adminMessageType);
  }

  public void recordAdminMessageStartProcessingLatency(double value, AdminMessageType adminMessageType) {
    queueToStartProcessingTimeMetric.record(value, adminMessageType);
  }

  public void recordAdminMessageProcessLatency(double value, AdminMessageType adminMessageType) {
    if (adminMessageType == AdminMessageType.ADD_VERSION) {
      addVersionProcessLatencyMetric.record(value, adminMessageType);
    } else {
      processLatencyMetric.record(value, adminMessageType);
    }
  }

  public void recordAdminMessageTotalLatency(double value) {
    adminMessageTotalLatencySensor.record(value);
  }

  public void setAdminConsumptionOffsetLag(long adminConsumptionOffsetLag) {
    this.adminConsumptionOffsetLag = adminConsumptionOffsetLag;
  }

  public void setMaxAdminConsumptionOffsetLag(long maxAdminConsumptionOffsetLag) {
    this.maxAdminConsumptionOffsetLag = maxAdminConsumptionOffsetLag;
  }

  public void recordAdminMessagesWithFutureProtocolVersionCount() {
    futureSchemaCountMetric.record(1);
  }

  enum AdminConsumptionTehutiMetricNameEnum implements TehutiMetricNameEnum {
    FAILED_ADMIN_MESSAGES, FAILED_RETRIABLE_ADMIN_MESSAGES, ADMIN_MESSAGE_DIV_ERROR_REPORT_COUNT,
    ADMIN_MESSAGES_WITH_FUTURE_PROTOCOL_VERSION_COUNT, ADMIN_MESSAGE_MM_LATENCY_MS, ADMIN_MESSAGE_DELEGATE_LATENCY_MS,
    ADMIN_MESSAGE_START_PROCESSING_LATENCY_MS, ADMIN_CONSUMPTION_CYCLE_DURATION_MS, ADMIN_MESSAGE_PROCESS_LATENCY_MS,
    ADMIN_MESSAGE_ADD_VERSION_PROCESS_LATENCY_MS, PENDING_ADMIN_MESSAGES_COUNT,
    STORES_WITH_PENDING_ADMIN_MESSAGES_COUNT, ADMIN_CONSUMPTION_OFFSET_LAG, MAX_ADMIN_CONSUMPTION_OFFSET_LAG
  }

  public enum AdminConsumptionOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Count of failed admin messages */
    ADMIN_CONSUMPTION_MESSAGE_FAILURE_COUNT(
        "admin_consumption.message.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of failed admin messages", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Count of retriable failed admin messages */
    ADMIN_CONSUMPTION_MESSAGE_RETRIABLE_FAILURE_COUNT(
        "admin_consumption.message.retriable_failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of retriable failed admin messages", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Count of admin message DIV error reports */
    ADMIN_CONSUMPTION_MESSAGE_DIV_FAILURE_COUNT(
        "admin_consumption.message.div_failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of admin message DIV error reports", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Count of admin messages with future protocol versions requiring schema fetch */
    ADMIN_CONSUMPTION_MESSAGE_FUTURE_SCHEMA_COUNT(
        "admin_consumption.message.future_schema_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of admin messages with future protocol versions requiring schema fetch", setOf(VENICE_CLUSTER_NAME)
    ),

    /** Time taken for the message from parent to be replicated to the child controller's admin topic */
    ADMIN_CONSUMPTION_MESSAGE_PHASE_REPLICATION_TO_LOCAL_BROKER_TIME(
        "admin_consumption.message.phase.replication_to_local_broker.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Time taken for the message from parent to be replicated to the child controller's admin topic",
        setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)
    ),
    /** Time from local broker timestamp to delegation to processing queue (broker to queue) */
    ADMIN_CONSUMPTION_MESSAGE_PHASE_BROKER_TO_PROCESSING_QUEUE_TIME(
        "admin_consumption.message.phase.broker_to_processing_queue.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND, "Time from local broker timestamp to delegation to processing queue",
        setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)
    ),
    /** Time from delegation to first processing attempt (queue to start) */
    ADMIN_CONSUMPTION_MESSAGE_PHASE_QUEUE_TO_START_PROCESSING_TIME(
        "admin_consumption.message.phase.queue_to_start_processing.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND, "Time from delegation to first processing attempt",
        setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)
    ),
    /** Time from start of processing to completion (start to end), differentiated by admin message type */
    ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME(
        "admin_consumption.message.phase.start_to_end_processing.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND, "Time from start of processing to completion",
        setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)
    ),
    /** Duration of batch processing cycle */
    ADMIN_CONSUMPTION_MESSAGE_BATCH_PROCESSING_CYCLE_TIME(
        "admin_consumption.message.batch_processing_cycle.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND, "Duration of batch processing cycle", setOf(VENICE_CLUSTER_NAME)
    ),

    /** Pending admin messages remaining in the internal queue */
    ADMIN_CONSUMPTION_MESSAGE_PENDING_COUNT(
        "admin_consumption.message.pending_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Pending admin messages remaining in the internal queue", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Number of stores with pending admin messages */
    ADMIN_CONSUMPTION_STORE_PENDING_COUNT(
        "admin_consumption.store.pending_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Number of stores with pending admin messages", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Difference between end offset and latest consumed offset */
    ADMIN_CONSUMPTION_CONSUMER_OFFSET_LAG(
        "admin_consumption.consumer.offset_lag", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Difference between end offset and latest consumed offset", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Difference between end offset and latest persisted (checkpoint) offset */
    ADMIN_CONSUMPTION_CONSUMER_CHECKPOINT_OFFSET_LAG(
        "admin_consumption.consumer.checkpoint_offset_lag", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Difference between end offset and latest persisted offset", setOf(VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    AdminConsumptionOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
