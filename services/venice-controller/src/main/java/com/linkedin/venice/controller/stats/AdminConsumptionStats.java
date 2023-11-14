package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;


public class AdminConsumptionStats extends AbstractVeniceStats {
  final private Sensor adminConsumeFailCountSensor;
  final private Sensor adminConsumeFailRetriableMessageCountSensor;
  final private Sensor adminTopicDIVErrorReportCountSensor;
  final private Sensor adminConsumptionCycleDurationMsSensor;
  /**
   * The time it took MM to copy the message from parent to child controller's admin topic.
   */
  final private Sensor adminMessageMMLatencySensor;
  /**
   * The time difference between the message made available to the local topic and delegated to the in memory topic
   * by the {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask}.
   */
  final private Sensor adminMessageDelegateLatencySensor;
  /**
   * The time difference between the message's delegated time and the first attempt to process the message.
   */
  final private Sensor adminMessageStartProcessingLatencySensor;
  /**
   * The time difference between the first attempt to process the message and when the message is fully processed. This
   * includes the latency caused by failures/retries. This metric does not include process time for add version admin messages.
   */
  final private Sensor adminMessageProcessLatencySensor;
  /**
   * Similar to {@code adminMessageProcessLatencySensor} but specifically for add version admin messages.
   */
  final private Sensor adminMessageAddVersionProcessLatencySensor;
  /**
   * Total end to end latency from the time when the message was first generated in the parent controller to when it's
   * fully processed in the child controller.
   */
  final private Sensor adminMessageTotalLatencySensor;

  private long adminConsumptionFailedOffset;
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
   * A gauge that represents the consumption offset checkpointed into ZK. If remote consumption is enabled, this is the
   * checkpoint upstream offset; otherwise, it's the checkpoint local consumption offset.
   */
  private long adminConsumptionCheckpointOffset;

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

  public AdminConsumptionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    adminConsumeFailCountSensor = registerSensor("failed_admin_messages", new Count());
    adminConsumeFailRetriableMessageCountSensor = registerSensor("failed_retriable_admin_messages", new Count());
    adminTopicDIVErrorReportCountSensor = registerSensor("admin_message_div_error_report_count", new Count());
    registerSensor(new AsyncGauge((c, t) -> adminConsumptionFailedOffset, "failed_admin_message_offset"));
    adminConsumptionCycleDurationMsSensor =
        registerSensor("admin_consumption_cycle_duration_ms", new Avg(), new Min(), new Max());
    registerSensor(new AsyncGauge((c, t) -> pendingAdminMessagesCountGauge, "pending_admin_messages_count"));
    registerSensor(
        new AsyncGauge((c, t) -> storesWithPendingAdminMessagesCountGauge, "stores_with_pending_admin_messages_count"));
    adminMessageMMLatencySensor = registerSensor("admin_message_mm_latency_ms", new Avg(), new Max());
    adminMessageDelegateLatencySensor = registerSensor("admin_message_delegate_latency_ms", new Avg(), new Max());
    adminMessageStartProcessingLatencySensor =
        registerSensor("admin_message_start_processing_latency_ms", new Avg(), new Max());
    adminMessageProcessLatencySensor = registerSensor("admin_message_process_latency_ms", new Avg(), new Max());
    adminMessageAddVersionProcessLatencySensor =
        registerSensor("admin_message_add_version_process_latency_ms", new Avg(), new Max());
    adminMessageTotalLatencySensor = registerSensor("admin_message_total_latency_ms", new Avg(), new Max());
    registerSensor(new AsyncGauge((c, t) -> this.adminConsumptionOffsetLag, "admin_consumption_offset_lag"));
    registerSensor(new AsyncGauge((c, t) -> this.maxAdminConsumptionOffsetLag, "max_admin_consumption_offset_lag"));
  }

  /**
   * Record the number of failed admin messages in the past one minute;
   * if controller keeps retrying the admin messages, this metric will keep growing;
   * this metric will be reset to 0 once the blocked admin message is processed.
   */
  public void recordFailedAdminConsumption() {
    adminConsumeFailCountSensor.record();
  }

  public void recordFailedRetriableAdminConsumption() {
    adminConsumeFailRetriableMessageCountSensor.record();
  }

  public void recordAdminTopicDIVErrorReportCount() {
    adminTopicDIVErrorReportCountSensor.record();
  }

  public void recordAdminConsumptionCycleDurationMs(double value) {
    adminConsumptionCycleDurationMsSensor.record(value);
  }

  public void recordPendingAdminMessagesCount(double value) {
    this.pendingAdminMessagesCountGauge = value;
  }

  public void recordStoresWithPendingAdminMessagesCount(double value) {
    this.storesWithPendingAdminMessagesCountGauge = value;
  }

  public void setAdminConsumptionFailedOffset(long adminConsumptionFailedOffset) {
    this.adminConsumptionFailedOffset = adminConsumptionFailedOffset;
  }

  public void recordAdminMessageMMLatency(double value) {
    adminMessageMMLatencySensor.record(value);
  }

  public void recordAdminMessageDelegateLatency(double value) {
    adminMessageDelegateLatencySensor.record(value);
  }

  public void recordAdminMessageStartProcessingLatency(double value) {
    adminMessageStartProcessingLatencySensor.record(value);
  }

  public void recordAdminMessageProcessLatency(double value) {
    adminMessageProcessLatencySensor.record(value);
  }

  public void recordAdminMessageAddVersionProcessLatency(double value) {
    adminMessageAddVersionProcessLatencySensor.record(value);
  }

  public void recordAdminMessageTotalLatency(double value) {
    adminMessageTotalLatencySensor.record(value);
  }

  public void setAdminConsumptionCheckpointOffset(long adminConsumptionCheckpointOffset) {
    this.adminConsumptionCheckpointOffset = adminConsumptionCheckpointOffset;
  }

  /**
   * Lazily register the checkpoint offset metric after knowing the latest checkpoint offset, so that restarting the
   * controller node will not result in the metric value dipping to 0.
   */
  public void registerAdminConsumptionCheckpointOffset() {
    registerSensorIfAbsent(
        new AsyncGauge((c, t) -> this.adminConsumptionCheckpointOffset, "admin_consumption_checkpoint_offset"));
  }

  public void setAdminConsumptionOffsetLag(long adminConsumptionOffsetLag) {
    this.adminConsumptionOffsetLag = adminConsumptionOffsetLag;
  }

  public void setMaxAdminConsumptionOffsetLag(long maxAdminConsumptionOffsetLag) {
    this.maxAdminConsumptionOffsetLag = maxAdminConsumptionOffsetLag;
  }
}
