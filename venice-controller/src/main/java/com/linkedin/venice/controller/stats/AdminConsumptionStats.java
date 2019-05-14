package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;


public class AdminConsumptionStats extends AbstractVeniceStats {
  final private Sensor adminConsumeFailCountSensor;
  final private Sensor adminConsumeFailRetriableMessageCountSensor;
  final private Sensor adminTopicDIVErrorReportCountSensor;
  final private Sensor adminConsumptionCycleDurationMsSensor;
  final private Sensor pendingAdminMessagesCountSensor;
  final private Sensor storesWithPendingAdminMessagesCountSensor;
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

  public AdminConsumptionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    adminConsumeFailCountSensor = registerSensor("failed_admin_messages", new Count());
    adminConsumeFailRetriableMessageCountSensor = registerSensor("failed_retriable_admin_messages", new Count());
    adminTopicDIVErrorReportCountSensor = registerSensor("admin_message_div_error_report_count", new Count());
    registerSensor("failed_admin_message_offset", new Gauge(() -> adminConsumptionFailedOffset));
    adminConsumptionCycleDurationMsSensor = registerSensor("admin_consumption_cycle_duration_ms",
        new Avg(), new Min(), new Max());
    pendingAdminMessagesCountSensor = registerSensor("pending_admin_messages_count",
        new Gauge(() -> pendingAdminMessagesCountGauge), new Avg(), new Min(), new Max());
    storesWithPendingAdminMessagesCountSensor = registerSensor("stores_with_pending_admin_messages_count",
        new Gauge(() -> storesWithPendingAdminMessagesCountGauge), new Avg(), new Min(), new Max());
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
    pendingAdminMessagesCountSensor.record(value);
    this.pendingAdminMessagesCountGauge = value;
  }

  public void recordStoresWithPendingAdminMessagesCount(double value) {
    storesWithPendingAdminMessagesCountSensor.record(value);
    this.storesWithPendingAdminMessagesCountGauge = value;
  }

  public void setAdminConsumptionFailedOffset(long adminConsumptionFailedOffset) {
    this.adminConsumptionFailedOffset = adminConsumptionFailedOffset;
  }
}