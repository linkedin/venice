package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class AdminConsumptionStats extends AbstractVeniceStats {
  final private Sensor adminConsumeFailCountSensor;
  final private Sensor adminTopicDIVErrorReportCountSensor;

  public AdminConsumptionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    adminConsumeFailCountSensor = registerSensor("failed_admin_messages", new Count());
    adminTopicDIVErrorReportCountSensor = registerSensor("admin_message_div_error_report_count", new Count());
  }

  /**
   * @param retryCount the number of times that a failure has consecutively triggered a retry
   */

  /**
   * Record the number of failed admin messages in the past one minute;
   * if controller keeps retrying the admin messages, this metric will keep growing;
   * this metric will be reset to 0 once the blocked admin message is processed.
   */
  public void recordFailedAdminConsumption() {
    adminConsumeFailCountSensor.record();
  }

  public void recordAdminTopicDIVErrorReportCount() {
    adminTopicDIVErrorReportCountSensor.record();
  }
}