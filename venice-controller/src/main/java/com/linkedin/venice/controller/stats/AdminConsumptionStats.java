package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


public class AdminConsumptionStats extends AbstractVeniceStats {
  final private Sensor adminConsumeFailCountSensor;
  final private Sensor adminTopicDIVErrorReportCountSensor;

  public AdminConsumptionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    adminConsumeFailCountSensor = registerSensor("failed_admin_messages", new Max());
    adminTopicDIVErrorReportCountSensor = registerSensor("admin_message_div_error_report_count", new Count());
  }

  /**
   * @param retryCount the number of times that a failure has consecutively triggered a retry
   */
  public void recordFailedAdminConsumption(double retryCount) {
    adminConsumeFailCountSensor.record(retryCount);
  }

  public void recordAdminTopicDIVErrorReportCount() {
    adminTopicDIVErrorReportCountSensor.record();
  }
}