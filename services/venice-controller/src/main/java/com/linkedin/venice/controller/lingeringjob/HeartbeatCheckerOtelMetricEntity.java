package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;


/** OTel metric entity definitions for batch job heartbeat checking. Used by {@link HeartbeatBasedCheckerStats}. */
public enum HeartbeatCheckerOtelMetricEntity implements ModuleMetricEntityInterface {
  BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT(
      "batch_job_heartbeat.check_failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Failed heartbeat check operations"
  ),
  BATCH_JOB_HEARTBEAT_TIMEOUT_COUNT(
      "batch_job_heartbeat.timeout_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Batch jobs timed out based on heartbeat"
  ),
  BATCH_JOB_HEARTBEAT_ACTIVE_COUNT(
      "batch_job_heartbeat.active_count", MetricType.COUNTER, MetricUnit.NUMBER, "Batch jobs with valid heartbeat"
  );

  private final MetricEntity metricEntity;

  HeartbeatCheckerOtelMetricEntity(String metricName, MetricType metricType, MetricUnit unit, String description) {
    this.metricEntity = MetricEntity.createWithNoDimensions(metricName, metricType, unit, description);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
