package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/** OTel metric entities for participant store consumption task operations. */
public enum ParticipantStoreConsumptionOtelMetricEntity implements ModuleMetricEntityInterface {
  KILL_PUSH_JOB_LATENCY(
      "participant_store.consumption_task.push_job.kill.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency from kill signal generation in the child controller to kill execution on the storage node",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
  ),
  KILL_PUSH_JOB_COUNT(
      "participant_store.consumption_task.push_job.kill.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of push job kill attempts on the storage node",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT(
      "participant_store.consumption_task.consumption.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of exceptions thrown during participant store consumption for kill push job records",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
  ),
  FAILED_INITIALIZATION_COUNT(
      "participant_store.consumption_task.initialization.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of participant store consumption task initialization failures", setOf(VENICE_CLUSTER_NAME)
  ),
  HEARTBEAT_COUNT(
      "participant_store.consumption_task.heartbeat.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Heartbeat count for the participant store consumption task", setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  ParticipantStoreConsumptionOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
