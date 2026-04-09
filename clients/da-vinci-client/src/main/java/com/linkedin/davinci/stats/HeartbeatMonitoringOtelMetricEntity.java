package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HEARTBEAT_COMPONENT;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum HeartbeatMonitoringOtelMetricEntity implements ModuleMetricEntityInterface {
  HEARTBEAT_MONITORING_EXCEPTION_COUNT(
      "ingestion.heartbeat_monitoring.exception_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Number of exceptions caught in the heartbeat monitoring service threads",
      setOf(VENICE_CLUSTER_NAME, VENICE_HEARTBEAT_COMPONENT)
  ),
  HEARTBEAT_MONITORING_HEARTBEAT_COUNT(
      "ingestion.heartbeat_monitoring.heartbeat_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Liveness count for the heartbeat monitoring service threads",
      setOf(VENICE_CLUSTER_NAME, VENICE_HEARTBEAT_COMPONENT)
  ),
  HEARTBEAT_MONITORING_VERSION_NOT_FOUND_FOR_LAG_MONITOR_COUNT(
      "ingestion.heartbeat_monitoring.version_not_found_for_lag_monitor_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Number of times a version was not found in ZK when updating lag monitor for SET_FOLLOWER_MONITOR, "
          + "likely due to a race between version cleanup (ZK deletion) and SIT's asynchronous queue processing",
      setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  HeartbeatMonitoringOtelMetricEntity(
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
