package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONNECTION_SOURCE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link com.linkedin.venice.stats.ServerConnectionStats}.
 */
public enum ServerConnectionOtelMetricEntity implements ModuleMetricEntityInterface {
  CONNECTION_ACTIVE_COUNT(
      "connection.active_count", MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER,
      "Active connection count by source (router or client)", setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)
  ),

  CONNECTION_REQUEST_COUNT(
      "connection.request_count", MetricType.COUNTER, MetricUnit.NUMBER, "Connection establishment requests by source",
      setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)
  ),

  CONNECTION_SETUP_TIME(
      "connection.setup_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "SSL handshake setup latency from channel init to completion",
      setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)
  );

  private final MetricEntity metricEntity;

  ServerConnectionOtelMetricEntity(
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
