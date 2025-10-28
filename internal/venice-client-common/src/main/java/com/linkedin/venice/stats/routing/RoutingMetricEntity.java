package com.linkedin.venice.stats.routing;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_GROUP_ID;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * Metric entities for routing-related metrics.
 */
public enum RoutingMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Count of available Helix groups for routing.
   */
  HELIX_GROUP_COUNT(
      "helix_group.count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Count of available Helix groups for routing", setOf(VENICE_STORE_NAME)
  ),

  /**
   * Count of requests routed to each Helix group.
   */
  HELIX_GROUP_CALL_COUNT(
      "helix_group.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of requests routed to each Helix group",
      setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)
  ),
  /**
   * Pending requests for each Helix group.
   */
  HELIX_GROUP_REQUEST_PENDING_REQUESTS(
      "helix_group.request.pending_requests", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Pending requests for each Helix group", setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)
  ),
  /**
   * Waiting time for responses from each Helix group.
   */
  HELIX_GROUP_CALL_TIME(
      "helix_group.call_time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
      "Waiting time for responses from each Helix group", setOf(VENICE_STORE_NAME, VENICE_HELIX_GROUP_ID)
  );

  private final MetricEntity entity;

  RoutingMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(name, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return entity;
  }
}
