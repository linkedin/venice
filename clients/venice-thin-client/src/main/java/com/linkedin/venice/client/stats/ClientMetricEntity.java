package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum ClientMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Client retry count.
   */
  RETRY_CALL_COUNT(
      "retry.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all retry requests for client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),

  /**
   * Client retry key count.
   */
  RETRY_REQUEST_KEY_COUNT(
      "retry.request_key_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Key count of retry requests for client", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)
  ),

  /**
   * Client retry response key count.
   */
  RETRY_RESPONSE_KEY_COUNT(
      "retry.response_key_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Key count of retry responses for client", setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)
  );

  private final MetricEntity entity;

  ClientMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
  }

  ClientMetricEntity(
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
