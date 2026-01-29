package com.linkedin.venice.stats;

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


public enum RetryManagerMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Rate limit for retry operations (tokens per second)
   */
  RETRY_RATE_LIMIT_TARGET_TOKENS(
      "retry.rate_limit.target_tokens", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Rate limit for retry operations (tokens per second)",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),

  /**
   * Number of remaining retry operations in the current time window.
   */
  RETRY_RATE_LIMIT_REMAINING_TOKENS(
      "retry.rate_limit.remaining_tokens", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Number of remaining retry operations in the current time window",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  ),

  /**
   * Number of rejected retry operations.
   */
  RETRY_RATE_LIMIT_REJECTION_COUNT(
      "retry.rate_limit.rejection_count", MetricType.COUNTER, MetricUnit.NUMBER, "Number of rejected retry operations",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)
  );

  private final MetricEntity entity;

  RetryManagerMetricEntity(
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
