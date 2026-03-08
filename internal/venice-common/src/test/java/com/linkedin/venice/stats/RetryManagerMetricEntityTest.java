package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class RetryManagerMetricEntityTest extends AbstractModuleMetricEntityTest<RetryManagerMetricEntity> {
  public RetryManagerMetricEntityTest() {
    super(RetryManagerMetricEntity.class);
  }

  @Override
  protected Map<RetryManagerMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RetryManagerMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        RetryManagerMetricEntity.RETRY_RATE_LIMIT_TARGET_TOKENS,
        new MetricEntityExpectation(
            "retry.rate_limit.target_tokens",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Rate limit for retry operations (tokens per second)",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    map.put(
        RetryManagerMetricEntity.RETRY_RATE_LIMIT_REMAINING_TOKENS,
        new MetricEntityExpectation(
            "retry.rate_limit.remaining_tokens",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Number of remaining retry operations in the current time window",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    map.put(
        RetryManagerMetricEntity.RETRY_RATE_LIMIT_REJECTION_COUNT,
        new MetricEntityExpectation(
            "retry.rate_limit.rejection_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Number of rejected retry operations",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    return map;
  }
}
