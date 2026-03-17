package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entities for server read quota usage stats.
 *
 * <p>Consolidates Tehuti sensors into 3 OTel metrics:
 * <ul>
 *   <li>{@link #READ_QUOTA_REQUEST_COUNT} — high-perf counter with outcome and version role dimensions</li>
 *   <li>{@link #READ_QUOTA_KEY_COUNT} — high-perf counter with outcome and version role dimensions</li>
 *   <li>{@link #READ_QUOTA_USAGE_RATIO} — async double gauge (unchanged)</li>
 * </ul>
 */
public enum ServerReadQuotaOtelMetricEntity implements ModuleMetricEntityInterface {
  READ_QUOTA_REQUEST_COUNT(
      "read.quota.request.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of read quota requests per outcome and version role",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_QUOTA_REQUEST_OUTCOME)
  ),
  READ_QUOTA_KEY_COUNT(
      "read.quota.key.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of read quota keys (RCU) per outcome and version role",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_QUOTA_REQUEST_OUTCOME)
  ),
  READ_QUOTA_USAGE_RATIO(
      "read.quota.usage_ratio", MetricType.ASYNC_DOUBLE_GAUGE, MetricUnit.RATIO,
      "Ratio of read quota used, based on requested keys per second relative to the node's quota responsibility",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  ServerReadQuotaOtelMetricEntity(
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
