package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum FastClientMetricEntity implements ModuleMetricEntityInterface {
  /**
   * Count of retry requests where the retry "won" (outperformed the original).
   */
  RETRY_REQUEST_WIN_COUNT(
      "retry.request.win_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of retry requests which won",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
  ),

  /**
   * Metadata staleness watermark reported asynchronously in milliseconds.
   */
  METADATA_STALENESS_DURATION(
      "metadata.staleness_duration", MetricType.ASYNC_GAUGE, MetricUnit.MILLISECOND,
      "High watermark of metadata staleness in ms", setOf(VENICE_STORE_NAME)
  ),

  /**
   * Fanout size distribution for requests, with fanout type as a dimension.
   */
  REQUEST_FANOUT_COUNT(
      "request.fanout_count", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER, "Fanout size for requests",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_FANOUT_TYPE)
  ),

  /**
   * Count of requests rejected by the client.
   */
  REQUEST_REJECTION_COUNT(
      "request.rejection_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of requests rejected by the client",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_REJECTION_REASON)
  ),

  /**
   * Ratio of requests rejected by the client to total requests.
   * TODO: Evaluate if this metric can be implemented as a derived OTel metric and if so, migrate to that.
   */
  REQUEST_REJECTION_RATIO(
      "request.rejection_ratio", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Ratio of requests rejected by the client to total requests",
      setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_REJECTION_REASON)
  );

  private final MetricEntity entity;

  FastClientMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.entity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
  }

  FastClientMetricEntity(
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
