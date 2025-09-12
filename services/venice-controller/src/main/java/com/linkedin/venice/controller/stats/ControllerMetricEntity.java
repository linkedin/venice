package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_REQUEST_URL;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.REPUSH_TRIGGER_SOURCE;
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


public enum ControllerMetricEntity implements ModuleMetricEntityInterface {
  SPARK_SERVER_REQUESTS_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_FINISHED_REQUESTS_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all finished calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_SUCCESSFUL_REQUESTS_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all successful calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_FAILED_REQUESTS_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all failed calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all current inflight calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_SUCCESSFUL_REQUESTS_LATENCY(
      MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency histogram of all successful calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  SPARK_SERVER_FAILED_REQUESTS_LATENCY(
      MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Latency histogram of all failed calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, HTTP_REQUEST_URL)
  ),
  REPUSH_CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all repush request calls to a controller endpoint",
      setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME, REPUSH_TRIGGER_SOURCE)
  ),
  COMPACTION_ELIGIBLE_STATE(
      MetricType.GAUGE, MetricUnit.NUMBER,
      "This metric indicates the duration from when a store is first nominated for compaction until the store is compacted successfully."
          + " When a store is nominated for scheduled compaction and remains uncompacted, this metric will be at 1."
          + " When the store is compacted, this metric will return to 0.",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),
  STORE_NOMINATED_FOR_COMPACTION_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of stores nominated for scheduled compaction",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;
  private final String metricName;

  ControllerMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricName = this.name().toLowerCase();
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
  }

  public String getMetricName() {
    return metricName;
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
