package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


public enum ControllerMetricEntity implements ModuleMetricEntityInterface {
  /** Count of current in flight messages to AdminSparkServer */
  INFLIGHT_CALL_COUNT(
      MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER, "Count of all current inflight calls to controller spark server",
      setOf(VENICE_CLUSTER_NAME, VENICE_CONTROLLER_ENDPOINT)
  ),
  /** Count of completed calls to AdminSparkServer */
  CALL_COUNT(
      MetricType.COUNTER, MetricUnit.NUMBER, "Count of all calls to controller spark server",
      setOf(
          VENICE_CLUSTER_NAME,
          VENICE_CONTROLLER_ENDPOINT,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  /** Histogram of call latency to AdminSparkServer */
  CALL_TIME(
      MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Latency histogram of all successful calls to controller spark server",
      setOf(
          VENICE_CLUSTER_NAME,
          VENICE_CONTROLLER_ENDPOINT,
          HTTP_RESPONSE_STATUS_CODE,
          HTTP_RESPONSE_STATUS_CODE_CATEGORY,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),
  /** Count of all requests to repush a store */
  STORE_REPUSH_CALL_COUNT(
      "store.repush.call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all requests to repush a store",
      setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME, STORE_REPUSH_TRIGGER_SOURCE)
  ),

  /** log compaction related metrics */

  /** Count of stores nominated for scheduled compaction */
  STORE_COMPACTION_NOMINATED_COUNT(
      "store.compaction.nominated_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of stores nominated for scheduled compaction", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),

  /**
   * Track the state from the time a store is nominated for compaction to the
   * time the repush is completed to finish compaction. stays 1 after nomination
   * and becomes 0 when the compaction is compacted
   */
  STORE_COMPACTION_ELIGIBLE_STATE(
      "store.compaction.eligible_state", MetricType.GAUGE, MetricUnit.NUMBER,
      "Track the state from the time a store is nominated for compaction to the time the repush is completed",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)
  ),

  /** Count of log compaction repush triggered for a store after it becomes eligible */
  STORE_COMPACTION_TRIGGERED_COUNT(
      "store.compaction.triggered_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of log compaction repush triggered for a store after it becomes eligible",
      setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME)
  ),

  /** PushJobStatusStats: Push job completions */
  PUSH_JOB_COUNT(
      "push_job.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Push job completions, differentiated by push type and status",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_STATUS)
  ),

  /** TopicCleanupServiceStats: Gauge of topics currently eligible for deletion */
  TOPIC_CLEANUP_DELETABLE_COUNT(
      "topic_cleanup_service.topic.deletable_count", MetricType.GAUGE, MetricUnit.NUMBER,
      "Count of topics currently eligible for deletion"
  ),
  /** TopicCleanupServiceStats: Count of topic deletion operations (success and failure) */
  TOPIC_CLEANUP_DELETED_COUNT(
      "topic_cleanup_service.topic.deleted_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of topic deletion operations", setOf(VENICE_RESPONSE_STATUS_CODE_CATEGORY)
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

  ControllerMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricName = metricName;
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
  }

  /** Constructor for metrics with no dimensions, to be used only with {@link com.linkedin.venice.stats.metrics.MetricEntityStateBase} */
  ControllerMetricEntity(String metricName, MetricType metricType, MetricUnit unit, String description) {
    this.metricName = metricName;
    this.metricEntity = MetricEntity.createWithNoDimensions(metricName, metricType, unit, description);
  }

  @VisibleForTesting
  public String getMetricName() {
    return metricName;
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
