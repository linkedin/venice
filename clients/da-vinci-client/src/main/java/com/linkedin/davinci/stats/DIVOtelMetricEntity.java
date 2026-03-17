package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_RESULT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_SEVERITY;
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
 * OTel metric entity definitions for Data Integrity Validation (DIV) stats.
 * Consolidates 8 Tehuti AsyncGauge sensors into 4 OTel counter metrics.
 */
public enum DIVOtelMetricEntity implements ModuleMetricEntityInterface {
  // Recorded per-message on the leader ingestion path — uses async counter for high throughput.
  MESSAGE_COUNT(
      "ingestion.div.message.count", MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES, MetricUnit.NUMBER,
      "Count of DIV message validation results",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DIV_RESULT)
  ),

  OFFSET_REWIND_COUNT(
      "ingestion.div.offset.rewind.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of leader offset rewind events",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DIV_SEVERITY)
  ),

  PRODUCER_FAILURE_COUNT(
      "ingestion.div.producer.failure.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of leader producer failures", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  // Not mutually exclusive with PRODUCER_FAILURE_COUNT — a single produce operation can increment both counters.
  BENIGN_PRODUCER_FAILURE_COUNT(
      "ingestion.div.producer.failure.benign_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of benign leader producer failures", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  );

  private final MetricEntity metricEntity;

  DIVOtelMetricEntity(
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
