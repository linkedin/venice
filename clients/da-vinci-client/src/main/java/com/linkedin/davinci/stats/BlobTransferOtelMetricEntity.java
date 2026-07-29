package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
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
 * OTel metric entities for blob transfer operations.
 *
 * <p>Maps blob transfer telemetry into OTel metrics:
 * <ul>
 *   <li>source-specific request outcomes → 1 COUNTER with source and response-status dimensions</li>
 *   <li>Kafka fallbacks → 2 COUNTERS for no candidates and failed hosts</li>
 *   <li>3 count sensors (total/success/fail) → 1 COUNTER with {@code response_status_category} dimension</li>
 *   <li>throughput gauge → dropped (derivable as rate from {@code bytes.received})</li>
 *   <li>time gauge → 1 HISTOGRAM in seconds</li>
 *   <li>bytes received rate gauge → 1 COUNTER in bytes</li>
 *   <li>bytes sent rate gauge → 1 COUNTER in bytes</li>
 * </ul>
 */
public enum BlobTransferOtelMetricEntity implements ModuleMetricEntityInterface {
  REQUEST_COUNT(
      "ingestion.blob_transfer.request.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of blob transfer requests by source and status",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_VERSION_ROLE,
          VENICE_BLOB_TRANSFER_SOURCE,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  KAFKA_FALLBACK_NO_CANDIDATES_COUNT(
      "ingestion.blob_transfer.kafka_fallback.no_candidates.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of Kafka fallbacks because no blob candidates were discovered",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  KAFKA_FALLBACK_FAILED_HOSTS_COUNT(
      "ingestion.blob_transfer.kafka_fallback.failed_hosts.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of Kafka fallbacks after all usable blob hosts failed",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  RESPONSE_COUNT(
      "ingestion.blob_transfer.response.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of blob transfer responses by status (success/fail)",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  TIME(
      "ingestion.blob_transfer.time", MetricType.HISTOGRAM, MetricUnit.SECOND, "Blob transfer time in seconds",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BYTES_RECEIVED(
      "ingestion.blob_transfer.bytes.received", MetricType.COUNTER, MetricUnit.BYTES,
      "Bytes received via blob transfer", setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  ),

  BYTES_SENT(
      "ingestion.blob_transfer.bytes.sent", MetricType.COUNTER, MetricUnit.BYTES, "Bytes sent via blob transfer",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)
  );

  private final MetricEntity metricEntity;

  BlobTransferOtelMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(name, metricType, unit, description, dimensionsList);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
