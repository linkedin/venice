package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_FALLBACK_REASON;
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
 *   <li>Kafka fallbacks → 1 COUNTER with a {@code blob_transfer.fallback_reason} dimension</li>
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
      "Count of attempted blob transfers by source and status. Hosts rejected by the pre-transfer connectability "
          + "check are never attempted and so are not counted here",
      setOf(
          VENICE_STORE_NAME,
          VENICE_CLUSTER_NAME,
          VENICE_VERSION_ROLE,
          VENICE_BLOB_TRANSFER_SOURCE,
          VENICE_RESPONSE_STATUS_CODE_CATEGORY)
  ),

  KAFKA_FALLBACK_COUNT(
      "ingestion.blob_transfer.kafka_fallback.count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of replicas that fell back to Kafka bootstrapping instead of blob transfer, by reason",
      setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_BLOB_TRANSFER_FALLBACK_REASON)
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
