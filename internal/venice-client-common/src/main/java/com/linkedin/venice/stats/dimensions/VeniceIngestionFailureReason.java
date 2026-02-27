package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_FAILURE_REASON;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_INGESTION_FAILURE_REASON} dimension, representing
 * the categorized reason for an ingestion task failure.
 *
 * <p>Ingestion tasks can fail for various reasons during the consumption and processing of data from
 * Kafka topics. This dimension categorizes the failure reason, enabling operators to quickly identify
 * the root cause of ingestion issues through metric dashboards and alerts.
 *
 * <p>Used by the {@code ingestion.failure.count} metric to count ingestion failures by reason.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#INGESTION_FAILURE_COUNT
 */
public enum VeniceIngestionFailureReason implements VeniceDimensionInterface {
  /** The ingestion task was explicitly killed (e.g., during version swap or shutdown) */
  TASK_KILLED,
  /** A checksum verification failure was detected on ingested data, indicating data corruption */
  CHECKSUM_VERIFICATION_FAILURE,
  /** The ingestion task timed out while waiting for bootstrap (initial data load) to complete */
  BOOTSTRAP_TIMEOUT,
  /** The push job timed out before all data could be ingested */
  PUSH_TIMEOUT,
  /** A remote Kafka broker in another region was unreachable during cross-region replication */
  REMOTE_BROKER_UNREACHABLE,
  /** A general/uncategorized ingestion failure that does not match any specific reason above */
  GENERAL;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INGESTION_FAILURE_REASON;
  }
}
