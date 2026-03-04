package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_OPERATION;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_DCR_OPERATION} dimension, representing the type of
 * deterministic conflict resolution (DCR) merge operation performed during active-active replication.
 *
 * <p>When a record update arrives at an active-active enabled store, the ingestion pipeline must merge
 * the incoming update with the existing record using DCR logic. This dimension captures which type of
 * merge operation was executed, enabling latency and throughput analysis per operation type.
 *
 * <p>Used by the {@code ingestion.dcr.merge.time} metric to break down DCR merge latency by operation type.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#DCR_MERGE_TIME
 */
public enum VeniceDCROperation implements VeniceDimensionInterface {
  /** A put (full record write) merge operation was performed during conflict resolution */
  PUT,
  /** An update (partial update / write compute) merge operation was performed during conflict resolution */
  UPDATE,
  /** A delete merge operation was performed during conflict resolution */
  DELETE;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_DCR_OPERATION;
  }
}
