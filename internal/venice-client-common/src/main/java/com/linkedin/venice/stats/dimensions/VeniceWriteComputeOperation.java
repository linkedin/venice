package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_WRITE_COMPUTE_OPERATION;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_WRITE_COMPUTE_OPERATION} dimension, representing
 * the phase of a write compute (partial update) operation during ingestion.
 *
 * <p>Write compute allows clients to send partial updates (e.g., list add/remove, map put/delete) instead
 * of full record replacements. Processing a write compute message involves two phases:
 * <ol>
 *   <li><b>Query</b> — Looking up the existing record value from the storage engine to use as the base
 *       for applying the partial update.</li>
 *   <li><b>Update</b> — Applying the write compute operations (e.g., collection merges) to produce the
 *       final updated record.</li>
 * </ol>
 *
 * <p>Used by the {@code ingestion.write_compute.time} metric to break down write compute latency by phase.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#WRITE_COMPUTE_TIME
 */
public enum VeniceWriteComputeOperation implements VeniceDimensionInterface {
  /** The lookup phase: reading the existing record value from storage to use as the base for the update */
  QUERY,
  /** The update phase: applying write compute operations to the base record to produce the final value */
  UPDATE;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_WRITE_COMPUTE_OPERATION;
  }
}
