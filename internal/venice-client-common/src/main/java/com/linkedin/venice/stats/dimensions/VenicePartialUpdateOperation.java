package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_OPERATION_PHASE;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_PARTIAL_UPDATE_OPERATION_PHASE} dimension, representing
 * the phase of a partial update (write compute) operation during ingestion.
 *
 * <p>Partial update allows clients to send partial updates (e.g., list add/remove, map put/delete) instead
 * of full record replacements. Processing a partial update message involves two phases:
 * <ol>
 *   <li><b>Query</b> — Looking up the existing record value from the storage engine to use as the base
 *       for applying the partial update.</li>
 *   <li><b>Update</b> — Applying the partial update operations (e.g., collection merges) to produce the
 *       final updated record.</li>
 * </ol>
 *
 * <p>Used by the {@code ingestion.partial_update.time} metric to break down partial update latency by phase.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#PARTIAL_UPDATE_TIME
 */
public enum VenicePartialUpdateOperation implements VeniceDimensionInterface {
  /** The lookup phase: reading the existing record value from storage to use as the base for the update */
  QUERY,
  /** The update phase: applying partial update operations to the base record to produce the final value */
  UPDATE;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_PARTIAL_UPDATE_OPERATION_PHASE;
  }
}
