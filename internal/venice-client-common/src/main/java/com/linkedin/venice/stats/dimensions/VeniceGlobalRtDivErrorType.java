package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_ERROR_TYPE;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_GLOBAL_RT_DIV_ERROR_TYPE} dimension,
 * representing the phase of a Global RT DIV operation that failed.
 *
 * <p>The Global RT DIV feature propagates RT DIV state from the leader to followers. Several of its
 * phases are best-effort: errors are caught and logged rather than propagated. This dimension
 * categorizes which phase failed, enabling targeted alerting.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#GLOBAL_RT_DIV_ERROR_COUNT
 */
public enum VeniceGlobalRtDivErrorType implements VeniceDimensionInterface {
  /** Failed to serialize or compress the RT DIV snapshot before producing it to the version topic */
  SEND,
  /** Failed to write the received RT DIV state to metadata storage */
  PERSIST,
  /** Failed to sync the latest consumed VT position into the OffsetRecord */
  VT_SYNC,
  /** Failed to delete RT DIV metadata during chunk cleanup */
  DELETE,
  /** Failed to read or deserialize persisted RT DIV state on F→L leader promotion */
  LOAD;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_GLOBAL_RT_DIV_ERROR_TYPE;
  }
}
