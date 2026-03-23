package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_ERROR_TYPE;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_GLOBAL_RT_DIV_ERROR_TYPE} dimension,
 * representing the type of error in a Global RT DIV best-effort operation.
 *
 * <p>The Global RT DIV feature (leader-to-follower RT DIV state propagation) is best-effort:
 * errors in any phase are caught and logged rather than propagated. This dimension categorizes
 * which phase failed, enabling targeted alerting.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#GLOBAL_RT_DIV_ERROR_COUNT
 */
public enum VeniceGlobalRtDivErrorType implements VeniceDimensionInterface {
  /** Failed to produce the RT DIV snapshot message to the version topic */
  SEND,
  /** Failed to write the received RT DIV state to metadata storage */
  PERSIST,
  /** Failed to sync the latest consumed VT position into the OffsetRecord */
  VT_SYNC,
  /** Failed to delete RT DIV metadata during rollback or cleanup */
  DELETE;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_GLOBAL_RT_DIV_ERROR_TYPE;
  }
}
