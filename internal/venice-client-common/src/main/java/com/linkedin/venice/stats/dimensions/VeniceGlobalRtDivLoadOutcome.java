package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_LOAD_OUTCOME;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_GLOBAL_RT_DIV_LOAD_OUTCOME} dimension,
 * representing the outcome of loading Global RT DIV state during F→L leader promotion.
 *
 * <p>FOUND means state was on disk and the leader resumes from a saved checkpoint. NOT_FOUND means
 * nothing usable was read, which is expected for the first leader ever elected for the partition but
 * also covers a failed read; {@code error_count} with type {@code LOAD} distinguishes the latter.
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#GLOBAL_RT_DIV_LOAD_COUNT
 */
public enum VeniceGlobalRtDivLoadOutcome implements VeniceDimensionInterface {
  /** State found on disk; leader resumes from saved checkpoint position */
  FOUND,
  /** No usable state read from disk; leader starts from EARLIEST */
  NOT_FOUND;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_GLOBAL_RT_DIV_LOAD_OUTCOME;
  }
}
