package com.linkedin.venice.stats.dimensions;

public enum LogCompactionSelectionReason implements VeniceDimensionInterface {
  TIME_SINCE_LAST_VERSION_CREATION_EXCEEDS_THRESHOLD;

  private final String reason;

  LogCompactionSelectionReason() {
    this.reason = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.LOG_COMPACTION_SELECTION_REASON;
  }

  @Override
  public String getDimensionValue() {
    return this.reason;
  }
}
