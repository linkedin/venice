package com.linkedin.venice.stats.dimensions;

public enum StoreRepushTriggerSource implements VeniceDimensionInterface {
  MANUAL, SCHEDULED_FOR_LOG_COMPACTION;

  private final String triggerSource;

  StoreRepushTriggerSource() {
    this.triggerSource = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
  }

  @Override
  public String getDimensionValue() {
    return this.triggerSource;
  }
}
