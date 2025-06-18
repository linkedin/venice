package com.linkedin.venice.stats.dimensions;

public enum RepushStoreTriggerSource implements VeniceDimensionInterface {
  MANUAL, SCHEDULED;

  private final String triggerSource;

  RepushStoreTriggerSource() {
    this.triggerSource = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.REPUSH_STORE_TRIGGER_SOURCE;
  }

  @Override
  public String getDimensionValue() {
    return this.triggerSource;
  }
}
