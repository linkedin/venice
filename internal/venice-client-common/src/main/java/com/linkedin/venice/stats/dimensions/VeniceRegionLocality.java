package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;


/**
 * Dimension to represent whether a region is local or remote.
 */
public enum VeniceRegionLocality implements VeniceDimensionInterface {
  /** The region is the same as the current server's region */
  LOCAL,
  /** The region is different from the current server's region */
  REMOTE;

  private final String locality;

  VeniceRegionLocality() {
    this.locality = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_REGION_LOCALITY;
  }

  @Override
  public String getDimensionValue() {
    return locality;
  }
}
