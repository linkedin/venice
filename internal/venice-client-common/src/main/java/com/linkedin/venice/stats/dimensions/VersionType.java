package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent the version type for versioned stats.
 */
public enum VersionType implements VeniceDimensionInterface {
  CURRENT, FUTURE, BACKUP;

  private final String versionType;

  VersionType() {
    this.versionType = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_VERSION_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return this.versionType;
  }
}
