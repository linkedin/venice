package com.linkedin.venice.stats.dimensions;

/**
 * Role of a store's version: Backup/Current/Future.
 */
public enum VersionRole implements VeniceDimensionInterface {
  BACKUP, CURRENT, FUTURE;

  private final String versionRole;

  VersionRole() {
    this.versionRole = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_VERSION_ROLE;
  }

  @Override
  public String getDimensionValue() {
    return this.versionRole;
  }
}
