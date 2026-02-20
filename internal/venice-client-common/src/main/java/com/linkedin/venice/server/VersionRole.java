package com.linkedin.venice.server;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


/**
 * Role of a store's version: Backup/Current/Future.
 */
public enum VersionRole implements VeniceDimensionInterface {
  BACKUP, CURRENT, FUTURE;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_VERSION_ROLE;
  }
}
