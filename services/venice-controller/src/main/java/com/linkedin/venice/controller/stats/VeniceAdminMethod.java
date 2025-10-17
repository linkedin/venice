package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


/**
 * This enum lists the methods in the {@link com.linkedin.venice.controller.VeniceParentHelixAdmin}
 * or {@link com.linkedin.venice.controller.VeniceHelixAdmin} class.
 */
public enum VeniceAdminMethod implements VeniceDimensionInterface {
  KILL_OFFLINE_PUSH("kill_offline_push"), INCREMENT_VERSION_IDEMPOTENT("increment_version_idempotent"),
  ROLL_FORWARD_TO_FUTURE_VERSION("roll_forward_to_future_version");

  final String category;

  VeniceAdminMethod(String category) {
    this.category = category;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD;
  }

  @Override
  public String getDimensionValue() {
    return category;
  }
}
