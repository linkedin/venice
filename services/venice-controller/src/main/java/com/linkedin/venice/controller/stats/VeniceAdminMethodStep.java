package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


/**
 * This enum lists the steps involved in the {@link com.linkedin.venice.controller.VeniceParentHelixAdmin}
 * or {@link com.linkedin.venice.controller.VeniceHelixAdmin} methods.
 */
public enum VeniceAdminMethodStep implements VeniceDimensionInterface {
  /** The step to kill offline push */
  KILL_OFFLINE_PUSH("kill_offline_push"),

  /** The step of adding version and topic only */
  ADD_VERSION_AND_TOPIC_ONLY("add_version_and_topic_only"),

  /** The step to truncate kafka topic */
  TRUNCATE_KAFKA_TOPIC("truncate_kafka_topic"),

  /** The step includes get and update store */
  STORE_STATUS_UPDATE_TOTAL("store_status_update_total"),

  /** The step to update the repository store */
  REPOSITORY_STORE_STATUS_UPDATE("repository_store_status_update");

  final String category;

  VeniceAdminMethodStep(String category) {
    this.category = category;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD_STEP;
  }

  @Override
  public String getDimensionValue() {
    return category;
  }
}
