package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


public enum ConsumerPoolType implements VeniceDimensionInterface {
  REGULAR_POOL(""), // For other kinds of workload, and this pool type is also being used when using a single consumer
                    // pool.

  /**
   * The following pool types are being used when current version prioritization strategy is enabled.
   */
  CURRENT_VERSION_AA_WC_LEADER_POOL("_for_current_aa_wc_leader"),
  CURRENT_VERSION_SEP_RT_LEADER_POOL("_for_current_sep_rt_leader"),
  CURRENT_VERSION_NON_AA_WC_LEADER_POOL("_for_current_non_aa_wc_leader"),
  NON_CURRENT_VERSION_AA_WC_LEADER_POOL("_for_non_current_aa_wc_leader"),
  NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL("_for_non_current_non_aa_wc_leader");

  private final String statSuffix;

  ConsumerPoolType(String statSuffix) {
    this.statSuffix = statSuffix;
  }

  public String getStatSuffix() {
    return statSuffix;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_CONSUMER_POOL_TYPE;
  }
}
