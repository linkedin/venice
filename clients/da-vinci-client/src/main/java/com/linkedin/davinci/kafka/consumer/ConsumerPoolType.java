package com.linkedin.davinci.kafka.consumer;

public enum ConsumerPoolType {
  /**
   * The following pool types are being used when the dedicated AA/WC leader consumer pool feature is enabled.
   */
  AA_WC_LEADER_POOL, // For AA/WC leader
  REGULAR_POOL, // For other kinds of workload except AA/WC leader

  /**
   * The followings pool types are being used when current version prioritization strategy is enabled.
   * Eventually, this new strategy will replace the above one once it is fully validated and ramped.
   */
  CURRENT_VERSION_AA_WC_LEADER_POOL, CURRENT_VERSION_NON_AA_WC_LEADER_POOL, NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
  NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL
}
