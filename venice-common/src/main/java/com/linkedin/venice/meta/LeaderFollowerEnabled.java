package com.linkedin.venice.meta;

/**
 * Enums the cases in which LeaderFollower is enabled by default
 */
public enum LeaderFollowerEnabled {
  /**
   * Leader follower is not enabled for either new stores or stores
   * migrated to hybrid or incremental
   */
  NONE,

  /**
   * Leader follower is only enabled for stores that are migrated to hybrid
   */
  HYBRID_ONLY,

  /**
   * Leader follower is enabled for stores that are migrated to hybrid
   * or incremental
   */
  HYBRID_OR_INCREMENTAL,

  /**
   * Leader follow is enabled for all stores
   */
  ALL;
}