package com.linkedin.venice.meta;

/**
 * Enums that lists most Venice user store types.
 */
public enum VeniceUserStoreType {
  /**
   * The data in batch-only stores will be populated by offline pushes/Samza reprocessing only.
   */
  BATCH_ONLY,

  /**
   * The data in hybrid stores will be populated by offline pushes/Samza reprocessing as well as Samza real-time updates.
   */
  HYBRID_ONLY,

  /**
   * The data in these stores are from full pushes and incremental pushes.
   */
  INCREMENTAL_PUSH,

  /**
   * These stores are either hybrid or incremental push enabled.
   */
  HYBRID_OR_INCREMENTAL,

  /**
   * These stores are system stores whose name starts with {@link Store#SYSTEM_STORE_NAME_PREFIX}.
   */
  SYSTEM,

  /**
   * All user stores in Venice, excluding system stores.
   */
  ALL;
}
