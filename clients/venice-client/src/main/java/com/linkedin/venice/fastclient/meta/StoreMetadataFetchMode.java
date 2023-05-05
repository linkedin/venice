package com.linkedin.venice.fastclient.meta;

/**
 * Modes that control how fast client will fetch store metadata
 */
public enum StoreMetadataFetchMode {
  /**
   * Use thin-client to query meta system store via routers
   */
  THIN_CLIENT_BASED_METADATA,

  /**
   * Use da-vinci-client to query the locally materialized meta system store.
   * Note: Using da-vinci-client and fast-client with {@link DA_VINCI_CLIENT_BASED_METADATA} has a cyclic dependency
   * issue, so only one of DaVinci or FC with DaVinci can be used.
   */
  DA_VINCI_CLIENT_BASED_METADATA,

  /**
   * Fast-client will make API calls to storage-nodes to get metadata information
   */
  SERVER_BASED_METADATA
}
