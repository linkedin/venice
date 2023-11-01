package com.linkedin.venice.fastclient.meta;

/**
 * Modes that control how fast client will fetch store metadata
 */
public enum StoreMetadataFetchMode {
  /**
   * Fast-client will make API calls to storage-nodes to get metadata information
   */
  SERVER_BASED_METADATA
}
