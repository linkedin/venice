package com.linkedin.venice.client.store;

import java.io.Closeable;
import java.util.Set;


/**
 * Public interface for fetching Venice store metadata that is not tied to a specific store or cluster.
 * It is intended to provide metadata operations that span across all stores and clusters, as opposed to
 * {@link com.linkedin.venice.client.schema.StoreSchemaFetcher} which is scoped to a single store.
 */
public interface StoreMetadataFetcher extends Closeable {
  /**
   * Returns all store names available across all clusters.
   */
  Set<String> getAllStoreNames();
}
