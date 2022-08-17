package com.linkedin.venice.meta;

public interface ClusterInfoProvider {
  /**
   * Get the associated Venice cluster name given a Venice store name.
   * @return the cluster name that the store belongs to or null if such information cannot be provided.
   */
  String getVeniceCluster(String storeName);
}
