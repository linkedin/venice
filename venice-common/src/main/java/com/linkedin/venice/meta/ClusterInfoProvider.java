package com.linkedin.venice.meta;

import java.util.Set;


public interface ClusterInfoProvider {

  /**
   * Get the Venice clusters that are associated with the Venice component that holds this {@link kafka.cluster.Cluster}.
   * @return a {@link Set} of associated Venice cluster names.
   */
  Set<String> getAssociatedClusters();
}
