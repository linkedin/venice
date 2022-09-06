package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collections;
import java.util.Set;


public class StaticClusterInfoProvider implements ClusterInfoProvider {
  private final Set<String> clusterNames;

  // The current implementation of the StaticClusterInfoProvider only accepts a single cluster. This can be changed in
  // the future if needed.
  public StaticClusterInfoProvider(Set<String> clusterNames) {
    if (clusterNames.size() != 1) {
      throw new VeniceException(
          "Invalid clusterNames provided: " + clusterNames.toString() + ". "
              + StaticClusterInfoProvider.class.getSimpleName() + " can only accept a single cluster name");
    }
    this.clusterNames = Collections.unmodifiableSet(clusterNames);
  }

  @Override
  public String getVeniceCluster(String storeName) {
    return clusterNames.iterator().next();
  }
}
