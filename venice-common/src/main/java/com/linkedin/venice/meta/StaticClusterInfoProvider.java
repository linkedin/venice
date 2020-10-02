package com.linkedin.venice.meta;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class StaticClusterInfoProvider implements ClusterInfoProvider {

  private final Set<String> clusterNames;

  public StaticClusterInfoProvider(Set<String> clusterNames) {
    this.clusterNames = Collections.unmodifiableSet(clusterNames);
  }

  @Override
  public Set<String> getAssociatedClusters() {
    return clusterNames;
  }
}
