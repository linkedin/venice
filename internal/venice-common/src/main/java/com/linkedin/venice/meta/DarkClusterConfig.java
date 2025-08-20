package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.ConfigKeys;
import java.util.HashSet;
import java.util.Set;


public class DarkClusterConfig {
  @JsonProperty(ConfigKeys.DARK_CLUSTER_TARGET_STORES)
  private Set<String> targetStores = new HashSet<>();

  public DarkClusterConfig() {
  }

  public DarkClusterConfig(DarkClusterConfig clone) {
    if (clone.getTargetStores() != null) {
      targetStores = new HashSet<>(clone.getTargetStores());
    }
  }

  public Set<String> getTargetStores() {
    return targetStores;
  }

  public void setTargetStores(Set<String> targetStores) {
    this.targetStores = targetStores;
  }

  // Add more fields and methods as needed for dark cluster specific config
}
