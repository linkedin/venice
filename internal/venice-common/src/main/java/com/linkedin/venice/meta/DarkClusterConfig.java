package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.ConfigKeys;
import java.util.ArrayList;
import java.util.List;


public class DarkClusterConfig {
  @JsonProperty(ConfigKeys.DARK_CLUSTER_TARGET_STORES)
  private List<String> targetStores = new ArrayList<>();

  public DarkClusterConfig() {
  }

  public DarkClusterConfig(DarkClusterConfig clone) {
    if (clone.getTargetStores() != null) {
      targetStores = new ArrayList<>(clone.getTargetStores());
    }
  }

  public List<String> getTargetStores() {
    return targetStores;
  }

  public void setTargetStores(List<String> targetStores) {
    this.targetStores = targetStores;
  }
}
