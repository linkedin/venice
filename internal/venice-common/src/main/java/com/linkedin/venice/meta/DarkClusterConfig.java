package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.ConfigKeys;
import java.util.ArrayList;
import java.util.List;


public class DarkClusterConfig {
  @JsonProperty(ConfigKeys.IS_DARK_CLUSTER)
  private boolean isDarkCluster = false;

  @JsonProperty(ConfigKeys.STORES_TO_REPLICATE)
  private List<String> storesToReplicate = new ArrayList<>();

  public DarkClusterConfig() {
  }

  public DarkClusterConfig(DarkClusterConfig clone) {
    if (clone.getStoresToReplicate() != null) {
      storesToReplicate = new ArrayList<>(clone.getStoresToReplicate());
    }
  }

  public boolean getIsDarkCluster() {
    return isDarkCluster;
  }

  public void setIsDarkCluster(boolean darkCluster) {
    isDarkCluster = darkCluster;
  }

  public List<String> getStoresToReplicate() {
    return storesToReplicate;
  }

  public void setStoresToReplicate(List<String> storesToReplicate) {
    this.storesToReplicate = storesToReplicate;
  }
}
