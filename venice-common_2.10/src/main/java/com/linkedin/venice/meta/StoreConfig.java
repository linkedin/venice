package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * Configurations of a store which are non-cluster specified.
 */
public class StoreConfig {
  // TODO we could add "clusterMigratingTo" later once we implement the feature that migration one store to another.
  public static final String CLUSTER_PROPERTY = "cluster";
  public static final String IS_DELETING_PROPERTY = "is_deleting";
  private String storeName;
  private Map<String, String> configs;

  public StoreConfig(String storeName) {
    this.storeName = storeName;
    configs = new HashMap<>();
  }

  public String getStoreName() {
    return storeName;
  }

  public boolean isDeleting() {
    if (configs.containsKey(IS_DELETING_PROPERTY)) {
      return Boolean.valueOf(configs.get(IS_DELETING_PROPERTY)).booleanValue();
    } else {
      return false;
    }
  }

  public void setDeleting(boolean isDeleting) {
    configs.put(IS_DELETING_PROPERTY, String.valueOf(isDeleting));
  }

  public String getCluster() {
    if (configs.containsKey(CLUSTER_PROPERTY)) {
      return configs.get(CLUSTER_PROPERTY);
    } else {
      throw new VeniceException(
          "Could not find property:" + CLUSTER_PROPERTY + " in the config of store: " + storeName);
    }
  }

  public void setCluster(String cluster) {
    configs.put(CLUSTER_PROPERTY, cluster);
  }
}


