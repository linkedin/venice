package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


/**
 * Configurations of a store which are non-cluster specified.
 */
public class StoreConfig {
  public static final String CLUSTER_PROPERTY = "cluster";
  public static final String IS_DELETING_PROPERTY = "is_deleting";
  public static final String MIGRATION_SRC_CLUSTER = "migration_src_cluster";
  public static final String MIGRATION_DEST_CLUSTER = "migration_dest_cluster";
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

  public String getMigrationSrcCluster() {
    return configs.get(MIGRATION_SRC_CLUSTER);  // This will return null if key does not exist
  }

  public void setMigrationSrcCluster(String srcClusterName) {
    configs.put(MIGRATION_SRC_CLUSTER, srcClusterName);
  }

  public String getMigrationDestCluster() {
    return configs.get(MIGRATION_DEST_CLUSTER); // This will return null if key does not exist
  }

  public void setMigrationDestCluster(String destClusterName) {
    configs.put(MIGRATION_DEST_CLUSTER, destClusterName);
  }
}