package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.utils.AvroRecordUtils;


/**
 * Configurations of a store which are non-cluster specified.
 */
public class StoreConfig implements DataModelBackedStructure<StoreClusterConfig> {
  private final StoreClusterConfig storeClusterConfig;

  public StoreConfig(String storeName) {
    storeClusterConfig = AvroRecordUtils.prefillAvroRecordWithDefaultValue(new StoreClusterConfig());
    storeClusterConfig.storeName = storeName;
  }

  public StoreConfig(StoreClusterConfig storeClusterConfig) {
    this.storeClusterConfig = storeClusterConfig;
  }

  public String getStoreName() {
    return storeClusterConfig.storeName.toString();
  }

  public boolean isDeleting() {
    return storeClusterConfig.deleting;
  }

  public void setDeleting(boolean isDeleting) {
    storeClusterConfig.deleting = isDeleting;
  }

  public String getCluster() {
    if (storeClusterConfig.cluster.length() != 0) {
      return storeClusterConfig.cluster.toString();
    } else {
      throw new VeniceException(
          "Could not find cluster property in the config of store: " + storeClusterConfig.storeName.toString());
    }
  }

  public void setCluster(String cluster) {
    storeClusterConfig.cluster = cluster;
  }

  public String getMigrationSrcCluster() {
    return storeClusterConfig.migrationSrcCluster == null ? null : storeClusterConfig.migrationSrcCluster.toString(); // This
                                                                                                                      // will
                                                                                                                      // return
                                                                                                                      // null
                                                                                                                      // if
                                                                                                                      // does
                                                                                                                      // not
                                                                                                                      // exist
  }

  public void setMigrationSrcCluster(String srcClusterName) {
    storeClusterConfig.migrationSrcCluster = srcClusterName;
  }

  public String getMigrationDestCluster() {
    return storeClusterConfig.migrationDestCluster == null ? null : storeClusterConfig.migrationDestCluster.toString(); // This
                                                                                                                        // will
                                                                                                                        // return
                                                                                                                        // null
                                                                                                                        // if
                                                                                                                        // does
                                                                                                                        // not
                                                                                                                        // exist
  }

  public void setMigrationDestCluster(String destClusterName) {
    storeClusterConfig.migrationDestCluster = destClusterName;
  }

  @Override
  public StoreClusterConfig dataModel() {
    return storeClusterConfig;
  }

  public StoreConfig cloneStoreConfig() {
    StoreConfig clonedStoreConfig = new StoreConfig(storeClusterConfig.storeName.toString());
    clonedStoreConfig.setCluster(getCluster());
    clonedStoreConfig.setDeleting(isDeleting());
    clonedStoreConfig.setMigrationSrcCluster(getMigrationSrcCluster());
    clonedStoreConfig.setMigrationDestCluster(getMigrationDestCluster());
    return clonedStoreConfig;
  }
}
