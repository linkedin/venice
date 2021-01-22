package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;


/**
 * Configurations of a store which are non-cluster specified.
 */
public class StoreConfig implements DataModelBackedStructure<StoreClusterConfig> {
  private final String storeName;
  private final StoreClusterConfig storeClusterConfig;

  public StoreConfig(String storeName) {
    this.storeName = storeName;
    storeClusterConfig = Store.prefillAvroRecordWithDefaultValue(new StoreClusterConfig());
    storeClusterConfig.storeName = storeName;
  }

  public String getStoreName() {
    return storeName;
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
          "Could not find cluster property in the config of store: " + storeName);
    }
  }

  public void setCluster(String cluster) {
    storeClusterConfig.cluster = cluster;
  }

  public String getMigrationSrcCluster() {
    return storeClusterConfig.migrationSrcCluster == null ? null
        : storeClusterConfig.migrationSrcCluster.toString();  // This will return null if does not exist
  }

  public void setMigrationSrcCluster(String srcClusterName) {
    storeClusterConfig.migrationSrcCluster = srcClusterName;
  }

  public String getMigrationDestCluster() {
    return storeClusterConfig.migrationDestCluster == null ? null
        : storeClusterConfig.migrationDestCluster.toString(); // This will return null if does not exist
  }

  public void setMigrationDestCluster(String destClusterName) {
    storeClusterConfig.migrationDestCluster = destClusterName;
  }

  @Override
  public StoreClusterConfig dataModel() {
    return storeClusterConfig;
  }

  public StoreConfig cloneStoreConfig() {
    StoreConfig clonedStoreConfig = new StoreConfig(storeName);
    clonedStoreConfig.setCluster(getCluster());
    clonedStoreConfig.setDeleting(isDeleting());
    clonedStoreConfig.setMigrationSrcCluster(getMigrationSrcCluster());
    clonedStoreConfig.setMigrationDestCluster(getMigrationDestCluster());
    return clonedStoreConfig;
  }
}