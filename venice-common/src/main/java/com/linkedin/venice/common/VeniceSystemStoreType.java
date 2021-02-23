package com.linkedin.venice.common;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;


/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 */
public enum VeniceSystemStoreType {

  METADATA_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "metadata_store"), true,
      StoreMetadataKey.SCHEMA$.toString(), StoreMetadataValue.SCHEMA$.toString(), "", false),
  DAVINCI_PUSH_STATUS_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "davinci_push_status_store"), true,
      PushStatusKey.SCHEMA$.toString(), PushStatusValue.SCHEMA$.toString(), "", false),

  // New Metadata system store
  META_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "meta_store"), true, StoreMetaKey.SCHEMA$.toString(),
      StoreMetaValue.SCHEMA$.toString(), AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName(), true);

  private final String prefix;
  private final boolean isStoreZkShared;
  private final String keySchema;
  private final String valueSchema;
  private final String zkSharedStoreName;
  /**
   * Whether this specific type has adopted the new metadata repositories, such as
   * {@link com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository}
   * {@link com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter}
   * {@link com.linkedin.venice.helix.HelixReadWriteStoreRepositoryAdapter}
   *
   * When some specific system store type adopts the new repository arch, it needs to follow the following design pattern:
   * 1. Having the zk shared system store only created in system store cluster.
   * 2. This specific store type needs to onboard {@link com.linkedin.venice.meta.SystemStore} structure, which will
   *    extract the common store property from its zk shared system store and distinct properties from the corresponding
   *    regular venice store structure (such as version related metadata).
   */
  private final boolean newMedataRepositoryAdopted;


  VeniceSystemStoreType(String prefix, boolean isStoreZkShared, String keySchema, String valueSchema, String zkSharedStoreName,
      boolean newMedataRepositoryAdopted) {
    this.prefix = prefix;
    this.isStoreZkShared = isStoreZkShared;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    if (zkSharedStoreName.isEmpty()) {
      this.zkSharedStoreName = this.getPrefix();
    } else {
      this.zkSharedStoreName = zkSharedStoreName;
    }
    this.newMedataRepositoryAdopted = newMedataRepositoryAdopted;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isStoreZkShared() {
    return isStoreZkShared;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public String getValueSchema() {
    return valueSchema;
  }

  public String getZkSharedStoreName() {
    return zkSharedStoreName;
  }

  public String getZkSharedStoreNameInCluster(String clusterName) {
    return isNewMedataRepositoryAdopted() ? zkSharedStoreName : zkSharedStoreName + VeniceSystemStoreUtils.SEPARATOR + clusterName;
  }

  public boolean isNewMedataRepositoryAdopted() {
    return newMedataRepositoryAdopted;
  }

  /**
   * This function is to compose a system store name according to the current system store type and the regular store name.
   * @param regularStoreName
   * @return
   */
  public String getSystemStoreName(String regularStoreName) {
    return getPrefix() + VeniceSystemStoreUtils.SEPARATOR + regularStoreName;
  }

  /**
   * This function is used to check whether the passed store name belongs to the current system store type.
   * @param storeName
   * @return
   */
  public boolean isSystemStore(String storeName) {
    return storeName.startsWith(getPrefix() + VeniceSystemStoreUtils.SEPARATOR);
  }

  /**
   * This function is to extract the regular store name from the system store name.
   * @param systemStoreName
   * @return
   */
  public String extractRegularStoreName(String systemStoreName) {
    if (isSystemStore(systemStoreName)) {
      return systemStoreName.substring((getPrefix() + VeniceSystemStoreUtils.SEPARATOR).length());
    }
    throw new IllegalArgumentException("Invalid system store name: " + systemStoreName + " for system store type: " + name());
  }

  public static VeniceSystemStoreType getSystemStoreType(String storeName) {
    for (VeniceSystemStoreType systemStoreType : values()) {
      if (storeName.startsWith(systemStoreType.getPrefix() + VeniceSystemStoreUtils.SEPARATOR)) {
        return systemStoreType;
      }
    }
    return null;
  }
}
