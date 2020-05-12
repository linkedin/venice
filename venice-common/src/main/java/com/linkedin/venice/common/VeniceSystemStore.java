package com.linkedin.venice.common;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;


/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 */
public enum VeniceSystemStore {

  METADATA_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "metadata_store"), true,
      StoreMetadataKey.SCHEMA$.toString(), StoreMetadataValue.SCHEMA$.toString());

  private final String prefix;
  private final boolean isStoreZkShared;
  private final String keySchema;
  private final String valueSchema;

  VeniceSystemStore(String prefix, boolean isStoreZkShared, String keySchema, String valueSchema) {
    this.prefix = prefix;
    this.isStoreZkShared = isStoreZkShared;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
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
}
