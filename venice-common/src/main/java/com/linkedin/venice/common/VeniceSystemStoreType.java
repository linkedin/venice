package com.linkedin.venice.common;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;


/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 */
public enum VeniceSystemStoreType {

  METADATA_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "metadata_store"), true,
      StoreMetadataKey.SCHEMA$.toString(), StoreMetadataValue.SCHEMA$.toString()),
  DAVINCI_PUSH_STATUS_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "davinci_push_status_store"), true,
      PushStatusKey.SCHEMA$.toString(), PushStatusValue.SCHEMA$.toString());

  private final String prefix;
  private final boolean isStoreZkShared;
  private final String keySchema;
  private final String valueSchema;

  VeniceSystemStoreType(String prefix, boolean isStoreZkShared, String keySchema, String valueSchema) {
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
