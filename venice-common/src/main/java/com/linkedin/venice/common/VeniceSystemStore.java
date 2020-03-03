package com.linkedin.venice.common;

import com.linkedin.venice.meta.Store;

/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 */
public enum VeniceSystemStore {

  METADATA_STORE(String.format(Store.SYSTEM_STORE_FORMAT, "metadata_store"), true);

  private final String prefix;
  private final boolean isStoreZkShared;

  VeniceSystemStore(String prefix, boolean isStoreZkShared) {
    this.prefix = prefix;
    this.isStoreZkShared = isStoreZkShared;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isStoreZkShared() {
    return isStoreZkShared;
  }
}
