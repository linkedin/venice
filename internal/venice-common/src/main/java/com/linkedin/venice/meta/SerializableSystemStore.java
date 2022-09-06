package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;


public class SerializableSystemStore {
  private final ZKStore zkSharedStore;
  private final VeniceSystemStoreType systemStoreType;
  private final ZKStore veniceStore;

  public SerializableSystemStore(
      final ZKStore zkSharedStore,
      final VeniceSystemStoreType systemStoreType,
      final ZKStore veniceStore) {
    this.zkSharedStore = (ZKStore) zkSharedStore;
    this.systemStoreType = systemStoreType;
    this.veniceStore = (ZKStore) veniceStore;
  }

  public Store getZkSharedStore() {
    return zkSharedStore;
  }

  public VeniceSystemStoreType getSystemStoreType() {
    return systemStoreType;
  }

  public Store getVeniceStore() {
    return veniceStore;
  }
}
