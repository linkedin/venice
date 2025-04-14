package com.linkedin.venice.meta;

public class StoreVersionInfo {
  private final Store store;
  private final Version version;

  public StoreVersionInfo(Store store, Version version) {
    this.store = store;
    this.version = version;
  }

  public Store getStore() {
    return store;
  }

  public Version getVersion() {
    return version;
  }
}
