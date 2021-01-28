package com.linkedin.davinci.client;


public class DaVinciConfig {
  private boolean isManaged = true;
  private StorageClass storageClass = StorageClass.DISK_BACKED_MEMORY;
  private RemoteReadPolicy remoteReadPolicy = RemoteReadPolicy.FAIL_FAST;
  private long memoryLimit = 0; // 0 means unlimited memory

  /**
   * If true, ingestion will freeze once the partition is ready to serve,
   * or ingestion will not start if local data exists.
   */
  private boolean suppressLiveUpdates = false;


  public DaVinciConfig() {
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig()
               .setManaged(isManaged())
               .setStorageClass(getStorageClass())
               .setRemoteReadPolicy(getRemoteReadPolicy())
               .setMemoryLimit(getMemoryLimit())
               .setSuppressLiveUpdates(isSuppressingLiveUpdates());
  }

  public boolean isManaged() {
    return isManaged;
  }

  public DaVinciConfig setManaged(boolean isManaged) {
    this.isManaged = isManaged;
    return this;
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public DaVinciConfig setStorageClass(StorageClass storageClass) {
    this.storageClass = storageClass;
    return this;
  }

  public RemoteReadPolicy getRemoteReadPolicy() {
    return remoteReadPolicy;
  }

  public DaVinciConfig setRemoteReadPolicy(RemoteReadPolicy remoteReadPolicy) {
    this.remoteReadPolicy = remoteReadPolicy;
    return this;
  }

  public long getMemoryLimit() {
    return memoryLimit;
  }

  public DaVinciConfig setMemoryLimit(long memoryLimit) {
    this.memoryLimit = memoryLimit;
    return this;
  }

  public boolean isSuppressingLiveUpdates() {
    return suppressLiveUpdates;
  }

  /**
   * Freeze ingestion if ready to serve or local data exists
   */
  public DaVinciConfig setSuppressLiveUpdates(boolean suppressLiveUpdates) {
    this.suppressLiveUpdates = suppressLiveUpdates;
    return this;
  }
}
