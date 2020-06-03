package com.linkedin.davinci.client;


public class DaVinciConfig {
  private StorageClass storageClass = StorageClass.DISK_BACKED_MEMORY;
  private RemoteReadPolicy remoteReadPolicy = RemoteReadPolicy.FAIL_FAST;
  private long rocksDBMemoryLimit = 0; // 0 means unlimited memory

  public DaVinciConfig() {
  }

  public DaVinciConfig(
      StorageClass storageClass,
      RemoteReadPolicy remoteReadPolicy,
      long rocksDBMemoryLimit) {
    this.storageClass = storageClass;
    this.remoteReadPolicy = remoteReadPolicy;
    this.rocksDBMemoryLimit = rocksDBMemoryLimit;
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig(storageClass, remoteReadPolicy, rocksDBMemoryLimit);
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public DaVinciConfig setStorageClass(StorageClass storageClass) {
    this.storageClass = storageClass;
    return this;
  }

  public RemoteReadPolicy getRemoteReadPolicy() {
    return this.remoteReadPolicy;
  }

  public DaVinciConfig setRemoteReadPolicy(RemoteReadPolicy remoteReadPolicy) {
    this.remoteReadPolicy = remoteReadPolicy;
    return this;
  }

  public long getRocksDBMemoryLimit() {
    return rocksDBMemoryLimit;
  }

  public void setRocksDBMemoryLimit(long rocksDBMemoryLimit) {
    this.rocksDBMemoryLimit = rocksDBMemoryLimit;
  }
}
