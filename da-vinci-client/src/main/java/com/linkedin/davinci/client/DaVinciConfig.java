package com.linkedin.davinci.client;


public class DaVinciConfig {
  private StorageClass storageClass = StorageClass.DISK_BACKED_MEMORY;
  private RemoteReadPolicy remoteReadPolicy = RemoteReadPolicy.FAIL_FAST;

  public DaVinciConfig() {
  }

  protected DaVinciConfig(StorageClass storageClass, RemoteReadPolicy remoteReadPolicy) {
    this.storageClass = storageClass;
    this.remoteReadPolicy = remoteReadPolicy;
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig(storageClass, remoteReadPolicy);
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
}
