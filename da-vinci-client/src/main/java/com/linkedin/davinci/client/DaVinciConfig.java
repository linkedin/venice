package com.linkedin.davinci.client;


public class DaVinciConfig {
  private StorageClass storageClass = StorageClass.DISK_BACKED_MEMORY;
  private RemoteReadPolicy remoteReadPolicy = RemoteReadPolicy.FAIL_FAST;
  private VersionSwapPolicy versionSwapPolicy = VersionSwapPolicy.PER_PROCESS;

  public DaVinciConfig() {
  }

  public DaVinciConfig(
      StorageClass storageClass,
      RemoteReadPolicy remoteReadPolicy,
      VersionSwapPolicy versionSwapPolicy) {
    this.storageClass = storageClass;
    this.versionSwapPolicy = versionSwapPolicy;
    this.remoteReadPolicy = remoteReadPolicy;
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig(storageClass, remoteReadPolicy, versionSwapPolicy);
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(StorageClass storageClass) {
    this.storageClass = storageClass;
  }

  public VersionSwapPolicy getVersionSwapPolicy() {
    return this.versionSwapPolicy;
  }

  public RemoteReadPolicy getRemoteReadPolicy() {
    return this.remoteReadPolicy;
  }

  public DaVinciConfig setRemoteReadPolicy(RemoteReadPolicy remoteReadPolicy) {
    this.remoteReadPolicy = remoteReadPolicy;
    return this;
  }

  public DaVinciConfig setVersionSwapPolicy(VersionSwapPolicy versionSwapPolicy) {
    this.versionSwapPolicy = versionSwapPolicy;
    return this;
  }
}
