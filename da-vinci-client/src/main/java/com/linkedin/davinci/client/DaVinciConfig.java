package com.linkedin.davinci.client;

import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;


public class DaVinciConfig {
  /**
   * Indicates whether client's local state is managed by Da Vinci or by application. The flag has no effect unless
   * the feature is enabled at the factory level by providing a set of required managed stores. Da Vinci automatically
   * removes local state of unused managed stores.
   */
  private boolean managed = true;

  /**
   * Indicates whether client is isolated from accessing partitions of other clients created for the same store.
   * It's application responsibility to ensure that subscription of isolated clients does not overlap, otherwise
   * isolated is not guaranteed since all such clients share the same {@link com.linkedin.davinci.StoreBackend}.
   */
  private boolean isolated = false;

  /**
   * Indicates what storage tier to use for client's local state.
   */
  private StorageClass storageClass = StorageClass.MEMORY_BACKED_BY_DISK;

  /**
   * Indicates how to handle access to not-subscribed partitions.
   */
  private NonLocalAccessPolicy nonLocalAccessPolicy = NonLocalAccessPolicy.FAIL_FAST;

  /**
   * Indicates total memory limit in bytes per store, where zero means no limit. The limit is best effort and its
   * precision greatly depends on granularity, the recommended limit granularity is 1GB. Da Vinci stalls ingestion
   * of new data when the limit is met.
   */
  private long memoryLimit = 0;

  /**
   * Cache settings
   */
  private ObjectCacheConfig cacheConfig;

  public DaVinciConfig() {
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig()
            .setManaged(isManaged())
            .setIsolated(isIsolated())
            .setStorageClass(getStorageClass())
            .setNonLocalAccessPolicy(getNonLocalAccessPolicy())
            .setMemoryLimit(getMemoryLimit())
            .setCacheConfig(getCacheConfig());
  }

  @Override
  public String toString() {
    return "DaVinciConfig{" +
               "managed=" + managed +
               ", isolated=" + isolated +
               ", storageClass=" + storageClass +
               ", nonLocalAccessPolicy=" + nonLocalAccessPolicy +
               ", memoryLimit=" + memoryLimit +
               ", cacheConfig=" + cacheConfig +
               "}";
  }

  public boolean isManaged() {
    return managed;
  }

  public DaVinciConfig setManaged(boolean managed) {
    this.managed = managed;
    return this;
  }

  public boolean isIsolated() {
    return isolated;
  }

  public DaVinciConfig setIsolated(boolean isolated) {
    this.isolated = isolated;
    return this;
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public DaVinciConfig setStorageClass(StorageClass storageClass) {
    this.storageClass = storageClass;
    return this;
  }

  public NonLocalAccessPolicy getNonLocalAccessPolicy() {
    return nonLocalAccessPolicy;
  }

  public DaVinciConfig setNonLocalAccessPolicy(NonLocalAccessPolicy nonLocalAccessPolicy) {
    this.nonLocalAccessPolicy = nonLocalAccessPolicy;
    return this;
  }

  public long getMemoryLimit() {
    return memoryLimit;
  }

  public DaVinciConfig setMemoryLimit(long memoryLimit) {
    this.memoryLimit = memoryLimit;
    return this;
  }

  public boolean isCacheEnabled() {
    return cacheConfig != null;
  }

  public ObjectCacheConfig getCacheConfig() {
    return cacheConfig;
  }

  public DaVinciConfig setCacheConfig(ObjectCacheConfig cacheConfig) {
    this.cacheConfig = cacheConfig;
    return this;
  }
}
