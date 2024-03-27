package com.linkedin.davinci.client;

import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import java.util.function.Function;


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
   *
   * This feature will be completely removed in a later release.
   * Here are the reasons this feature doesn't work well:
   * 1. Remote Venice query is much slower than DaVinci local lookup.
   * 2. Typically, Venice Backend doesn't provision enough capacity for DaVinci use cases since we normally don't know
   *    the qps of DaVinci apps and for most of the scenarios, the DaVinci qps can be very high, and we don't want to
   *    pre-allocate a lot of backend resources for this feature since the remote resources won't be leveraged most of
   *    the time.
   */
  @Deprecated
  private NonLocalAccessPolicy nonLocalAccessPolicy = NonLocalAccessPolicy.FAIL_FAST;

  /**
   * Cache settings
   */
  private ObjectCacheConfig cacheConfig;

  /**
   * Record transformer reference
   */
  private Function<Integer, DaVinciRecordTransformer> recordTransformerFunction;

  /**
   * Whether to enable read-path metrics.
   * Metrics are expensive compared with the DaVinci performance and this feature should be disabled for
   * the high throughput use cases.
   */
  private boolean readMetricsEnabled = false;

  public DaVinciConfig() {
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig().setManaged(isManaged())
        .setIsolated(isIsolated())
        .setStorageClass(getStorageClass())
        .setNonLocalAccessPolicy(getNonLocalAccessPolicy())
        .setCacheConfig(getCacheConfig());
  }

  @Override
  public String toString() {
    return "DaVinciConfig{" + "managed=" + managed + ", isolated=" + isolated + ", storageClass=" + storageClass
        + ", nonLocalAccessPolicy=" + nonLocalAccessPolicy + ", cacheConfig=" + cacheConfig + "}";
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

  public boolean isCacheEnabled() {
    return cacheConfig != null;
  }

  public boolean isRecordTransformerEnabled() {
    return recordTransformerFunction != null;
  }

  public ObjectCacheConfig getCacheConfig() {
    return cacheConfig;
  }

  public DaVinciConfig setCacheConfig(ObjectCacheConfig cacheConfig) {
    this.cacheConfig = cacheConfig;
    return this;
  }

  public DaVinciRecordTransformer getRecordTransformer(Integer storeVersion) {
    if (recordTransformerFunction != null) {
      return recordTransformerFunction.apply(storeVersion);
    }
    return null;
  }

  public DaVinciConfig setRecordTransformerFunction(
      Function<Integer, DaVinciRecordTransformer> recordTransformerFunction) {
    this.recordTransformerFunction = recordTransformerFunction;
    return this;
  }

  public boolean isReadMetricsEnabled() {
    return readMetricsEnabled;
  }

  public void setReadMetricsEnabled(boolean readMetricsEnabled) {
    this.readMetricsEnabled = readMetricsEnabled;
  }
}
