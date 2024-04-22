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

  /**
   * When the request key count exceeds the following threshold, it will be split into multiple small
   * chunks with max size to be the threshold and these chunks will be executed concurrently in a
   * pre-allocated thread pool.
   */
  private int largeBatchRequestSplitThreshold = AvroGenericDaVinciClient.DEFAULT_CHUNK_SPLIT_THRESHOLD;

  public DaVinciConfig() {
  }

  public DaVinciConfig clone() {
    return new DaVinciConfig().setManaged(isManaged())
        .setIsolated(isIsolated())
        .setStorageClass(getStorageClass())
        .setCacheConfig(getCacheConfig());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DaVinciConfig{managed=")
        .append(managed)
        .append(", isolated=")
        .append(isolated)
        .append(", storageClass=")
        .append(storageClass)
        .append(", cacheConfig=")
        .append(cacheConfig)
        .append(", largeBatchRequestSplitThreshold=")
        .append(largeBatchRequestSplitThreshold)
        .append("}");
    return sb.toString();
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

  public DaVinciConfig setReadMetricsEnabled(boolean readMetricsEnabled) {
    this.readMetricsEnabled = readMetricsEnabled;
    return this;
  }

  public int getLargeBatchRequestSplitThreshold() {
    return largeBatchRequestSplitThreshold;
  }

  public DaVinciConfig setLargeBatchRequestSplitThreshold(int largeBatchRequestSplitThreshold) {
    if (largeBatchRequestSplitThreshold < 1) {
      throw new IllegalArgumentException("'largeBatchRequestSplitThreshold' param needs to be at least 1");
    }
    this.largeBatchRequestSplitThreshold = largeBatchRequestSplitThreshold;
    return this;
  }
}
