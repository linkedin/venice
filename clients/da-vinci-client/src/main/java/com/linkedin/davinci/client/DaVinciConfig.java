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
   * Cache settings
   */
  private ObjectCacheConfig cacheConfig;

  /**
   * Record transformer reference
   */
  private DaVinciRecordTransformerConfig recordTransformerConfig;

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

  /**
   * Determines whether to enable request-based metadata retrieval directly from the Venice Server.
   * By default, metadata is retrieved from a system store via a thin client.
   */
  private boolean useRequestBasedMetaRepository = false;

  /**
   * Disables RocksDB block cache for this store only. Beneficial for sequential access patterns
   * where data is read once (e.g., sequential scans). Prevents cache pollution of the shared
   * block cache used by other DaVinci stores and reduces memory pressure. Not recommended for
   * random access or repeated reads. Default: false.
   */
  private boolean disableBlockCache = false;

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
        .append(", disableBlockCache=")
        .append(disableBlockCache)
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
    return recordTransformerConfig != null;
  }

  public ObjectCacheConfig getCacheConfig() {
    return cacheConfig;
  }

  public DaVinciConfig setCacheConfig(ObjectCacheConfig cacheConfig) {
    this.cacheConfig = cacheConfig;
    return this;
  }

  public DaVinciConfig setRecordTransformerConfig(DaVinciRecordTransformerConfig recordTransformerConfig) {
    this.recordTransformerConfig = recordTransformerConfig;
    return this;
  }

  public DaVinciRecordTransformerConfig getRecordTransformerConfig() {
    return recordTransformerConfig;
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

  public boolean isUseRequestBasedMetaRepository() {
    return useRequestBasedMetaRepository;
  }

  public DaVinciConfig setUseRequestBasedMetaRepository(boolean useRequestBasedMetaRepository) {
    this.useRequestBasedMetaRepository = useRequestBasedMetaRepository;
    return this;
  }

  public boolean isDisableBlockCache() {
    return disableBlockCache;
  }

  public DaVinciConfig setDisableBlockCache(boolean disableBlockCache) {
    this.disableBlockCache = disableBlockCache;
    return this;
  }
}
