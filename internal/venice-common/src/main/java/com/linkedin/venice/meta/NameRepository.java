package com.linkedin.venice.meta;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;


/**
 * A repository of {@link StoreName} and {@link StoreVersionName}, which are intended to be used as shared instances.
 */
public class NameRepository {
  public static final int DEFAULT_MAXIMUM_ENTRY_COUNT = 2000;
  private final LoadingCache<String, StoreName> storeNameCache;
  private final LoadingCache<String, StoreVersionName> storeVersionNameCache;

  public NameRepository() {
    this(DEFAULT_MAXIMUM_ENTRY_COUNT);
  }

  public NameRepository(int maxEntryCount) {
    this.storeNameCache = Caffeine.newBuilder().maximumSize(maxEntryCount).build(StoreName::new);
    this.storeVersionNameCache = Caffeine.newBuilder().maximumSize(maxEntryCount).build(storeVersionNameString -> {
      String storeNameString = Version.parseStoreFromKafkaTopicName(storeVersionNameString);
      StoreName storeName = getStoreName(storeNameString);
      return new StoreVersionName(storeVersionNameString, storeName);
    });
  }

  public StoreName getStoreName(String storeName) {
    return this.storeNameCache.get(storeName);
  }

  public StoreVersionName getStoreVersionName(String storeVersionName) {
    return this.storeVersionNameCache.get(storeVersionName);
  }

  public StoreVersionName getStoreVersionName(String storeName, int versionNumber) {
    return getStoreVersionName(Version.composeKafkaTopic(storeName, versionNumber));
  }
}
