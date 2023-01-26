package com.linkedin.davinci.store.cache.backend;

import com.linkedin.davinci.client.CacheValueTransformer;
import java.util.Optional;


public class ObjectCacheConfig {
  private Optional<Long> maxCacheSize = Optional.empty();
  private Optional<Long> ttlInMilliseconds = Optional.empty();
  private Optional<CacheValueTransformer> valueTransformer = Optional.empty();

  public ObjectCacheConfig setMaxPerPartitionCacheSize(Long maxPerPartitionCacheSize) {
    this.maxCacheSize = Optional.of(maxPerPartitionCacheSize);
    return this;
  }

  public ObjectCacheConfig setTtlInMilliseconds(Long ttlInMilliseconds) {
    this.ttlInMilliseconds = Optional.of(ttlInMilliseconds);
    return this;
  }

  public ObjectCacheConfig setValueTransformer(CacheValueTransformer transformer) {
    this.valueTransformer = Optional.of(transformer);
    return this;
  }

  public Optional<Long> getMaxCacheSize() {
    return maxCacheSize;
  }

  public Optional<Long> getTtlInMilliseconds() {
    return ttlInMilliseconds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectCacheConfig that = (ObjectCacheConfig) o;
    return maxCacheSize.equals(that.maxCacheSize) && ttlInMilliseconds.equals(that.ttlInMilliseconds)
        && valueTransformer.equals(that.valueTransformer);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + maxCacheSize.hashCode();
    result = result * 31 + ttlInMilliseconds.hashCode();
    result = result * 31 + valueTransformer.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ObjectCacheConfig{" + "maxCacheSize=" + maxCacheSize + ", ttlInMilliseconds=" + ttlInMilliseconds + "}";
  }
}
