package com.linkedin.davinci.store.cache.backend;

import java.util.Optional;


public class ObjectCacheConfig {
  private Optional<Long> maxCacheSize = Optional.empty();
  private Optional<Long> ttlInMilliseconds = Optional.empty();

  public ObjectCacheConfig setMaxPerPartitionCacheSize(Long maxPerPartitionCacheSize) {
    this.maxCacheSize = Optional.of(maxPerPartitionCacheSize);
    return this;
  }

  public ObjectCacheConfig setTtlInMilliseconds(Long ttlInMilliseconds) {
    this.ttlInMilliseconds = Optional.of(ttlInMilliseconds);
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
    if (o == null) {
      return false;
    }

    if (o == this) {
      return true;
    }
    if (!(o instanceof ObjectCacheConfig)) {
      return false;
    }
    ObjectCacheConfig c = (ObjectCacheConfig) o;
    if (!this.getTtlInMilliseconds().orElse(-1L).equals(c.getTtlInMilliseconds().orElse(-1L))) {
      return false;
    }
    if (!this.getMaxCacheSize().orElse(-1L).equals(c.getMaxCacheSize().orElse(-1L))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + maxCacheSize.hashCode();
    result = result * 31 + ttlInMilliseconds.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ObjectCacheConfig{" + "maxCacheSize=" + maxCacheSize + ", ttlInMilliseconds=" + ttlInMilliseconds + "}";
  }
}
