package com.linkedin.venice.meta;

import java.util.Optional;


/**
 * Interface defined the way to retrieve the store config from a repository.
 */
public interface ReadOnlyStoreConfigRepository {
  Optional<StoreConfig> getStoreConfig(String storeName);

  StoreConfig getStoreConfigOrThrow(String storeName);
}
