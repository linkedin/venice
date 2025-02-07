package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.views.VeniceView;
import java.util.Optional;


/**
 * This repository provides read-only interface to access store configs for both regular Venice store names and view
 * store names. This adapter is only needed if the component is expected to work with view stores.
 */
public class HelixReadOnlyStoreViewConfigRepositoryAdapter implements ReadOnlyStoreConfigRepository, VeniceResource {
  private final HelixReadOnlyStoreConfigRepository storeConfigRepository;

  public HelixReadOnlyStoreViewConfigRepositoryAdapter(HelixReadOnlyStoreConfigRepository storeConfigRepository) {
    this.storeConfigRepository = storeConfigRepository;
  }

  @Override
  public void refresh() {
    storeConfigRepository.refresh();
  }

  @Override
  public void clear() {
    storeConfigRepository.clear();
  }

  @Override
  public Optional<StoreConfig> getStoreConfig(String storeName) {
    return storeConfigRepository.getStoreConfig(VeniceView.getStoreName(storeName));
  }

  @Override
  public StoreConfig getStoreConfigOrThrow(String storeName) {
    return storeConfigRepository.getStoreConfigOrThrow(VeniceView.getStoreName(storeName));
  }
}
