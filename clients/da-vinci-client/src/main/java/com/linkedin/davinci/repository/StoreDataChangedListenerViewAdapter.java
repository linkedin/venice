package com.linkedin.davinci.repository;

import com.linkedin.venice.meta.ReadOnlyViewStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;


/**
 * Adapter that provides store data changed listener interface for the store and any subscribed view stores.
 */
public class StoreDataChangedListenerViewAdapter implements StoreDataChangedListener {
  private final StoreDataChangedListener storeDataChangedListener;
  private final SubscribedViewStoreProvider viewStoreProvider;

  /**
   * We need an external provider to provide us the set of subscribed view stores because the passed in Store object
   * after data changed may or may not include all the view stores that were subscribed and expecting the store change
   * event. e.g. A view that's being deprecated via new version pushes.
   */
  public StoreDataChangedListenerViewAdapter(
      StoreDataChangedListener storeDataChangedListener,
      SubscribedViewStoreProvider viewStoreProvider) {
    this.storeDataChangedListener = storeDataChangedListener;
    this.viewStoreProvider = viewStoreProvider;
  }

  @Override
  public void handleStoreCreated(Store store) {
    storeDataChangedListener.handleStoreCreated(store);
    for (String viewStore: viewStoreProvider.getSubscribedViewStores(store.getName())) {
      storeDataChangedListener.handleStoreCreated(new ReadOnlyViewStore(store, viewStore));
    }
  }

  @Override
  public void handleStoreDeleted(Store store) {
    storeDataChangedListener.handleStoreDeleted(store);
    for (String viewStore: viewStoreProvider.getSubscribedViewStores(store.getName())) {
      storeDataChangedListener.handleStoreDeleted(new ReadOnlyViewStore(store, viewStore));
    }
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    storeDataChangedListener.handleStoreDeleted(storeName);
    for (String viewStore: viewStoreProvider.getSubscribedViewStores(storeName)) {
      storeDataChangedListener.handleStoreDeleted(viewStore);
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
    storeDataChangedListener.handleStoreChanged(store);
    for (String viewStore: viewStoreProvider.getSubscribedViewStores(store.getName())) {
      storeDataChangedListener.handleStoreChanged(new ReadOnlyViewStore(store, viewStore));
    }
  }
}
