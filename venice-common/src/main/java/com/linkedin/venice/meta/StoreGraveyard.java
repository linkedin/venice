package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;


/**
 * The graveyard used to keep all deleted stores. While re-creating a store, Venice could retrieve the important info
 * like largest used version number to avoid resource conflict caused by the same name.
 * <p>
 * Like the store repository, each cluster should have its own store graveyard instance.
 */
public interface StoreGraveyard {
  /**
   * Retrieve the largest used version number by the given store name from graveyard. Return 0 if the store dose not
   * exist in the graveyard, which is the default value we used for the new store.
   */
  int getLargestUsedVersionNumber(String storeName);

  int getSystemStoreLargestUsedVersionNumber(String systemStoreName);

  /**
   * Put the given store into grave yard. If the store has already existed in the grave yard, update it by this given
   * store.
   */
  void putStoreIntoGraveyard(String clusterNaem, Store store);
}
