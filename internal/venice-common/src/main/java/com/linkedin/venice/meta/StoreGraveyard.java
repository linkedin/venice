package com.linkedin.venice.meta;

import java.util.List;
import org.apache.zookeeper.data.Stat;


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

  /**
   * Put the given store into graveyard. If the store has already existed in the graveyard, update it by this given
   * store.
   */
  void putStoreIntoGraveyard(String clusterName, Store store);

  /**
   * Get store from the graveyard in the specified cluster.
   */
  Store getStoreFromGraveyard(String clusterName, String storeName, Stat stat);

  /**
   * Remove the given store from graveyard in the specified cluster.
   */
  void removeStoreFromGraveyard(String clusterName, String storeName);

  /**
   * List store names from graveyard in the specified cluster.
   */
  List<String> listStoreNamesFromGraveyard(String clusterName);
}
