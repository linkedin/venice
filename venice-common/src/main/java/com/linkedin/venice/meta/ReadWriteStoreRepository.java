package com.linkedin.venice.meta;

/**
 * Interface defined read and write operations to access stores.
 */
public interface ReadWriteStoreRepository extends ReadOnlyStoreRepository {
  /**
   * Update store in repository.
   *
   * @param store store need to be udpated.
   */
  void updateStore(Store store);

  /**
   * Delete store from repository.
   *
   * @param name name of wantted store.
   */
  void deleteStore(String name);

  /**
   * Add store into repository.
   *
   * @param store store need to be added.
   */
  void addStore(Store store);
}
