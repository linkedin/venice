package com.linkedin.venice.meta;

/**
 * Interface of metadata repository to provide operations of stores and versions.
 */
public interface MetadataRepository {
    /**
     * Get one store by given name from repository.
     *
     * @param name name of wanted store.
     *
     * @return Store for given name.
     */
    public Store getStore(String name);

    /**
     * Update store in repository.
     *
     * @param store store need to be udpated.
     */
    public void updateStore(Store store);

    /**
     * Delete store from repository.
     *
     * @param name name of wantted store.
     */
    public void deleteStore(String name);

    /**
     * Add store into repository.
     *
     * @param store store need to be added.
     */
    public void addStore(Store store);
}
