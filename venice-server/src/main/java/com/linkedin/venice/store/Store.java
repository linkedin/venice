package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.utils.ByteArray;

/**
 *     The basic interface used for storage. Allows the usual CRUD operations.
 *
 */

public interface Store {

    /**
     * @return The name of the Store
     */
    public String getName();

    /**
     * Get the value associated with the given key
     *
     * @param key  The key to check for
     * @return  The value associated with the key or an empty list if no values are found.
     * @throws VeniceStorageException
     */
    public byte[] get(ByteArray key) throws VeniceStorageException;

    /**
     * Associate the value with the key in this store
     *
     * TODO: Partial put should be implemented by layers above storage and they should invoke a put on the storage with the whole key and value
     *
     * @param key
     * @param value
     * @throws VeniceStorageException
     */
    public void put(ByteArray key, byte[] value) throws VeniceStorageException;

    /**
     * Delete entry corresponding to the given key
     *
     * @param key  The key to delete
     * @param value  The current value of the key
     * @return True if anything was deleted
     * @throws VeniceStorageException
     */
    public boolean delete(ByteArray key, byte[] value) throws VeniceStorageException;


    /**
     * Close the store.
     *
     * @throws VeniceStorageException If closing fails.
     */
    public void close() throws VeniceStorageException;
}
