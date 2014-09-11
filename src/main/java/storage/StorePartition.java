package storage;

import java.util.HashMap;
import java.util.Map;

/**
 * A generalized class for a storage partition
 * Created by clfung on 9/10/14.
 */
public class StorePartition {

    private Map<String, Object> storage;
    private int store_id;

    /**
     * Constructor
     * */
    public StorePartition(int store_id) {

        // initialize empty storage unit
        storage = new HashMap<String, Object>();
        this.store_id = store_id;

    }

    /**
     * Returns the id of this given partition
     * */
    public int getId() {
        return store_id;
    }

    /**
     * Puts a value into the key value store
     * */
    public void put(String key, Object payload) {
        storage.put(key, payload);
    }


    /**
     * Gets a value from the key value store
     * */
    public Object get(String key) {
        if (storage.containsKey(key))
            return storage.get(key);

        return null;
    }


    /**
     * Deletes a value from the key value store
     * */
    public void delete(String key) {
        storage.remove(key);
    }

}


