package storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for managing the storage system and its partitions
 * Created by clfung on 9/10/14.
 */
public class Store {

    Map<Integer, StorePartition> partitions;

    /**
     * Constructor for the Store
     */
    public Store() {
        partitions = new HashMap<Integer, StorePartition>();
    }

    public void addPartition(int store_id) {

        if (partitions.containsKey(store_id)) {
            //logger.error();
        }

        partitions.put(store_id, new StorePartition(store_id));

    }

    public Object get(int store_id, String key) {
        if (!partitions.containsKey(store_id))
            // partition not found
            return null;

        return partitions.get(store_id).get(key);
    }

}
