package MetadataManager;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by clfung on 9/11/14.
 */
public class KeyCache {

    static final Logger logger = Logger.getLogger(KeyCache.class.getName());

    private static Map<String, Integer> keyCache = new HashMap<String, Integer>();

    public KeyCache() {

    }

    public synchronized static int getPartitionId(String key) {

        if (keyCache.containsKey(key)) {
            return keyCache.get(key);
        }

        return 1;
    }

    private synchronized static void registerNewKey(String key, int partitionId) {

        // already registered
        if (keyCache.containsKey(key)) {
            int currentId = keyCache.get(key);

            // value is the same, nothing to do here
            if (currentId != partitionId) {
                // we have a problem
                logger.error("Key conflict on: " + key);
            }

            return;
        }

        // add key to cache
        keyCache.put(key, partitionId);
    }

}
