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

  // This class is meant to be a singleton, and will be statically called
  public KeyCache() {

  }

  public synchronized static int getPartitionId(String key) {

    // key already exists in cache
    if (keyCache.containsKey(key)) {
      return keyCache.get(key);
    }

    // TODO: use the logic which determines which partition keys are going to
    return 1;

  }

  private synchronized static void registerNewKey(String key, int partitionId) {

    // already registered
    if (keyCache.containsKey(key)) {

      int currentId = keyCache.get(key);

      // value is the same, nothing to do here
      if (currentId != partitionId) {
        logger.error("Key conflict on: " + key);
      }

      return;
    }

    // add key to cache
    keyCache.put(key, partitionId);

  }

}
