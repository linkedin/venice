package com.linkedin.venice.metadata;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by clfung on 9/11/14.
 */
public class KeyCache {

  static final Logger logger = Logger.getLogger(KeyCache.class.getName());

  private static KeyCache instance = null;
  private static Map<String, KeyAddress> keyMap; // mapping of key to node/partition

  // This class is meant to be a singleton, and will be statically called
  private KeyCache() {
    keyMap = new HashMap<String, KeyAddress>();
  }

  public static synchronized KeyCache getInstance() {

    if (null == instance) {
      instance = new KeyCache();
    }

    return instance;

  }

  public synchronized KeyAddress getKeyAddress(String key) {

    // key already exists in cache
    if (keyMap.containsKey(key)) {
      return keyMap.get(key);
    }

    // TODO: use the logic which determines which partition keys are going to
    return new KeyAddress(1, 1);

  }

  private synchronized void registerNewKey(String key, KeyAddress newAddress) {

    // already registered
    if (keyMap.containsKey(key)) {

      KeyAddress currentAddress = keyMap.get(key);

      // value is the same
      if (!currentAddress.equals(newAddress)) {
        logger.error("Key conflict on: " + key);
      }

      return;

    }

    // add key to cache
    keyMap.put(key, newAddress);

  }

}
