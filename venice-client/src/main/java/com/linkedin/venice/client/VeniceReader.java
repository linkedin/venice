package com.linkedin.venice.client;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.serialization.Serializer;
import org.apache.log4j.Logger;

/**
 * Class which acts as the primary reader API
 */
public class VeniceReader<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceReader.class.getName());

  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  public VeniceReader(Serializer<K> keySerializer, Serializer<V> valueSerializer) {

    // TODO: Deprecate/refactor the config. It's really not needed for the most part
    try {
      GlobalConfiguration.initializeFromFile("./config/config.properties");
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (Exception e) {
      logger.error("Error while starting up configuration for VeniceReader.");
      logger.error(e);
      System.exit(1);
    }
  }

  /**
   * Execute a standard "get" on the key. Returns null if empty.
   * @param key - The key to look for in storage.
   * @return The result of the "Get" operation
   * */
  public V get(K key) {
    // TODO: not implemented yet
    throw new UnsupportedOperationException("Get operation is not ready.");
  }
}
