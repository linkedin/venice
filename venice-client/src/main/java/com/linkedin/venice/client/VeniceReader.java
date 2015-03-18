package com.linkedin.venice.client;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.Serializer;
import com.linkedin.venice.utils.Props;
import org.apache.log4j.Logger;

/**
 * Class which acts as the primary reader API
 */
public class VeniceReader<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceReader.class.getName());

  private Props props;
  private final String kafkaBrokerUrl;
  private final String storeName;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public VeniceReader(Props props, String storeName, Serializer<K> keySerializer, Serializer<V> valueSerializer) {

    // TODO: Deprecate/refactor the config. It's really not needed for the most part
    try {
      GlobalConfiguration.initializeFromFile("./config/config.properties");
      this.props = props;
      this.kafkaBrokerUrl = props.getString("kafka.broker.url", "localhost:9092");
      this.storeName = storeName;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (Exception e) {
      logger.error("Error while starting up configuration for VeniceReader.", e);
      throw new VeniceException("Error while starting up configuration for VeniceReader", e);
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
