package com.linkedin.venice.client;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.producer.KafkaProducer;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Serializer;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API
 */
public class VeniceWriter<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceWriter.class.getName());

  private static KafkaProducer kp;

  private KafkaKey kafkaKey;
  private KafkaValue kafkaValue;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  public VeniceWriter(Serializer<K> keySerializer, Serializer<V> valueSerializer) {

    // TODO: Deprecate/refactor the config. It's really not needed for the most part
    try {
      GlobalConfiguration.initializeFromFile("./config/config.properties");
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (Exception e) {
      logger.error("Error while starting up configuration for VeniceWriter.");
      logger.error(e);
      System.exit(1);
    }

    kp = new KafkaProducer();
  }

  /**
   * Execute a standard "delete" on the key.
   * @param key - The key to delete in storage.
   * */
  public void delete(K key) {

    kafkaKey = new KafkaKey(keySerializer.toBytes(key));
    kafkaValue = new KafkaValue(OperationType.DELETE);
    kp.sendMessage(kafkaKey, kafkaValue);
  }

  /**
   * Execute a standard "put" on the key.
   * @param key - The key to put in storage.
   * @param value - The value to be associated with the given key
   * */
  public void put(K key, V value) {

    kafkaKey = new KafkaKey(keySerializer.toBytes(key));
    kafkaValue = new KafkaValue(OperationType.PUT, valueSerializer.toBytes(value));
    kp.sendMessage(kafkaKey, kafkaValue);
  }

  /**
   * Execute a standard "partial put" on the key.
   * @param key - The key to put in storage.
   * @param value - The value to be associated with the given key
   * */
  public void putPartial(K key, V value) {

    throw new UnsupportedOperationException("Partial put is not supported yet.");
  }
}
