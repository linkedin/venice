package com.linkedin.venice.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.producer.KafkaProducerWrapper;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API
 */
public class VeniceWriter<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceWriter.class.getName());

  protected final KafkaProducerWrapper producer;

  protected VeniceProperties props;
  protected final String storeName;
  protected final VeniceSerializer<K> keySerializer;
  protected final VeniceSerializer<V> valueSerializer;


  public VeniceWriter(VeniceProperties props, String storeName, VeniceSerializer<K> keySerializer, VeniceSerializer<V> valueSerializer) {
    this.props = props;
    this.storeName = storeName;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    try {
      this.producer = new KafkaProducerWrapper(props);
    } catch (Exception e) {
      throw new VeniceException("Error while constructing VeniceWriter KafkaProducer " + storeName, e);
    }
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key) {
    // Ensure the Operation type for KafkaKey is WRITE. And the actual Operation type DELETE is used in KafkaValue
    KafkaKey kafkaKey = new KafkaKey(OperationType.WRITE, keySerializer.serialize(storeName, key));
    KafkaValue kafkaValue = new KafkaValue(OperationType.DELETE);
    return producer.sendMessage(storeName, kafkaKey, kafkaValue);
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> put(K key, V value) {
    // Ensure the Operation type for KafkaKey is WRITE. And the actual Operation type PUT is used in KafkaValue
    KafkaKey kafkaKey = new KafkaKey(OperationType.WRITE, keySerializer.serialize(storeName, key));
    KafkaValue kafkaValue = new KafkaValue(OperationType.PUT, valueSerializer.serialize(storeName, value));
    try {
      return producer.sendMessage(storeName, kafkaKey, kafkaValue);
    } catch (Exception e) {
      throw new VeniceException(" Got an exception while trying to produce to Kafka.", e);
    }
  }

  /**
   * Send a control message to all the Partitions for a topic
   *
   * @param opType OperationType to be sent.
   * @param jobId jobId sending the control message.
   */
  public void writeControlMessage(OperationType opType, long jobId) {
    ControlFlagKafkaKey key = new ControlFlagKafkaKey(opType, new byte[]{} , jobId );
    KafkaValue value = new KafkaValue(opType);

    try {
      producer.sendControlMessage(storeName, key, value);
    } catch (Exception e) {
      throw new VeniceException(" Got an exception while trying to send control message to Kafka " + opType, e);
    }
  }

  /**
   * Execute a standard "partial put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   */
  public void putPartial(K key, V value) {

    throw new UnsupportedOperationException("Partial put is not supported yet.");
  }

  public void close() {
    producer.close();
  }

  public String getStoreName() {
    return storeName;
  }
}
