package com.linkedin.venice.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.producer.KafkaProducerWrapper;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.VeniceProperties;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API
 */
public class VeniceWriter<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceWriter.class);

  protected final VeniceProperties props;
  protected final String storeName;
  protected final VeniceSerializer<K> keySerializer;
  protected final VeniceSerializer<V> valueSerializer;
  protected final KafkaProducerWrapper producer;

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
    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, keySerializer.serialize(storeName, key));

    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.DELETE);

    return producer.sendMessage(storeName, kafkaKey, kafkaValue);
  }

  // TODO: Once we finishes venice client/schema registry integration, we need to remove this interface
  public Future<RecordMetadata> put(K key, V value) {
    return put(key, value, -1);
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(storeName, key));

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.PUT);
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(valueSerializer.serialize(storeName, value));
    putPayload.schemaId = valueSchemaId;
    kafkaValue.payloadUnion = putPayload;

    try {
      return producer.sendMessage(storeName, kafkaKey, kafkaValue);
    } catch (Exception e) {
      throw new VeniceException("Got an error while trying to produce to Kafka.", e);
    }
  }


  /**
   * Send a control message to all the Partitions for a topic
   *
   * @param controlMessageType OperationType to be sent.
   */
  public void broadcastControlMessage(ControlMessageType controlMessageType) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[]{});

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope value = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = controlMessageType.getValue();
    controlMessage.debugInfo = new HashMap<>(); // TODO: Expose this in the API
    controlMessage.controlMessageUnion = controlMessageType.getNewInstance();
    value.payloadUnion = controlMessage;

    try {
      producer.broadcastMessage(storeName, kafkaKey, value);
    } catch (Exception e) {
      throw new VeniceException("Got an error while trying to broadcast '" + controlMessageType.name() +
          "' control message to Kafka.", e);
    }
  }

  public void close() {
    producer.close();
  }

  public String getStoreName() {
    return storeName;
  }

  /**
   * A utility function to centralize some boiler plate code for the instantiation of
   * {@link org.apache.avro.specific.SpecificRecord} classes holding the content of our
   * Kafka values.
   *
   * @param messageType an instance of the {@link MessageType} enum.
   * @return A {@link KafkaMessageEnvelope} suitable for producing into Kafka
   */
  private KafkaMessageEnvelope getKafkaMessageEnvelope(MessageType messageType) {
    // If single-threaded, the kafkaValue could be re-used (and clobbered). TODO: explore GC tuning later.
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = messageType.getValue();

    // TODO: Populate producer metadata properly
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = new GUID();
    producerMetadata.producerGUID.bytes(new byte[16]);
    producerMetadata.messageSequenceNumber = -1;
    producerMetadata.segmentNumber = -1;
    producerMetadata.messageTimestamp = -1;
    kafkaValue.producerMetadata = producerMetadata;

    return kafkaValue;
  }
}
