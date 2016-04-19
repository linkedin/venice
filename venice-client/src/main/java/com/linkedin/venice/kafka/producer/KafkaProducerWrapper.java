package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Implementation of the Kafka Producer for sending messages to Kafka.
 */
public class KafkaProducerWrapper {

  private final KafkaProducer<KafkaKey, KafkaValue> producer;
  private final String PROPERTIES_KAFKA_PREFIX = "kafka.";
  private final VenicePartitioner partitioner;

  public KafkaProducerWrapper(VeniceProperties props) {
    Properties properties = getKafkaPropertiesFromVeniceProps(props);

    // TODO : For sending control message, this is not required. Move this higher in the stack.
    checkMandatoryProp(properties, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class.getName());
    checkMandatoryProp(properties, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class.getName());

    if (!properties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new ConfigurationException("Props key not found: " + PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    producer = new KafkaProducer<>(properties);
    // TODO: Consider making the choice of partitioner implementation configurable
    this.partitioner = new DefaultVenicePartitioner(props);
  }

  /**
   * Function which sets some needed defaults... Also bubbles up an exception in order
   * to fail fast if any calling class tries to override these defaults.
   *
   * TODO: Decide if this belongs here or higher up the call-stack
   */
  private void checkMandatoryProp(Properties properties, String requiredConfigKey, String requiredConfigValue) {
    String actualConfigValue = properties.getProperty(requiredConfigKey);
    if (actualConfigValue == null) {
      properties.setProperty(requiredConfigKey, requiredConfigValue);
    } else if (!actualConfigValue.equals(requiredConfigValue)) {
      // We fail fast rather than attempting to use non-standard serializers
      throw new VeniceException("The Kafka Producer must use the Venice serializers in order to work properly.");
    }
  }

  private Future<RecordMetadata> sendMessageToPartition(String topic, KafkaKey key, KafkaValue value, int partition) {
    ProducerRecord<KafkaKey, KafkaValue> kafkaRecord = new ProducerRecord<>(topic,
            partition,
            key,
            value);
    return producer.send(kafkaRecord);
  }

  private int getNumberOfPartitions(String topic) {
    // TODO: partitionsFor() is likely an expensive call, so consider caching with proper validation and/or eviction
    return producer.partitionsFor(topic).size();
  }


    /**
     * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
     * @param topic - The topic to be sent to.
     * @param key - The key of the message to be sent.
     * @param value - The KafkaValue, which acts as the Kafka payload.
     * */
  public Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaValue value) {
    int numberOfPartitions = getNumberOfPartitions(topic);
    int partition = partitioner.getPartitionId(key, numberOfPartitions);
    return sendMessageToPartition(topic, key, value, partition);
  }

  /**
   * Sends a control message to all the partitions in the kafka topic for storage nodes to consume.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The KafkaValue, which acts as the Kafka payload.
   */
  public void sendControlMessage(String topic, ControlFlagKafkaKey key, KafkaValue value) {
    int numberOfPartitions = producer.partitionsFor(topic).size();
    for(int partition = 0; partition < numberOfPartitions ; partition ++) {
      sendMessageToPartition(topic, key, value, partition);
    }
  }

  public void close() {
    if(producer != null) {
      producer.close();
    }
  }

  /**
   * This class takes in all properties that begin with "venice." and emits the rest of the property
   *
   * It omits those properties that do not begin with "venice."
  */
  public Properties getKafkaPropertiesFromVeniceProps(VeniceProperties props) {
    VeniceProperties kafkaProps = props.extractProperties(PROPERTIES_KAFKA_PREFIX);
    return kafkaProps.toProperties();
  }
}
