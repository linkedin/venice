package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Implementation of the Kafka Producer for sending messages to Kafka.
 */
public class ApacheKafkaProducer implements KafkaProducerWrapper {

  public static final String PROPERTIES_KAFKA_PREFIX = "kafka.";

  private final KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer;

  public ApacheKafkaProducer(VeniceProperties props) {
    /** TODO: Consider making these default settings part of {@link VeniceWriter} or {@link KafkaProducerWrapper} */

    Properties properties = getKafkaPropertiesFromVeniceProps(props);

    // TODO : For sending control message, this is not required. Move this higher in the stack.
    ensureMandatoryProp(properties, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class.getName());
    ensureMandatoryProp(properties, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class.getName());

    // This is to guarantee ordering, even in the face of failures.
    ensureMandatoryProp(properties, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

    if (!properties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new ConfigurationException("Props key not found: " + PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    producer = new KafkaProducer<>(properties);
    // TODO: Consider making the choice of partitioner implementation configurable
  }

  /**
   * Function which sets some needed defaults... Also bubbles up an exception in order
   * to fail fast if any calling class tries to override these defaults.
   *
   * TODO: Decide if this belongs here or higher up the call-stack
   */
  private void ensureMandatoryProp(Properties properties, String requiredConfigKey, String requiredConfigValue) {
    String actualConfigValue = properties.getProperty(requiredConfigKey);
    if (actualConfigValue == null) {
      properties.setProperty(requiredConfigKey, requiredConfigValue);
    } else if (!actualConfigValue.equals(requiredConfigValue)) {
      // We fail fast rather than attempting to use non-standard serializers
      throw new VeniceException("The Kafka Producer must use certain configuration settings in order to work properly. " +
          "requiredConfigKey: '" + requiredConfigKey +
          "', requiredConfigValue: '" + requiredConfigValue +
          "', actualConfigValue: '" + actualConfigValue + "'.");
    }
  }

  /**
   * N.B.: This is an expensive call, the result of which should be cached.
   *
   * @param topic for which we want to request the number of partitions.
   * @return the number of partitions for this topic.
   */
  public int getNumberOfPartitions(String topic) {
    return producer.partitionsFor(topic).size();
  }


    /**
     * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
     * @param topic - The topic to be sent to.
     * @param key - The key of the message to be sent.
     * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
     * */
  public Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition) {
    try {
      ProducerRecord<KafkaKey, KafkaMessageEnvelope> kafkaRecord = new ProducerRecord<>(topic,
          partition,
          key,
          value);
      return producer.send(kafkaRecord);
    } catch (Exception e) {
      throw new VeniceException("Got an error while trying to produce message into Kafka. Topic: '" + topic +
          "', partition: " + partition, e);
    }

  }

  public void close(int closeTimeOutMs) {
    if (producer != null) {
      producer.close(closeTimeOutMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * This class takes in all properties that begin with "{@value #PROPERTIES_KAFKA_PREFIX}" and emits the
   * rest of the properties.
   *
   * It omits those properties that do not begin with "{@value #PROPERTIES_KAFKA_PREFIX}".
   *
   * TODO: Consider making this logic part of {@link VeniceWriter} or {@link KafkaProducerWrapper}.
  */
  private Properties getKafkaPropertiesFromVeniceProps(VeniceProperties props) {
    VeniceProperties kafkaProps = props.clipAndFilterNamespace(PROPERTIES_KAFKA_PREFIX);
    return kafkaProps.toProperties();
  }
}
