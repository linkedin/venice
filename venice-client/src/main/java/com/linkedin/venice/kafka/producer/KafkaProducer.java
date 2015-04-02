package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.partitioner.DefaultKafkaPartitioner;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.utils.Props;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


/**
 * Implementation of the Kafka Producer.
 * Receives and sends messages to Kafka.
 */
public class KafkaProducer {

  private final Producer<KafkaKey, KafkaValue> producer;

  public KafkaProducer(Props props) {

    // TODO: figure out the actual configurations and startup procedures

    Properties properties = setPropertiesFromProp(props);

    if (!properties.containsKey("metadata.broker.list")) {
      throw new VeniceException("Props key not found: kafka.broker.url");
    }

    // using custom serializer
    properties.setProperty("key.serializer.class", KafkaKeySerializer.class.getName());
    properties.setProperty("serializer.class", KafkaValueSerializer.class.getName());

    // using custom partitioner
    properties.setProperty("partitioner.class", DefaultKafkaPartitioner.class.getName());
    properties.setProperty("request.required.acks", "-1");

    ProducerConfig config = new ProducerConfig(properties);
    producer = new Producer<KafkaKey, KafkaValue>(config);
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The KafkaValue, which acts as the Kafka payload.
   * */
  public void sendMessage(String topic, KafkaKey key, KafkaValue value) {

    KeyedMessage<KafkaKey, KafkaValue> kafkaMsg = new KeyedMessage<KafkaKey, KafkaValue>(topic, key, value);
    producer.send(kafkaMsg);
  }

  public void close() {
    producer.close();
  }

  /**
   * This class takes in all properties that begin with "kafka." and emits the rest of the property
   *
   * It omits those properties that do not begin with "kafka."
  */
  public Properties setPropertiesFromProp(Props props) {
    Properties properties = new Properties();
    for (String keyStr : props.keySet()) {
      if (keyStr.startsWith("kafka.")) {
        properties.put(keyStr.split("kafka.")[1], props.getString(keyStr));
      }
    }

    return properties;
  }
}
