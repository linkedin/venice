package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.Venice;
import com.linkedin.venice.message.VeniceMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Implementation of the Kafka Producer.
 * Receives and sends messages to Kafka.
 */
public class KafkaProducer {

  private Properties props;
  private ProducerConfig config;
  private Producer<String, VeniceMessage> producer;

  public KafkaProducer() {

    // TODO: figure out the actual configurations and startup procedures
    props = new Properties();
    props.setProperty("metadata.broker.list", "localhost:9092");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");

    // using custom serializer
    props.setProperty("serializer.class", "com.linkedin.venice.message.VeniceMessageSerializer");

    // using custom partitioner
    props.setProperty("partitioner.class", "com.linkedin.venice.kafka.partitioner.KafkaPartitioner");
    props.setProperty("request.required.acks", "1");

    config = new ProducerConfig(props);
    producer = new Producer<String, VeniceMessage>(config);

  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param key - The key of the message to be sent.
   * @param msg - The VeniceMessage, which acts as the Kafka payload.
   * */
  public void sendMessage(String key, VeniceMessage msg) {

    KeyedMessage<String, VeniceMessage> kafkaMsg = new KeyedMessage<String, VeniceMessage>(Venice.DEFAULT_TOPIC, key, msg);
    producer.send(kafkaMsg);

  }

}
