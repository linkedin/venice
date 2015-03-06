package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.config.GlobalConfiguration;
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
  private Producer<byte[], VeniceMessage> producer;

  private final String DEFAULT_TOPIC = "default_topic";

  public KafkaProducer() {

    // TODO: figure out the actual configurations and startup procedures
    props = new Properties();
    props.setProperty("metadata.broker.list", GlobalConfiguration.getKafkaBrokerUrl());
    props.setProperty("key.serializer.class", "kafka.serializer.DefaultEncoder");

    // using custom serializer
    props.setProperty("serializer.class", "com.linkedin.venice.message.VeniceMessageSerializer");

    // using custom partitioner
    props.setProperty("partitioner.class", "com.linkedin.venice.kafka.partitioner.KafkaPartitioner");
    props.setProperty("request.required.acks", "1");

    config = new ProducerConfig(props);
    producer = new Producer<byte[], VeniceMessage>(config);
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param key - The key of the message to be sent.
   * @param msg - The VeniceMessage, which acts as the Kafka payload.
   * */
  public void sendMessage(byte[] key, VeniceMessage msg) {

    KeyedMessage<byte[], VeniceMessage> kafkaMsg = new KeyedMessage<byte[], VeniceMessage>(DEFAULT_TOPIC, key, msg);
    producer.send(kafkaMsg);
  }
}
