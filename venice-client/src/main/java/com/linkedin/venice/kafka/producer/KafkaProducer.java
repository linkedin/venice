package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
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

  public KafkaProducer() {

    // TODO: figure out the actual configurations and startup procedures
    Properties props = new Properties();
    props.setProperty("metadata.broker.list", GlobalConfiguration.getKafkaBrokerUrl());

    // using custom serializer
    props.setProperty("key.serializer.class", "com.linkedin.venice.message.KafkaKeySerializer");
    props.setProperty("serializer.class", "com.linkedin.venice.message.KafkaValueSerializer");

    // using custom partitioner
    props.setProperty("partitioner.class", "com.linkedin.venice.kafka.partitioner.KafkaPartitioner");
    props.setProperty("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<>(config);
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param key - The key of the message to be sent.
   * @param msg - The VeniceMessage, which acts as the Kafka payload.
   * */
  public void sendMessage(String topic, KafkaKey key, KafkaValue msg) {

    KeyedMessage<KafkaKey, KafkaValue> kafkaMsg = new KeyedMessage<>(topic, key, msg);
    producer.send(kafkaMsg);
  }
}
