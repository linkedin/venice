package com.linkedin.venice.kafka.producer;

import com.linkedin.venice.Venice;
import com.linkedin.venice.message.VeniceMessage;

import com.linkedin.venice.server.VeniceServer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.linkedin.venice.client.VeniceClient;

import java.util.Properties;

/**
 * Created by clfung on 9/11/14.
 */
public class KafkaProducer {

  private Properties props;
  private ProducerConfig config;
  private Producer<String, VeniceMessage> producer;

  // process which will produce messages for Kafka
  public KafkaProducer() {

    // TODO: figure out the actual configurations and startup procedures
    props = new Properties();
    props.setProperty("metadata.broker.list", "localhost:9092");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("serializer.class", "com.linkedin.venice.message.VeniceMessageSerializer");
    props.setProperty("partitioner.class", "com.linkedin.venice.kafka.partitioner.KafkaPartitioner"); // using custom partitioner
    props.setProperty("request.required.acks", "1");

    config = new ProducerConfig(props);
    producer = new Producer<String, VeniceMessage>(config);

  }

  public void sendMessage(String key, VeniceMessage msg) {

    KeyedMessage<String, VeniceMessage> kafkaMsg = new KeyedMessage<String, VeniceMessage>(Venice.DEFAULT_TOPIC, key, msg);
    producer.send(kafkaMsg);

  }

}
