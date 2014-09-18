package kafka;

import message.VeniceMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import venice.VeniceClient;

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

    // TODO: figure out the actual configurations
    props = new Properties();
    props.setProperty("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("serializer.class", "message.VeniceMessageSerializer");
    props.setProperty("partitioner.class", "kafka.KafkaPartitioner"); // using custom partitioner
    props.setProperty("request.required.acks", "1");

    config = new ProducerConfig(props);
    producer = new Producer<String, VeniceMessage>(config);

  }

  public void sendMessage(VeniceMessage msg) {

    // TODO: investigate if key and payload are serialized together
    KeyedMessage<String, VeniceMessage> kafkaMsg = new KeyedMessage<String, VeniceMessage>(VeniceClient.TEST_TOPIC,
        VeniceClient.TEST_KEY, msg);

    // TODO: investigate Kafka startup
    producer.send(kafkaMsg);

  }

}
