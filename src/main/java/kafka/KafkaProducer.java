package kafka;

import message.Message;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by clfung on 9/11/14.
 */
public class KafkaProducer {

    private Properties props;
    private ProducerConfig config;
    private Producer<String, String> producer;

    // process which will produce messages for Kafka
    public KafkaProducer() {

        props = new Properties();
        props.setProperty("metadata.broker.list", "broker1:9092,broker2:9092");
        props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        props.setProperty("partitioner.class", "kafka.KafkaPartitioner"); // using custom partitioner
        props.setProperty("request.required.acks", "1");

        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

    }

    public void sendMessage(Message msg) {

        // TODO: need to serialize the message before sending it
        KeyedMessage<String, String> kafkaMsg = new KeyedMessage<String, String>("kafka_key", msg.getPayload());

        // TODO: investigate Kafka startup
        producer.send(kafkaMsg);

    }

}
