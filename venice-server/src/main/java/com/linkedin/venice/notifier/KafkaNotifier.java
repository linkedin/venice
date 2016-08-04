package com.linkedin.venice.notifier;

import com.linkedin.venice.serialization.avro.JobProgressKafkaRecord;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Sends confirmation to the Kafka topics.
 */
public class KafkaNotifier extends LogNotifier {

    private final JobProgressKafkaRecord recordGenerator;
    private final KafkaProducer<byte[], byte[]> ackProducer;

    public KafkaNotifier(String topic, Properties  props) {
        super();
        ackProducer = new KafkaProducer<>(props);
        recordGenerator = new JobProgressKafkaRecord(topic);
    }

    @Override
    public void completed(String topic, int partitionId, long offset) {
        super.completed(topic, partitionId, offset);
        ProducerRecord<byte[], byte[]> kafkaMessage = recordGenerator.generate(topic, partitionId, offset);
        ackProducer.send(kafkaMessage);
    }

  @Override
    public void close() {
        ackProducer.close();
    }
}
