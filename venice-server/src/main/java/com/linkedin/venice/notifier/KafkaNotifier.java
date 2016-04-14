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
    private final int nodeId;

    public KafkaNotifier(String topic, Properties  props, int nodeId) {
        super();
        ackProducer = new KafkaProducer<>(props);
        recordGenerator = new JobProgressKafkaRecord(topic);
        this.nodeId = nodeId;
    }

    @Override
    public void completed(long jobId, String topic, int partitionId, long offset) {
        super.completed(jobId, topic, partitionId, offset);
        ProducerRecord<byte[], byte[]> kafkaMessage = recordGenerator
                .generate(jobId, topic, partitionId, nodeId, offset);
        ackProducer.send(kafkaMessage);
    }

  @Override
    public void close() {
        ackProducer.close();
    }
}
