package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.controlmessage.JobProgressKafkaRecord;
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
    public void completed(long jobId, String topic, int partitionId, long totalMessagesProcessed) {
        super.completed(jobId, topic, partitionId, totalMessagesProcessed);
        ProducerRecord<byte[], byte[]> kafkaMessage = recordGenerator
                .generate(jobId, topic, partitionId, nodeId, totalMessagesProcessed);
        ackProducer.send(kafkaMessage);

    }

    @Override
    public void close() {
        ackProducer.close();
    }
}
