package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.serialization.Avro.JobProgressKafkaRecord;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


/**
 * Created by athirupa on 2/10/16.
 */
public class KafkaNotifier implements VeniceNotifier {

    private static final Logger logger = Logger.getLogger(KafkaConsumerPerStoreService.class.getName());

    private final JobProgressKafkaRecord recordGenerator;
    private final KafkaProducer<byte[], byte[]> ackProducer;
    private final int nodeId;
    public KafkaNotifier(String topic, Properties  props, int nodeId) {
        ackProducer = new KafkaProducer<>(props);
        recordGenerator = new JobProgressKafkaRecord(topic);
        this.nodeId = nodeId;
    }

    @Override
    public void started(long jobId, String topic, int partitionId) {
          logger.info("Push started for Store " + topic + " partitionId " + partitionId + " jobId " + jobId);
    }

    @Override
    public void completed(long jobId, String topic, int partitionId, long totalMessagesProcessed) {
        logger.info("Push completed for Store " + topic + " partitionId " + partitionId +
                " jobId " + jobId + " TotalMessage " + totalMessagesProcessed);

        ProducerRecord<byte[], byte[]> kafkaMessage = recordGenerator
                .generate(jobId, topic, partitionId, nodeId, totalMessagesProcessed);
        ackProducer.send(kafkaMessage);

    }

    @Override
    public void progress(long jobId, String topic, int partitionId, long counter) {
        logger.info("Push progress for Store " + topic + " partitionId " + partitionId
                + " jobId " + jobId + " TotalMessage " + counter);
    }

    @Override
    public void close() {
        ackProducer.close();
    }
}
