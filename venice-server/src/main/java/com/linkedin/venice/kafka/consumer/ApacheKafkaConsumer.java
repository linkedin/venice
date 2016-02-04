package com.linkedin.venice.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


/**
 * Created by athirupa on 2/3/16.
 */
public class ApacheKafkaConsumer implements VeniceConsumer {

    private final Consumer kafkaConsumer;

    public ApacheKafkaConsumer(Properties props) {
        this.kafkaConsumer = new KafkaConsumer(props);
    }
    @Override
    public long getLastOffset(String topic, int partition) {
        return kafkaConsumer.committed(new TopicPartition(topic, partition));
    }

    @Override
    public void subscribe(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        if(!kafkaConsumer.subscriptions().contains(topicPartition)) {
            kafkaConsumer.subscribe(topicPartition);
        }
    }

    @Override
    public void unSubscribe(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        if(kafkaConsumer.subscriptions().contains(topicPartition)) {
            kafkaConsumer.unsubscribe(topicPartition);
        }
    }

    @Override
    public long resetOffset(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        kafkaConsumer.seekToBeginning(topicPartition);
        long beginningOffSet = kafkaConsumer.position(topicPartition);

        // Commit the beginning offset to prevent the use of old committed offset.
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        offsetMap.put(topicPartition, beginningOffSet);
        kafkaConsumer.commit(offsetMap, CommitType.SYNC);

        return beginningOffSet;
    }

    @Override
    public void seek(String topic, int partition, long newOffset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        kafkaConsumer.seek(topicPartition, newOffset);
    }

    @Override
    public ConsumerRecords poll(long timeout) {
        return kafkaConsumer.poll(timeout);
    }

    @Override
    public void close() {
        if(kafkaConsumer != null){
            kafkaConsumer.close();
        }
    }
}
