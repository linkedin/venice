package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.offsets.OffsetRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

    private void seek(TopicPartition topicPartition , OffsetRecord offset) {
        if(offset.getOffset() == OffsetRecord.LOWEST_OFFSET) {
            kafkaConsumer.seekToBeginning(topicPartition);
        } else {
            // The last consumed message offset is remembered. Add +1 to the offset
            // to prevent retrieving the last message twice.
            kafkaConsumer.seek(topicPartition, offset.getOffset() + 1);
        }
    }

    @Override
    public void subscribe(String topic, int partition, OffsetRecord offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
        if(!topicPartitionSet.contains(topicPartition)) {
            List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
            topicPartitionList.add(topicPartition);
            kafkaConsumer.assign(topicPartitionList);
            seek(topicPartition, offset);
        }
    }

    @Override
    public void unSubscribe(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
        if(topicPartitionSet.contains(topicPartition)) {
            List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
            if( topicPartitionList.remove(topicPartition) ) {
                kafkaConsumer.assign(topicPartitionList);
            }
        }
    }

    @Override
    public void resetOffset(String topic, int partition) {
        // It intentionally throws an error when offset was reset for a topic
        // that is not subscribed to.
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        seek(topicPartition, OffsetRecord.NON_EXISTENT_OFFSET);
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
