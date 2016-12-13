package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.offsets.OffsetRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


/**
 * This class is not thread safe because of the internal {@link KafkaConsumer} being used.
 */
@NotThreadsafe
public class ApacheKafkaConsumer implements KafkaConsumerWrapper {
  private final Consumer kafkaConsumer;
  public ApacheKafkaConsumer(Properties props) {
    this.kafkaConsumer = new KafkaConsumer(props);
  }

  private void seek(TopicPartition topicPartition, OffsetRecord offset) {
    // Kafka Consumer controls the default offset to start by the property
    // "auto.offset.reset" , it is set to "earliest" to start from the
    // beginning.

    // Venice would prefer to start from the beginning and using seekToBeginning
    // would have made it clearer. But that call always fail and can be used
    // only after the offsets are remembered for a partition in 0.9.0.2

    long lastReadOffset = offset.getOffset();
    if (lastReadOffset != OffsetRecord.LOWEST_OFFSET) {
      long nextReadOffset = lastReadOffset + 1;
      kafkaConsumer.seek(topicPartition, nextReadOffset);
    }
  }

  @Override
  public void subscribe(String topic, int partition, OffsetRecord offset) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (!topicPartitionSet.contains(topicPartition)) {
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
    if (topicPartitionSet.contains(topicPartition)) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      if (topicPartitionList.remove(topicPartition)) {
        kafkaConsumer.assign(topicPartitionList);
      }
    }
  }

  @Override
  public void resetOffset(String topic, int partition) {
    // It intentionally throws an error when offset was reset for a topic
    // that is not subscribed to.
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    kafkaConsumer.seekToBeginning(topicPartition);
  }

  @Override
  public ConsumerRecords poll(long timeout) {
    return kafkaConsumer.poll(timeout);
  }

  @Override
  public void close() {
    if (kafkaConsumer != null) {
      kafkaConsumer.close();
    }
  }
}
