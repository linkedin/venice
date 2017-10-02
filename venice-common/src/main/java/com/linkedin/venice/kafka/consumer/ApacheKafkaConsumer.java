package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

  public ApacheKafkaConsumer(VeniceProperties props) {
    this(props.toProperties());
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
    } else {
      // Considering the offset of the same consumer group could be persisted by some other consumer in Kafka.
      kafkaConsumer.seekToBeginning(Arrays.asList(topicPartition));
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
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();

    if (! topicPartitionSet.contains(topicPartition)) {
      throw new UnsubscribedTopicPartitionException(topic, partition);
    }
    kafkaConsumer.seekToBeginning(Arrays.asList(topicPartition));
  }

  @Override
  public ConsumerRecords poll(long timeout) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    return kafkaConsumer.poll(timeout);
  }

  @Override
  public boolean hasSubscription() {
    return !kafkaConsumer.assignment().isEmpty();
  }

  @Override
  public void close() {
    if (kafkaConsumer != null) {
      kafkaConsumer.close();
    }
  }

}
