package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.log4j.Logger;


/**
 * This class is not thread safe because of the internal {@link KafkaConsumer} being used.
 * backoff
 */
@NotThreadsafe
public class ApacheKafkaConsumer implements KafkaConsumerWrapper {
  private static final Logger logger = Logger.getLogger(ApacheKafkaConsumer.class);

  public static final String CONSUMER_POLL_RETRY_TIMES_CONFIG = "consumer.poll.retry.times";
  public static final String CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG = "consumer.poll.retry.backoff.ms";

  private static final int CONSUMER_POLL_RETRY_TIMES_DEFAULT = 3;
  private static final int CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT = 0;

  private final Consumer kafkaConsumer;

  private final int consumerPollRetryTimes;
  private final int consumerPollRetryBackoffMs;

  public ApacheKafkaConsumer(Properties props) {
    this.kafkaConsumer = new KafkaConsumer(props);

    VeniceProperties veniceProperties = new VeniceProperties(props);
    consumerPollRetryTimes = veniceProperties.getInt(CONSUMER_POLL_RETRY_TIMES_CONFIG, CONSUMER_POLL_RETRY_TIMES_DEFAULT);
    consumerPollRetryBackoffMs = veniceProperties.getInt(CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG, CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT);
    logger.info("Consumer poll retry times: " + consumerPollRetryTimes);
    logger.info("Consumer poll retry back off in ms: " + consumerPollRetryBackoffMs);
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
      logger.info("Subscribed to Topic: " + topic + " Partition: " + partition + " Offset: " + offset.toString());
    } else {
      logger.warn("Already subscribed on Topic: " + topic + " Partition: " + partition
          + ", ignore the request of subscription.");
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

    if (!topicPartitionSet.contains(topicPartition)) {
      throw new UnsubscribedTopicPartitionException(topic, partition);
    }
    kafkaConsumer.seekToBeginning(Arrays.asList(topicPartition));
  }

  @Override
  public ConsumerRecords poll(long timeout) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION and when the
    // fetcher.retrieveOffsetsByTimes call inside kafkaConsumer times out,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    int attemptCount = 1;
    ConsumerRecords records = ConsumerRecords.empty();
    while (attemptCount <= consumerPollRetryTimes) {
      try {
        records = kafkaConsumer.poll(timeout);
        break;
      } catch (RetriableException e) {
        logger.warn(
            "Retriable exception thrown when attempting to consume records from kafka, attempt " + attemptCount + "/"
                + consumerPollRetryTimes, e);
        if (attemptCount == consumerPollRetryTimes) {
          throw e;
        }
        try {
          if (consumerPollRetryBackoffMs > 0) {
            Thread.sleep(consumerPollRetryBackoffMs);
          }
        } catch (InterruptedException ie) {
          // Here will still throw the actual exception thrown by internal consumer to make sure the stacktrace is meaningful.
          throw new VeniceException("Consumer poll retry back off sleep got interrupted", e);
        }
      } finally {
        attemptCount++;
      }
    }
    return records;
  }

  @Override
  public boolean hasSubscription() {
    return !kafkaConsumer.assignment().isEmpty();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return kafkaConsumer.listTopics();
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(List<TopicPartition> topicPartitions) {
    return kafkaConsumer.beginningOffsets(topicPartitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions) {
    return kafkaConsumer.endOffsets(topicPartitions);
  }

  @Override
  public void assign(List<TopicPartition> topicPartitions) {
    kafkaConsumer.assign(topicPartitions);
  }

  @Override
  public void seek(TopicPartition topicPartition, long nextOffset) {
    kafkaConsumer.seek(topicPartition, nextOffset);
  }

  @Override
  public void close() {
    if (kafkaConsumer != null) {
      kafkaConsumer.close();
    }
  }
}
