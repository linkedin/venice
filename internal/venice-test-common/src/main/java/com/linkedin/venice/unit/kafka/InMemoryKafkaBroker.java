package com.linkedin.venice.unit.kafka;

import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Used in unit tests in order to avoid spinning a full Kafka broker with network stack
 * and disk IO.
 *
 * Instead, this Kafka broker keeps messages in memory. It can be used via the following
 * mock classes:
 * @see com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer
 * @see MockInMemoryProducerAdapter
 */
public class InMemoryKafkaBroker {
  private final Map<String, InMemoryKafkaTopic> topics = new HashMap<>();
  private final int port;
  private final String brokerNamePrefix;

  public InMemoryKafkaBroker(String brokerNamePrefix) {
    port = TestUtils.getFreePort();
    this.brokerNamePrefix = brokerNamePrefix;
  }

  public synchronized void createTopic(String topicName, int partitionCount) {
    if (topics.containsKey(topicName)) {
      throw new IllegalStateException(
          "The topic " + topicName + " already exists in this " + InMemoryKafkaBroker.class.getSimpleName());
    }

    topics.put(topicName, new InMemoryKafkaTopic(partitionCount));
  }

  /**
   * @param topicName The name of the topic in which to produce.
   * @param partition The partition in which to produce a message.
   * @param message The {@link InMemoryKafkaMessage} to produce into the partition.
   * @return the offset of the produced message
   * @throws IllegalArgumentException if the topic or partition does not exist.
   */
  public long produce(String topicName, int partition, InMemoryKafkaMessage message) {
    InMemoryKafkaTopic topic = getTopic(topicName);
    return topic.produce(partition, message);
  }

  /**
   * @param topicName The name of the topic from which to consume.
   * @param partition The partition from which to produce a message.
   * @return Some {@link InMemoryKafkaMessage} instance, or the {@link Optional#empty()} instance if that partition is drained.
   * @throws IllegalArgumentException if the topic or partition does not exist.
   */
  public Optional<InMemoryKafkaMessage> consume(String topicName, int partition, long offset)
      throws IllegalArgumentException {
    InMemoryKafkaTopic topic = getTopic(topicName);
    return topic.consume(partition, offset);
  }

  public int getPartitionCount(String topicName) {
    InMemoryKafkaTopic topic = getTopic(topicName);
    return topic.getPartitionCount();
  }

  /**
   * @param topicName Name of the requested {@link InMemoryKafkaTopic}
   * @return the requested {@link InMemoryKafkaTopic}
   * @throws IllegalArgumentException if the topic does not exist.
   */
  private InMemoryKafkaTopic getTopic(String topicName) throws IllegalArgumentException {
    InMemoryKafkaTopic topic = topics.get(topicName);
    if (topic == null) {
      throw new IllegalArgumentException(
          "The topic " + topicName + " does not exist in this " + InMemoryKafkaBroker.class.getSimpleName());
    }
    return topic;
  }

  /**
   * @return an synthetic broker server url.
   */
  public String getKafkaBootstrapServer() {
    return brokerNamePrefix + "_InMemoryKafkaBroker:" + port;
  }

  public Long endOffsets(String topicName, int partition) {
    return topics.get(topicName).getEndOffsets(partition);
  }
}
