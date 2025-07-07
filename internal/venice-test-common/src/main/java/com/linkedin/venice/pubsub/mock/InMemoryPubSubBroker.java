package com.linkedin.venice.pubsub.mock;

import com.linkedin.venice.pubsub.mock.adapter.consumer.MockInMemoryConsumerAdapter;
import com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Used in unit tests in order to avoid spinning a full Kafka broker with network stack
 * and disk IO.
 *
 * Instead, this Kafka broker keeps messages in memory. It can be used via the following
 * mock classes:
 * @see MockInMemoryConsumerAdapter
 * @see MockInMemoryProducerAdapter
 */
public class InMemoryPubSubBroker {
  private final Map<String, InMemoryPubSubTopic> topics = new VeniceConcurrentHashMap<>();
  private final int port;
  private final String brokerNamePrefix;

  public InMemoryPubSubBroker(String brokerNamePrefix) {
    port = TestUtils.getFreePort();
    this.brokerNamePrefix = brokerNamePrefix;
  }

  public synchronized void createTopic(String topicName, int partitionCount) {
    if (topics.containsKey(topicName)) {
      throw new IllegalStateException(
          "The topic " + topicName + " already exists in this " + InMemoryPubSubBroker.class.getSimpleName());
    }

    topics.put(topicName, new InMemoryPubSubTopic(partitionCount));
  }

  /**
   * @param topicName The name of the topic in which to produce.
   * @param partition The partition in which to produce a message.
   * @param message The {@link InMemoryPubSubMessage} to produce into the partition.
   * @return the offset of the produced message
   * @throws IllegalArgumentException if the topic or partition does not exist.
   */
  public InMemoryPubSubPosition produce(String topicName, int partition, InMemoryPubSubMessage message) {
    InMemoryPubSubTopic topic = getTopic(topicName);
    return topic.produce(partition, message);
  }

  /**
   * @param topicName The name of the topic from which to consume.
   * @param partition The partition from which to produce a message.
   * @return Some {@link InMemoryPubSubMessage} instance, or the {@link Optional#empty()} instance if that partition is drained.
   * @throws IllegalArgumentException if the topic or partition does not exist.
   */
  public Optional<InMemoryPubSubMessage> consume(String topicName, int partition, InMemoryPubSubPosition position)
      throws IllegalArgumentException {
    InMemoryPubSubTopic topic = getTopic(topicName);
    return topic.consume(partition, position);
  }

  public int getPartitionCount(String topicName) {
    InMemoryPubSubTopic topic = getTopic(topicName);
    return topic.getPartitionCount();
  }

  /**
   * @param topicName Name of the requested {@link InMemoryPubSubTopic}
   * @return the requested {@link InMemoryPubSubTopic}
   * @throws IllegalArgumentException if the topic does not exist.
   */
  private InMemoryPubSubTopic getTopic(String topicName) throws IllegalArgumentException {
    InMemoryPubSubTopic topic = topics.get(topicName);
    if (topic == null) {
      throw new IllegalArgumentException(
          "The topic " + topicName + " does not exist in this " + InMemoryPubSubBroker.class.getSimpleName());
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

  public InMemoryPubSubPosition endPosition(String topicName, int partition) {
    return topics.get(topicName).endPosition(partition);
  }
}
