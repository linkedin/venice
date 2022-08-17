package com.linkedin.davinci.ingestion.consumption.kafka;

import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.TopicPartition;


public class TopicPartitionWithURL {
  private static final int NO_HASH_CODE = -1;

  private final TopicPartition topicPartition;
  private final String kafkaServerURL;
  /**
   * This field is need to support a situation where a topic partition in the same Kafka cluster needs to have multiple
   * concurrent consumption sessions. For example, there are 2 users of the {@link KafkaConsumptionService#startConsuming}
   * method and one user wants to start consuming tp_0 starting at offset 10 and another user wants to start consuming
   * tp_0 in the same Kafka cluster starting at offset 38. Each user should get their own consumed data stream without knowing
   * the other consumed data stream. In this case, users need to provide a unique {@param owner} to identify their own
   * {@link TopicPartitionWithURL}.
   */
  private final String owner;
  private int hashCode;

  public TopicPartitionWithURL(TopicPartition topicPartition, String kafkaServerURL, String owner) {
    this.topicPartition = Validate.notNull(topicPartition);
    this.kafkaServerURL = Validate.notNull(kafkaServerURL);
    this.owner = Validate.notNull(owner);
    this.hashCode = NO_HASH_CODE;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public String getKafkaServerURL() {
    return kafkaServerURL;
  }

  public String getOwner() {
    return owner;
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASH_CODE) {
      return hashCode = Objects.hash(topicPartition, kafkaServerURL, owner);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionWithURL other = (TopicPartitionWithURL) o;
    return this.owner.equals(other.owner) && this.kafkaServerURL.equals(other.kafkaServerURL)
        && this.topicPartition.equals(other.topicPartition);
  }
}
