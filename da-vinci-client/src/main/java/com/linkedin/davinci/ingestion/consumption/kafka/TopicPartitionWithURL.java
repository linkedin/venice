package com.linkedin.davinci.ingestion.consumption.kafka;

import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.TopicPartition;


public class TopicPartitionWithURL {
  private static final int NO_HASH_CODE = -1;

  private final TopicPartition topicPartition;
  private final String kafkaServerURL;
  private int hashCode;

  public TopicPartitionWithURL(TopicPartition topicPartition, String kafkaServerURL) {
    this.topicPartition = Validate.notNull(topicPartition);
    this.kafkaServerURL = Validate.notNull(kafkaServerURL);
    this.hashCode = NO_HASH_CODE;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public String getKafkaServerURL() {
    return kafkaServerURL;
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASH_CODE) {
      return hashCode = Objects.hash(topicPartition, kafkaServerURL);
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
    return this.kafkaServerURL.equals(other.kafkaServerURL) && this.topicPartition.equals(other.topicPartition);
  }
}
