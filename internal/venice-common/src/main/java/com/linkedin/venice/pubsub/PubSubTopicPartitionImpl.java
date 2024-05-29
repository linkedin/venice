package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.util.Objects;


public class PubSubTopicPartitionImpl implements PubSubTopicPartition {
  private final PubSubTopic topic;
  private final int partitionNumber;
  private final int hashCode;
  private String toStringOutput;

  public PubSubTopicPartitionImpl(PubSubTopic topic, int partitionNumber) {
    this.topic = Objects.requireNonNull(topic, "PubSubTopic cannot be null");
    this.partitionNumber = partitionNumber;
    this.hashCode = Objects.hash(topic, partitionNumber);
  }

  @Override
  public PubSubTopic getPubSubTopic() {
    return topic;
  }

  @Override
  public int getPartitionNumber() {
    return partitionNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PubSubTopicPartition)) {
      return false;
    }
    PubSubTopicPartition that = (PubSubTopicPartition) o;
    return partitionNumber == that.getPartitionNumber() && topic.equals(that.getPubSubTopic());
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    if (this.toStringOutput == null) {
      this.toStringOutput = Utils.getReplicaId(getTopicName(), getPartitionNumber());
    }
    return this.toStringOutput;
  }
}
