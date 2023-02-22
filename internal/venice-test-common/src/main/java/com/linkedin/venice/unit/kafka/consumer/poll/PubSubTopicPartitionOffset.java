package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


public class PubSubTopicPartitionOffset {
  private final PubSubTopicPartition pubSubTopicPartition;
  private final Long offset;

  private final int hashCode;

  public PubSubTopicPartitionOffset(PubSubTopicPartition pubSubTopicPartition, Long offset) {
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.offset = offset;
    this.hashCode = Objects.hash(pubSubTopicPartition, offset);
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return this.pubSubTopicPartition;
  }

  public Long getOffset() {
    return offset;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof PubSubTopicPartitionOffset)) {
      return false;
    }
    PubSubTopicPartitionOffset that = (PubSubTopicPartitionOffset) o;
    return pubSubTopicPartition.equals(that.getPubSubTopicPartition()) && offset.equals(that.offset);
  }
}
