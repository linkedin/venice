package com.linkedin.venice.pubsub.mock.adapter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import java.util.Objects;


public class MockInMemoryPartitionPosition {
  private final PubSubTopicPartition pubSubTopicPartition;
  private final InMemoryPubSubPosition pubSubPosition;

  private final int hashCode;

  public MockInMemoryPartitionPosition(PubSubTopicPartition pubSubTopicPartition, PubSubPosition pubSubPosition) {
    this.pubSubTopicPartition = pubSubTopicPartition;

    if (!(pubSubPosition instanceof InMemoryPubSubPosition)) {
      throw new IllegalArgumentException("PubSubPosition must be an instance of InMemoryPubSubPosition");
    }
    this.pubSubPosition = (InMemoryPubSubPosition) pubSubPosition;
    this.hashCode = Objects.hash(pubSubTopicPartition, pubSubPosition);
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return this.pubSubTopicPartition;
  }

  public InMemoryPubSubPosition getPubSubPosition() {
    return pubSubPosition;
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
    if (o == null || !(o instanceof MockInMemoryPartitionPosition)) {
      return false;
    }
    MockInMemoryPartitionPosition that = (MockInMemoryPartitionPosition) o;
    return pubSubTopicPartition.equals(that.getPubSubTopicPartition()) && pubSubPosition.equals(that.pubSubPosition);
  }
}
