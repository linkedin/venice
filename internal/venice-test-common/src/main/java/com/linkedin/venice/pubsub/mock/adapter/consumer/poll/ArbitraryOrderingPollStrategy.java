package com.linkedin.venice.pubsub.mock.adapter.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.MockInMemoryPartitionPosition;
import java.util.Map;
import java.util.Queue;


/**
 * A {@link PollStrategy} implementation which delivers messages in the order specified
 * at construction time.
 */
public class ArbitraryOrderingPollStrategy extends AbstractPollStrategy {
  private final Queue<MockInMemoryPartitionPosition> pollDeliveryOrder;

  public ArbitraryOrderingPollStrategy(Queue<MockInMemoryPartitionPosition> pollDeliveryOrder) {
    super(false);
    this.pollDeliveryOrder = pollDeliveryOrder;
  }

  @Override
  protected MockInMemoryPartitionPosition getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    if (offsets.isEmpty()) {
      // Not subscribed yet
      return null;
    }
    return pollDeliveryOrder.poll();
  }
}
