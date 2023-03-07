package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Map;
import java.util.Queue;


/**
 * A {@link PollStrategy} implementation which delivers messages in the order specified
 * at construction time.
 */
public class ArbitraryOrderingPollStrategy extends AbstractPollStrategy {
  private final Queue<PubSubTopicPartitionOffset> pollDeliveryOrder;

  public ArbitraryOrderingPollStrategy(Queue<PubSubTopicPartitionOffset> pollDeliveryOrder) {
    super(false);
    this.pollDeliveryOrder = pollDeliveryOrder;
  }

  @Override
  protected PubSubTopicPartitionOffset getNextPoll(Map<PubSubTopicPartition, Long> offsets) {
    if (offsets.isEmpty()) {
      // Not subscribed yet
      return null;
    }
    return pollDeliveryOrder.poll();
  }
}
