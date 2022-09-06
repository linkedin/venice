package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Queue;
import org.apache.kafka.common.TopicPartition;


/**
 * A {@link PollStrategy} implementation which delivers messages in the order specified
 * at construction time.
 */
public class ArbitraryOrderingPollStrategy extends AbstractPollStrategy {
  private final Queue<Pair<TopicPartition, Long>> pollDeliveryOrder;

  public ArbitraryOrderingPollStrategy(Queue<Pair<TopicPartition, Long>> pollDeliveryOrder) {
    super(false);
    this.pollDeliveryOrder = pollDeliveryOrder;
  }

  @Override
  protected Pair<TopicPartition, Long> getNextPoll(Map<TopicPartition, Long> offsets) {
    if (offsets.isEmpty()) {
      // Not subscribed yet
      return null;
    }
    return pollDeliveryOrder.poll();
  }
}
