package com.linkedin.venice.pubsub.mock.adapter.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.MockInMemoryPartitionPosition;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;


/**
 * A {@link PollStrategy} implementation which takes a queue of many poll strategies.
 *
 * It will drain each poll strategies in the order they are provided before moving on
 * to the next one.
 */
public class CompositePollStrategy extends AbstractPollStrategy {
  private final Queue<AbstractPollStrategy> pollStrategies;

  public CompositePollStrategy(Queue<AbstractPollStrategy> pollStrategies) {
    super(pollStrategies.stream().allMatch(pollStrategy -> pollStrategy.keepPollingWhenEmpty));
    this.pollStrategies = pollStrategies;
  }

  @Nullable
  @Override
  protected MockInMemoryPartitionPosition getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    // We need to make sure some topic + partition has been subscribed before polling
    while (!pollStrategies.isEmpty() && !offsets.isEmpty()) {
      AbstractPollStrategy pollStrategy = pollStrategies.peek();
      MockInMemoryPartitionPosition nextPoll = pollStrategy.getNextPoll(offsets);
      if (nextPoll != null) {
        return nextPoll;
      }
      pollStrategies.remove();
    }
    return null;
  }
}
