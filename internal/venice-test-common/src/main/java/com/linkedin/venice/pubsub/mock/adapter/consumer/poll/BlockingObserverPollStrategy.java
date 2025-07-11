package com.linkedin.venice.pubsub.mock.adapter.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.MockInMemoryPartitionPosition;
import java.util.Map;
import java.util.function.Consumer;


/**
 * This {@link PollStrategy} delegates polling to another implementation, and also executes
 * an arbitrary function during each poll. This function is only allowed to observe, not
 * to tamper with the data being polled. The function is executed synchronously, thus
 * making it easy to reason about the state of the consumption stream at the time of the
 * function's execution.
 */
public class BlockingObserverPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Consumer<MockInMemoryPartitionPosition> observer;

  public BlockingObserverPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Consumer<MockInMemoryPartitionPosition> observer) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.observer = observer;
  }

  @Override
  protected MockInMemoryPartitionPosition getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    MockInMemoryPartitionPosition nextPoll = basePollStrategy.getNextPoll(offsets);
    observer.accept(nextPoll);
    return nextPoll;
  }
}
