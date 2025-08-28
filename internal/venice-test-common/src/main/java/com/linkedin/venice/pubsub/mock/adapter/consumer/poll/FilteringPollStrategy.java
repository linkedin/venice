package com.linkedin.venice.pubsub.mock.adapter.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.MockInMemoryPartitionPosition;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PollStrategy} implementation which can pluck out records from the stream.
 */
public class FilteringPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<MockInMemoryPartitionPosition> topicPartitionOffsetsToFilterOut;

  public FilteringPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<MockInMemoryPartitionPosition> topicPartitionOffsetsToFilterOut) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.topicPartitionOffsetsToFilterOut = topicPartitionOffsetsToFilterOut;
    this.basePollStrategy = basePollStrategy;
  }

  @Override
  protected MockInMemoryPartitionPosition getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    MockInMemoryPartitionPosition nextPoll = basePollStrategy.getNextPoll(offsets);
    if (topicPartitionOffsetsToFilterOut.contains(nextPoll)) {
      incrementOffset(offsets, nextPoll.getPubSubTopicPartition(), nextPoll.getPubSubPosition());
      return getNextPoll(offsets);
    }
    return nextPoll;
  }
}
