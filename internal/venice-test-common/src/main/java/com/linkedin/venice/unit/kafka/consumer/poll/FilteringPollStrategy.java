package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.unit.kafka.InMemoryPubSubPosition;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PollStrategy} implementation which can pluck out records from the stream.
 */
public class FilteringPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<PubSubTopicPartitionOffset> topicPartitionOffsetsToFilterOut;

  public FilteringPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<PubSubTopicPartitionOffset> topicPartitionOffsetsToFilterOut) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.topicPartitionOffsetsToFilterOut = topicPartitionOffsetsToFilterOut;
    this.basePollStrategy = basePollStrategy;

  }

  @Override
  protected PubSubTopicPartitionOffset getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    PubSubTopicPartitionOffset nextPoll = basePollStrategy.getNextPoll(offsets);
    if (topicPartitionOffsetsToFilterOut.contains(nextPoll)) {
      incrementOffset(offsets, nextPoll.getPubSubTopicPartition(), nextPoll.getPubSubPosition());
      return getNextPoll(offsets);
    }
    return nextPoll;
  }
}
