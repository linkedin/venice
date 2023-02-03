package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PollStrategy} implementation which can pluck out records from the stream.
 */
public class FilteringPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<Pair<PubSubTopicPartition, Long>> topicPartitionOffsetsToFilterOut;

  public FilteringPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<Pair<PubSubTopicPartition, Long>> topicPartitionOffsetsToFilterOut) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.topicPartitionOffsetsToFilterOut = topicPartitionOffsetsToFilterOut;
  }

  @Override
  protected Pair<PubSubTopicPartition, Long> getNextPoll(Map<PubSubTopicPartition, Long> offsets) {
    Pair<PubSubTopicPartition, Long> nextPoll = basePollStrategy.getNextPoll(offsets);
    if (topicPartitionOffsetsToFilterOut.contains(nextPoll)) {
      incrementOffset(offsets, nextPoll.getFirst(), nextPoll.getSecond());
      return getNextPoll(offsets);
    }
    return nextPoll;
  }
}
