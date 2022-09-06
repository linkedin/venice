package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;


/**
 * A {@link PollStrategy} implementation which can pluck out records from the stream.
 */
public class FilteringPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<Pair<TopicPartition, Long>> topicPartitionOffsetsToFilterOut;

  public FilteringPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<Pair<TopicPartition, Long>> topicPartitionOffsetsToFilterOut) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.topicPartitionOffsetsToFilterOut = topicPartitionOffsetsToFilterOut;
  }

  @Override
  protected Pair<TopicPartition, Long> getNextPoll(Map<TopicPartition, Long> offsets) {
    Pair<TopicPartition, Long> nextPoll = basePollStrategy.getNextPoll(offsets);
    if (topicPartitionOffsetsToFilterOut.contains(nextPoll)) {
      incrementOffset(offsets, nextPoll.getFirst(), nextPoll.getSecond());
      return getNextPoll(offsets);
    }
    return nextPoll;
  }
}
