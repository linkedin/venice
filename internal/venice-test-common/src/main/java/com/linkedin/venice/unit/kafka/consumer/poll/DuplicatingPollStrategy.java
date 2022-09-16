package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;


/**
 * A {@link PollStrategy} implementation which can introduce duplicates.
 *
 * The message payload is duplicated verbatim, but the Kafka offsets are incremented, as it would
 * happen in a real Kafka deployment.
 */
public class DuplicatingPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<Pair<TopicPartition, Long>> topicPartitionOffsetsToDuplicate;
  private final Map<TopicPartition, Long> amountOfIntroducedDupes = new HashMap<>();

  public DuplicatingPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<Pair<TopicPartition, Long>> topicPartitionOffsetsToDuplicate) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.topicPartitionOffsetsToDuplicate = topicPartitionOffsetsToDuplicate;
  }

  @Override
  protected Pair<TopicPartition, Long> getNextPoll(Map<TopicPartition, Long> offsets) {
    Pair<TopicPartition, Long> nextPoll = basePollStrategy.getNextPoll(offsets);

    if (nextPoll == null) {
      return null;
    }

    TopicPartition topicPartition = nextPoll.getFirst();
    long offset = nextPoll.getSecond();
    offset += getAmountOfDupes(topicPartition);

    Pair<TopicPartition, Long> nextPollWithAdjustedOffset = new Pair<>(topicPartition, offset);

    if (topicPartitionOffsetsToDuplicate.contains(nextPoll)) {
      if (!amountOfIntroducedDupes.containsKey(topicPartition)) {
        amountOfIntroducedDupes.put(topicPartition, 0L);
      }
      long previousAmountOfDupes = getAmountOfDupes(topicPartition);
      amountOfIntroducedDupes.put(topicPartition, previousAmountOfDupes + 1);
      topicPartitionOffsetsToDuplicate.remove(nextPoll);
    }

    return nextPollWithAdjustedOffset;
  }

  private long getAmountOfDupes(TopicPartition topicPartition) {
    return amountOfIntroducedDupes.getOrDefault(topicPartition, 0L);
  }
}
