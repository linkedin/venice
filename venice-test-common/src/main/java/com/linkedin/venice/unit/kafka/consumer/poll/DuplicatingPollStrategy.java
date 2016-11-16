package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.offsets.OffsetRecord;
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
  private final Set<Pair<TopicPartition, OffsetRecord>> topicPartitionOffsetsToDuplicate;
  private final Map<TopicPartition, Long> amountOfIntroducedDupes = new HashMap<>();

  public DuplicatingPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<Pair<TopicPartition, OffsetRecord>> topicPartitionOffsetsToDuplicate) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.topicPartitionOffsetsToDuplicate = topicPartitionOffsetsToDuplicate;
  }

  @Override
  protected Pair<TopicPartition, OffsetRecord> getNextPoll(Map<TopicPartition, OffsetRecord> offsets) {
    Pair<TopicPartition, OffsetRecord> nextPoll = basePollStrategy.getNextPoll(offsets);

    if (null == nextPoll) {
      return null;
    }

    TopicPartition topicPartition = nextPoll.getFirst();
    OffsetRecord offsetRecord = nextPoll.getSecond();
    offsetRecord.setOffset(offsetRecord.getOffset() + getAmountOfDupes(topicPartition));

    Pair<TopicPartition, OffsetRecord> nextPollWithAdjustedOffset = new Pair<>(topicPartition, offsetRecord);

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
