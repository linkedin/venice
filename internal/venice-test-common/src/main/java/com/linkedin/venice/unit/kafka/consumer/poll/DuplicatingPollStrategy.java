package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PollStrategy} implementation which can introduce duplicates.
 *
 * The message payload is duplicated verbatim, but the Kafka offsets are incremented, as it would
 * happen in a real Kafka deployment.
 */
public class DuplicatingPollStrategy extends AbstractPollStrategy {
  private final AbstractPollStrategy basePollStrategy;
  private final Set<PubSubTopicPartitionOffset> PubSubTopicPartitionOffsetsToDuplicate;
  private final Map<PubSubTopicPartition, Long> amountOfIntroducedDupes = new HashMap<>();

  public DuplicatingPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<PubSubTopicPartitionOffset> PubSubTopicPartitionOffsetsToDuplicate) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.PubSubTopicPartitionOffsetsToDuplicate = PubSubTopicPartitionOffsetsToDuplicate;
  }

  @Override
  protected PubSubTopicPartitionOffset getNextPoll(Map<PubSubTopicPartition, Long> offsets) {
    PubSubTopicPartitionOffset nextPoll = basePollStrategy.getNextPoll(offsets);

    if (nextPoll == null) {
      return null;
    }

    PubSubTopicPartition topicPartition = nextPoll.getPubSubTopicPartition();
    long offset = nextPoll.getOffset();
    offset += getAmountOfDupes(topicPartition);

    PubSubTopicPartitionOffset nextPollWithAdjustedOffset = new PubSubTopicPartitionOffset(topicPartition, offset);

    if (PubSubTopicPartitionOffsetsToDuplicate.contains(nextPoll)) {
      if (!amountOfIntroducedDupes.containsKey(topicPartition)) {
        amountOfIntroducedDupes.put(topicPartition, 0L);
      }
      long previousAmountOfDupes = getAmountOfDupes(topicPartition);
      amountOfIntroducedDupes.put(topicPartition, previousAmountOfDupes + 1);
      PubSubTopicPartitionOffsetsToDuplicate.remove(nextPoll);
    }

    return nextPollWithAdjustedOffset;
  }

  private long getAmountOfDupes(PubSubTopicPartition PubSubTopicPartition) {
    return amountOfIntroducedDupes.getOrDefault(PubSubTopicPartition, 0L);
  }
}
