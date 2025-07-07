package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.unit.kafka.InMemoryPubSubPosition;
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
  private final Set<PubSubTopicPartitionOffset> partitionOffsets;
  private final Map<PubSubTopicPartition, Long> amountOfIntroducedDupes = new HashMap<>();

  public DuplicatingPollStrategy(
      AbstractPollStrategy basePollStrategy,
      Set<PubSubTopicPartitionOffset> partitionOffsets) {
    super(basePollStrategy.keepPollingWhenEmpty);
    this.basePollStrategy = basePollStrategy;
    this.partitionOffsets = partitionOffsets;
  }

  @Override
  protected PubSubTopicPartitionOffset getNextPoll(Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets) {
    PubSubTopicPartitionOffset nextPoll = basePollStrategy.getNextPoll(offsets);

    if (nextPoll == null) {
      return null;
    }

    PubSubTopicPartition topicPartition = nextPoll.getPubSubTopicPartition();
    InMemoryPubSubPosition offset = nextPoll.getPubSubPosition();
    offset = offset.getPositionAfterNRecords(getAmountOfDupes(topicPartition));

    PubSubTopicPartitionOffset nextPollWithAdjustedOffset = new PubSubTopicPartitionOffset(topicPartition, offset);

    if (partitionOffsets.contains(nextPoll)) {
      if (!amountOfIntroducedDupes.containsKey(topicPartition)) {
        amountOfIntroducedDupes.put(topicPartition, 0L);
      }
      long previousAmountOfDupes = getAmountOfDupes(topicPartition);
      amountOfIntroducedDupes.put(topicPartition, previousAmountOfDupes + 1);
      partitionOffsets.remove(nextPoll);
    }

    return nextPollWithAdjustedOffset;
  }

  private long getAmountOfDupes(PubSubTopicPartition topicPartition) {
    return amountOfIntroducedDupes.getOrDefault(topicPartition, 0L);
  }
}
