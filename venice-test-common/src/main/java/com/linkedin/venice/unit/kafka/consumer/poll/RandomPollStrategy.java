package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/**
 * A simple {@link PollStrategy} which delivers messages from any partition, while respecting the
 * ordering guarantee of individual partitions.
 *
 * The resulting consumption order is non-deterministic.
 */
public class RandomPollStrategy extends AbstractPollStrategy {
  public RandomPollStrategy() {
    super(true); // TODO: Change default to false once tests are ensured to be deterministic...
  }

  public RandomPollStrategy(int maxMessagePerPoll) {
    super(true, maxMessagePerPoll);
  }

  public RandomPollStrategy(boolean keepPollingWhenEmpty) {
    super(keepPollingWhenEmpty);
  }

  @Override
  protected Pair<TopicPartition, OffsetRecord> getNextPoll(Map<TopicPartition, OffsetRecord> offsets) {
    if (offsets.isEmpty()) {
      return null;
    }
    List<TopicPartition> topicPartitionList = Arrays.asList(offsets.keySet().toArray(new TopicPartition[]{}));
    int numberOfTopicPartitions = offsets.size();
    TopicPartition topicPartition = topicPartitionList.get((int) Math.round(Math.random() * (numberOfTopicPartitions - 1)));
    OffsetRecord offsetRecord = offsets.get(topicPartition);

    return new Pair<>(topicPartition, offsetRecord);
  }
}
