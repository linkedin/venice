package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


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
  protected PubSubTopicPartitionOffset getNextPoll(Map<PubSubTopicPartition, Long> offsets) {
    if (offsets.isEmpty()) {
      Utils.sleep(50); // So that keepPollingWhenEmpty doesn't lead to 10 null polls per ms
      return null;
    }
    List<PubSubTopicPartition> PubSubTopicPartitionList =
        Arrays.asList(offsets.keySet().toArray(new PubSubTopicPartition[] {}));
    int numberOfPubSubTopicPartitions = offsets.size();
    PubSubTopicPartition pubSubTopicPartition =
        PubSubTopicPartitionList.get((int) Math.round(Math.random() * (numberOfPubSubTopicPartitions - 1)));
    Long offset = offsets.get(pubSubTopicPartition);

    return new PubSubTopicPartitionOffset(pubSubTopicPartition, offset);
  }
}
