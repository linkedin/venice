package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Queue;
import org.apache.kafka.common.TopicPartition;

/**
 * A {@link PollStrategy} implementation which takes a queue of many poll strategies.
 *
 * It will drain each poll strategies in the order they are provided before moving on
 * to the next one.
 */
public class CompositePollStrategy extends AbstractPollStrategy {
  private final Queue<AbstractPollStrategy> pollStrategies;

  public CompositePollStrategy(Queue<AbstractPollStrategy> pollStrategies) {
    super(pollStrategies.stream().allMatch(pollStrategy -> pollStrategy.keepPollingWhenEmpty));
    this.pollStrategies = pollStrategies;
  }

  @Override
  protected Pair<TopicPartition, OffsetRecord> getNextPoll(Map<TopicPartition, OffsetRecord> offsets) {
    // We need to make sure some topic + partition has been subscribed before polling
    while (!pollStrategies.isEmpty() && !offsets.isEmpty()) {
      AbstractPollStrategy pollStrategy = pollStrategies.peek();
      Pair<TopicPartition, OffsetRecord> nextPoll = pollStrategy.getNextPoll(offsets);
      if (null == nextPoll) {
        pollStrategies.poll();
      } else {
        return nextPoll;
      }
    }
    return null;
  }
}
