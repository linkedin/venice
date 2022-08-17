package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;
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

  @Nullable
  @Override
  protected Pair<TopicPartition, Long> getNextPoll(Map<TopicPartition, Long> offsets) {
    // We need to make sure some topic + partition has been subscribed before polling
    while (!pollStrategies.isEmpty() && !offsets.isEmpty()) {
      AbstractPollStrategy pollStrategy = pollStrategies.peek();
      Pair<TopicPartition, Long> nextPoll = pollStrategy.getNextPoll(offsets);
      if (nextPoll != null) {
        return nextPoll;
      }
      pollStrategies.remove();
    }
    return null;
  }
}
