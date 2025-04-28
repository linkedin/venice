package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;


public class ConsumerPollTrackerReportingTask implements Runnable {
  private static final long DEFAULT_CONSUMER_POLL_TRACKER_CHECK_CYCLE = TimeUnit.MINUTES.toMillis(1);
  private static final String WARNING_MESSAGE_PREFIX = "Consumer poll tracker found stale topic partitions: ";
  private final ConsumerPollTracker consumerPollTracker;
  private final Time time;
  private final long staleThresholdMs;
  private final Logger LOGGER;
  private final StringBuilder stringBuilder = new StringBuilder();
  private volatile boolean reportingTaskStopped = false;

  public ConsumerPollTrackerReportingTask(
      ConsumerPollTracker consumerPollTracker,
      Time time,
      long staleThresholdMs,
      Logger logger) {
    this.consumerPollTracker = consumerPollTracker;
    this.time = time;
    this.staleThresholdMs = staleThresholdMs;
    this.LOGGER = logger;
  }

  public void stop() {
    reportingTaskStopped = true;
  }

  @Override
  public void run() {
    while (!reportingTaskStopped) {
      try {
        time.sleep(DEFAULT_CONSUMER_POLL_TRACKER_CHECK_CYCLE);
        long now = time.getMilliseconds();
        Map<PubSubTopicPartition, Long> staleTopicPartitions =
            consumerPollTracker.getStaleTopicPartitions(now - staleThresholdMs);
        if (!staleTopicPartitions.isEmpty()) {
          stringBuilder.append(WARNING_MESSAGE_PREFIX);
          for (Map.Entry<PubSubTopicPartition, Long> entry: staleTopicPartitions.entrySet()) {
            stringBuilder.append("\n topic: ");
            stringBuilder.append(entry.getKey().getTopicName());
            stringBuilder.append(" partition: ");
            stringBuilder.append(entry.getKey().getPartitionNumber());
            stringBuilder.append(" stale for: ");
            stringBuilder.append(now - entry.getValue());
            stringBuilder.append("ms");
          }
          LOGGER.warn(stringBuilder.toString());
          // clear the StringBuilder to be reused for next reporting cycle
          stringBuilder.setLength(0);
        }
      } catch (InterruptedException e) {
        break;
      }
    }
  }
}
