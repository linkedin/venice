package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This class maintains a map of all the subscribed topic partitions and the timestamp when it was subscribed. The
 * intention is to detect topic partitions that are subscribed but received no messages for an extended period of time
 * due to any of the following reasons:
 *   1. Starvation due to shared consumer
 *   2. Pub-sub broker or client issues
 *   3. Code bug
 * All subscribed topic partitions are expected to receive messages from polls because:
 *   1. RT topics have heartbeat messages produced to them periodically. VT for hybrid stores will receive them too.
 *   2. VT for batch only stores should unsubscribe after completion (EOP received).
 * Once message(s) are received the corresponding topic partition is removed from the tracking map.
 * TODO: Currently the tracker is unable to differentiate RT topic subscriptions across different versions. e.g. current
 * version and future version leader for a given partition is on the same host and subscribed to different offset of the
 * RT topic. Current version is able to poll successfully and future version cannot. The current implementation is
 * able to detect this but unable to report exactly which subscription is having trouble polling.
 */
@Threadsafe
public class ConsumerPollTracker {
  private final VeniceConcurrentHashMap<PubSubTopicPartition, Long> trackingMap = new VeniceConcurrentHashMap<>();
  private final Time time;

  public ConsumerPollTracker(Time time) {
    this.time = time;
  }

  /**
   * Record the subscribe timestamp for a given topic partition.
   * @param pubSubTopicPartition to record the activity for.
   */
  public void recordSubscribed(PubSubTopicPartition pubSubTopicPartition) {
    trackingMap.put(pubSubTopicPartition, time.getMilliseconds());
  }

  public void recordMessageReceived(PubSubTopicPartition pubSubTopicPartition) {
    trackingMap.computeIfPresent(pubSubTopicPartition, (topicPartition, timestamp) -> null);
  }

  public void removeTopicPartition(PubSubTopicPartition pubSubTopicPartition) {
    trackingMap.remove(pubSubTopicPartition);
  }

  /**
   * @param thresholdTimestamp to get topic partitions with older last activity timestamp than the threshold timestamp.
   * @return a map of topic partitions with last successful activity timestamp older than the provided timestamp.
   */
  public Map<PubSubTopicPartition, Long> getStaleTopicPartitions(long thresholdTimestamp) {
    return trackingMap.entrySet()
        .stream()
        .filter(entry -> entry.getValue() < thresholdTimestamp)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
