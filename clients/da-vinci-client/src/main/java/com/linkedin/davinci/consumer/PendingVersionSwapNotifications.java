package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Undelivered {@link ChangelogVersionSwapNotification}s, holding at most one entry per partition: the latest topic it
 * was subscribed to. Not thread-safe; callers must serialize access.
 */
class PendingVersionSwapNotifications {
  private static class Entry {
    private final String topic;
    private final VeniceChangeCoordinate position;

    private Entry(String topic, VeniceChangeCoordinate position) {
      this.topic = topic;
      this.position = position;
    }
  }

  private Map<Integer, Entry> pendingByPartition = new LinkedHashMap<>();

  /**
   * Records that {@code topicPartition} is now consumed from {@code position}, replacing any earlier entry for the
   * partition. Symbolic or null positions are recorded without a position.
   */
  void record(PubSubTopicPartition topicPartition, PubSubPosition position) {
    int partition = topicPartition.getPartitionNumber();
    String topic = topicPartition.getPubSubTopic().getName();
    VeniceChangeCoordinate coordinate =
        position == null || position.isSymbolic() ? null : new VeniceChangeCoordinate(topic, position, partition);
    pendingByPartition.remove(partition);
    pendingByPartition.put(partition, new Entry(topic, coordinate));
  }

  void remove(Collection<Integer> partitions) {
    pendingByPartition.keySet().removeAll(partitions);
  }

  void clear() {
    pendingByPartition.clear();
  }

  /**
   * Removes and returns all pending entries, grouped into one notification per topic in recording order.
   */
  List<ChangelogVersionSwapNotification> drain() {
    if (pendingByPartition.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, Set<Integer>> partitionsByTopic = new LinkedHashMap<>();
    Map<String, Map<Integer, VeniceChangeCoordinate>> positionsByTopic = new HashMap<>();
    for (Map.Entry<Integer, Entry> pending: pendingByPartition.entrySet()) {
      Entry entry = pending.getValue();
      partitionsByTopic.computeIfAbsent(entry.topic, t -> new HashSet<>()).add(pending.getKey());
      if (entry.position != null) {
        positionsByTopic.computeIfAbsent(entry.topic, t -> new HashMap<>()).put(pending.getKey(), entry.position);
      }
    }
    pendingByPartition.clear();
    List<ChangelogVersionSwapNotification> notifications = new ArrayList<>(partitionsByTopic.size());
    for (Map.Entry<String, Set<Integer>> topicPartitions: partitionsByTopic.entrySet()) {
      notifications.add(
          new ChangelogVersionSwapNotification(
              topicPartitions.getKey(),
              topicPartitions.getValue(),
              positionsByTopic.getOrDefault(topicPartitions.getKey(), Collections.emptyMap())));
    }
    return notifications;
  }

  /**
   * Puts undelivered notifications back ahead of the current entries. A partition recorded since the notifications
   * were drained keeps its newer entry.
   */
  void requeue(List<ChangelogVersionSwapNotification> undelivered) {
    Map<Integer, Entry> requeued = new LinkedHashMap<>();
    for (ChangelogVersionSwapNotification notification: undelivered) {
      for (int partition: notification.getPartitions()) {
        requeued.remove(partition);
        requeued
            .put(partition, new Entry(notification.getNewTopic(), notification.getNewTopicPositions().get(partition)));
      }
    }
    for (Map.Entry<Integer, Entry> pending: pendingByPartition.entrySet()) {
      requeued.remove(pending.getKey());
      requeued.put(pending.getKey(), pending.getValue());
    }
    pendingByPartition = requeued;
  }
}
