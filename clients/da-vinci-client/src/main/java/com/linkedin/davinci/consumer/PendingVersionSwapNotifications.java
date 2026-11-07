package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
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
import java.util.function.Consumer;


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

  private final Map<Integer, Entry> pendingByPartition = new LinkedHashMap<>();

  /**
   * Records that {@code topicPartition} is now consumed from {@code position}, replacing any earlier entry for the
   * partition. Symbolic or null positions are recorded without a position.
   */
  void record(PubSubTopicPartition topicPartition, PubSubPosition position) {
    int partition = topicPartition.getPartitionNumber();
    String topic = topicPartition.getPubSubTopic().getName();
    VeniceChangeCoordinate coordinate =
        position == null || position.isSymbolic() ? null : new VeniceChangeCoordinate(topic, position, partition);
    pendingByPartition.put(partition, new Entry(topic, coordinate));
  }

  void remove(Collection<Integer> partitions) {
    pendingByPartition.keySet().removeAll(partitions);
  }

  void clear() {
    pendingByPartition.clear();
  }

  /**
   * Delivers all pending notifications to {@code listener}. If it throws, the failed and remaining notifications are
   * requeued and a {@link VeniceException} is thrown.
   */
  void deliver(Consumer<ChangelogVersionSwapNotification> listener, String storeName) {
    List<ChangelogVersionSwapNotification> notifications = drain();
    for (int i = 0; i < notifications.size(); i++) {
      try {
        listener.accept(notifications.get(i));
      } catch (Exception e) {
        requeue(notifications.subList(i, notifications.size()));
        throw new VeniceException(
            "Version swap listener failed for store: " + storeName + " on notification: " + notifications.get(i),
            e);
      }
    }
  }

  /**
   * Removes and returns all pending entries, grouped into one notification per topic.
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
   * Puts undelivered notifications back. A partition recorded since the notifications were drained keeps its newer
   * entry.
   */
  void requeue(List<ChangelogVersionSwapNotification> undelivered) {
    for (ChangelogVersionSwapNotification notification: undelivered) {
      for (int partition: notification.getPartitions()) {
        pendingByPartition.putIfAbsent(
            partition,
            new Entry(notification.getNewTopic(), notification.getNewTopicPositions().get(partition)));
      }
    }
  }
}
