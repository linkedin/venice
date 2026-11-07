package com.linkedin.davinci.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class PendingVersionSwapNotificationsTest {
  private final PubSubTopicRepository topicRepository = new PubSubTopicRepository();
  private final PubSubTopic v1 = topicRepository.getTopic("store_v1");
  private final PubSubTopic v2 = topicRepository.getTopic("store_v2");

  @Test
  public void testDrainGroupsByTopicAndKeepsLatestEntryPerPartition() {
    PendingVersionSwapNotifications pending = new PendingVersionSwapNotifications();
    pending.record(partition(v1, 0), null);
    pending.record(partition(v1, 1), PubSubSymbolicPosition.EARLIEST);
    pending.record(partition(v1, 2), ApacheKafkaOffsetPosition.of(7L));
    pending.record(partition(v2, 0), ApacheKafkaOffsetPosition.of(3L));

    List<ChangelogVersionSwapNotification> drained = pending.drain();
    assertEquals(
        drained,
        Arrays.asList(
            new ChangelogVersionSwapNotification(
                v1.getName(),
                new HashSet<>(Arrays.asList(1, 2)),
                Collections.singletonMap(2, coordinate(v1, 2, 7L))),
            new ChangelogVersionSwapNotification(
                v2.getName(),
                Collections.singleton(0),
                Collections.singletonMap(0, coordinate(v2, 0, 3L)))));
    assertTrue(pending.drain().isEmpty());
  }

  @Test
  public void testRemoveAndClear() {
    PendingVersionSwapNotifications pending = new PendingVersionSwapNotifications();
    pending.record(partition(v1, 0), null);
    pending.record(partition(v1, 1), null);
    pending.remove(Collections.singleton(1));
    assertEquals(
        pending.drain(),
        Collections.singletonList(
            new ChangelogVersionSwapNotification(v1.getName(), Collections.singleton(0), Collections.emptyMap())));

    pending.record(partition(v1, 0), null);
    pending.clear();
    assertTrue(pending.drain().isEmpty());
  }

  @Test
  public void testRequeueKeepsNewerEntries() {
    PendingVersionSwapNotifications pending = new PendingVersionSwapNotifications();
    pending.record(partition(v1, 0), ApacheKafkaOffsetPosition.of(1L));
    pending.record(partition(v1, 1), ApacheKafkaOffsetPosition.of(2L));
    List<ChangelogVersionSwapNotification> undelivered = pending.drain();

    // Partition 1 moves again before the undelivered notification is requeued; its newer entry must win.
    pending.record(partition(v2, 1), ApacheKafkaOffsetPosition.of(5L));
    pending.requeue(undelivered);

    Map<Integer, VeniceChangeCoordinate> v1Positions = new HashMap<>();
    v1Positions.put(0, coordinate(v1, 0, 1L));
    assertEquals(
        pending.drain(),
        Arrays.asList(
            new ChangelogVersionSwapNotification(v1.getName(), Collections.singleton(0), v1Positions),
            new ChangelogVersionSwapNotification(
                v2.getName(),
                Collections.singleton(1),
                Collections.singletonMap(1, coordinate(v2, 1, 5L)))));
  }

  private static PubSubTopicPartition partition(PubSubTopic topic, int partition) {
    return new PubSubTopicPartitionImpl(topic, partition);
  }

  private static VeniceChangeCoordinate coordinate(PubSubTopic topic, int partition, long offset) {
    return new VeniceChangeCoordinate(topic.getName(), ApacheKafkaOffsetPosition.of(offset), partition);
  }
}
