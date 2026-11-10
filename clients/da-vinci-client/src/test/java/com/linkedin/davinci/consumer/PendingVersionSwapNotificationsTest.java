package com.linkedin.davinci.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public class PendingVersionSwapNotificationsTest {
  @Test
  public void testPartitionsUnsubscribedByListenerAreNotDeliveredOrRequeued() {
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PendingVersionSwapNotifications pending = new PendingVersionSwapNotifications();
    pending.record(new PubSubTopicPartitionImpl(topicRepository.getTopic("store_v1"), 0), null);
    pending.record(new PubSubTopicPartitionImpl(topicRepository.getTopic("store_v2"), 1), null);
    List<ChangelogVersionSwapNotification> delivered = new ArrayList<>();

    expectThrows(VeniceException.class, () -> pending.deliver(notification -> {
      delivered.add(notification);
      pending.remove(Collections.singleton(1));
      pending.remove(Collections.singleton(0));
      throw new IllegalStateException("listener failure");
    }, "store"));

    assertEquals(delivered.size(), 1);
    assertTrue(pending.drain().isEmpty());
  }
}
