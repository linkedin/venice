package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.testng.annotations.Test;


public class PubSubTopicRepositoryTest {
  @Test
  public void testGetTopicReturnsSameInstance() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic topic1 = repo.getTopic("test_store_v1");
    PubSubTopic topic2 = repo.getTopic("test_store_v1");
    assertSame(topic1, topic2);
  }

  @Test
  public void testGetTopicPartitionReturnsSameInstance() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic topic = repo.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = repo.getTopicPartition(topic, 0);
    PubSubTopicPartition tp2 = repo.getTopicPartition(topic, 0);
    assertSame(tp1, tp2);
    assertEquals(tp1.getPubSubTopic(), topic);
    assertEquals(tp1.getPartitionNumber(), 0);
  }

  @Test
  public void testGetTopicPartitionDifferentPartitions() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic topic = repo.getTopic("test_store_v1");
    PubSubTopicPartition tp0 = repo.getTopicPartition(topic, 0);
    PubSubTopicPartition tp1 = repo.getTopicPartition(topic, 1);
    assertNotNull(tp0);
    assertNotNull(tp1);
    assertEquals(tp0.getPartitionNumber(), 0);
    assertEquals(tp1.getPartitionNumber(), 1);
    assertSame(tp0.getPubSubTopic(), tp1.getPubSubTopic());
  }

  @Test
  public void testGetTopicPartitionDifferentTopics() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic topicA = repo.getTopic("store_a_v1");
    PubSubTopic topicB = repo.getTopic("store_b_v1");
    PubSubTopicPartition tpA = repo.getTopicPartition(topicA, 0);
    PubSubTopicPartition tpB = repo.getTopicPartition(topicB, 0);
    assertSame(tpA.getPubSubTopic(), topicA);
    assertSame(tpB.getPubSubTopic(), topicB);
  }

  @Test
  public void testGetTopicPartitionNormalizesNonCanonicalTopic() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic canonical = repo.getTopic("test_store_v1");
    // Create a non-canonical PubSubTopic with the same name
    PubSubTopic nonCanonical = new PubSubTopicImpl("test_store_v1");
    assertNotSame(canonical, nonCanonical);

    PubSubTopicPartition tp = repo.getTopicPartition(nonCanonical, 0);
    // Should use the canonical topic, not the non-canonical one
    assertSame(tp.getPubSubTopic(), canonical);
    // Should return the same instance as using the canonical topic directly
    assertSame(tp, repo.getTopicPartition(canonical, 0));
  }

  @Test
  public void testGetTopicPartitionByStringReturnsSameInstance() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp1 = repo.getTopicPartition("test_store_v1", 3);
    PubSubTopicPartition tp2 = repo.getTopicPartition("test_store_v1", 3);
    assertSame(tp1, tp2);
    assertSame(tp1.getPubSubTopic(), repo.getTopic("test_store_v1"));
    assertEquals(tp1.getPartitionNumber(), 3);

    // String overload and PubSubTopic overload return the same instance
    PubSubTopic topic = repo.getTopic("test_store_v1");
    assertSame(tp1, repo.getTopicPartition(topic, 3));
  }

  @Test
  public void testGetTopicPartitionConcurrentAccess() throws Exception {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic topic = repo.getTopic("concurrent_store_v1");
    int threadCount = 10;
    int partitionCount = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(1);

    // Use AtomicReferenceArray for identity-based tracking (not value-based equals/hashCode)
    AtomicReferenceArray<PubSubTopicPartition> firstSeen = new AtomicReferenceArray<>(partitionCount);
    ConcurrentHashMap<Integer, Boolean> duplicateDetected = new ConcurrentHashMap<>();
    List<Future<?>> futures = new ArrayList<>();

    try {
      for (int t = 0; t < threadCount; t++) {
        futures.add(executor.submit(() -> {
          try {
            latch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          for (int p = 0; p < partitionCount; p++) {
            PubSubTopicPartition tp = repo.getTopicPartition(topic, p);
            if (!firstSeen.compareAndSet(p, null, tp)) {
              // Another thread already recorded an instance — verify same reference
              if (firstSeen.get(p) != tp) {
                duplicateDetected.put(p, true);
              }
            }
          }
        }));
      }

      latch.countDown();
      for (Future<?> f: futures) {
        f.get();
      }
    } finally {
      executor.shutdown();
    }

    // No partition should have seen different instances across threads
    assertEquals(duplicateDetected.size(), 0, "Duplicate instances detected for partitions: " + duplicateDetected.keySet());
    for (int p = 0; p < partitionCount; p++) {
      assertSame(repo.getTopicPartition(topic, p), firstSeen.get(p));
    }
  }
}
