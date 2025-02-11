package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import java.util.PriorityQueue;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PubSubTopicPartitionInfo}.
 */
public class PubSubTopicPartitionInfoTest {
  Random rand = new Random();

  @Test
  public void testTopic() {
    PubSubTopic pubSubTopic = new PubSubTopicImpl("testTopic");
    PubSubTopicPartitionInfo partitionInfo = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopic result = partitionInfo.topic();
    assertEquals(result, pubSubTopic);
  }

  @Test
  public void testPartition() {
    PubSubTopicPartitionInfo partitionInfo = new PubSubTopicPartitionInfo(new PubSubTopicImpl("testTopic"), 2, true);
    assertEquals(partitionInfo.partition(), 2);
  }

  @Test
  public void testHasInSyncReplicas() {
    PubSubTopicPartitionInfo partitionInfo = new PubSubTopicPartitionInfo(new PubSubTopicImpl("testTopic"), 0, false);
    assertFalse(partitionInfo.hasInSyncReplicas());
  }

  @Test
  public void testToString() {
    PubSubTopicPartitionInfo partitionInfo = new PubSubTopicPartitionInfo(new PubSubTopicImpl("testTopic"), 0, true);
    assertEquals(partitionInfo.toString(), "Partition(topic = testTopic, partition=0, hasInSyncReplica = true)");
  }

  @Test
  public void testTopicDeletionPriority() {
    PriorityQueue<PubSubTopic> allTopics = new PriorityQueue<>(TopicCleanupService.topicPriorityComparator);
    int numberOfRTTopicsInTest = 0;

    for (int i = 0; i < 100; i++) {
      int randomNum = rand.nextInt(2);
      if (randomNum == 0) {
        allTopics.add(new PubSubTopicImpl("topic" + i + Version.REAL_TIME_TOPIC_SUFFIX));
        numberOfRTTopicsInTest++;
      } else {
        allTopics.add(new PubSubTopicImpl("topic" + i));
      }
    }

    int i = 0;
    while (!allTopics.isEmpty()) {
      if (i++ < numberOfRTTopicsInTest) {
        Assert.assertTrue(allTopics.poll().isRealTime());
      } else {
        Assert.assertFalse(allTopics.poll().isRealTime());
      }
    }
  }
}
