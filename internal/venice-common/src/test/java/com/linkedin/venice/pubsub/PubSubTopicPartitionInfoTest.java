package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PubSubTopicPartitionInfo}.
 */
public class PubSubTopicPartitionInfoTest {
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
}
