package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.testng.annotations.Test;


public class PubSubTopicPartitionImplTest {
  @Test
  public void testEqualsAndHashCode() {
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopic topic1 = pubSubTopicRepository.getTopic("1_rt");
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(topic1, 0);
    assertEquals(pubSubTopicPartition1, pubSubTopicPartition1);
    assertFalse(pubSubTopicPartition1.equals(null));
    assertFalse(pubSubTopicPartition1.equals(new Object()));

    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(topic1, 0);
    assertEquals(pubSubTopicPartition1, pubSubTopicPartition2);
    assertEquals(pubSubTopicPartition1.hashCode(), pubSubTopicPartition2.hashCode());

    PubSubTopicPartition pubSubTopicPartition3 = new PubSubTopicPartitionImpl(topic1, 1);
    assertNotEquals(pubSubTopicPartition1, pubSubTopicPartition3);

    PubSubTopic topic2 = pubSubTopicRepository.getTopic("2_rt");
    PubSubTopicPartition pubSubTopicPartition4 = new PubSubTopicPartitionImpl(topic2, 0);
    assertNotEquals(pubSubTopicPartition1, pubSubTopicPartition4);
  }
}
