package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import org.testng.annotations.Test;


public class PubSubTopicTest {
  @Test
  public void testEqualsAndHashCode() {
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopic topic1 = pubSubTopicRepository.getTopic("1_rt");
    PubSubTopic topic2 = pubSubTopicRepository.getTopic("1_rt");
    assertEquals(topic1, topic2);
    assertEquals(topic1.hashCode(), topic2.hashCode());
    assertTrue(topic1 == topic2); // Should cache the same instances
    assertFalse(topic1.equals(null));
    assertFalse(topic1.equals(new Object()));
    assertTrue(topic1.isRealTime());
    assertFalse(topic1.isVersionTopic());
    assertFalse(topic1.isStreamReprocessingTopic());
    assertFalse(topic1.isVersionTopicOrStreamReprocessingTopic());

    PubSubTopic topic3 = pubSubTopicRepository.getTopic("2_rt");
    assertNotEquals(topic1, topic3);

    PubSubTopic topic4 = pubSubTopicRepository.getTopic("1_v1");
    assertNotEquals(topic1, topic4);
    assertNotEquals(topic3, topic4);
    assertFalse(topic4.isRealTime());
    assertTrue(topic4.isVersionTopic());
    assertFalse(topic4.isStreamReprocessingTopic());
    assertTrue(topic4.isVersionTopicOrStreamReprocessingTopic());

    PubSubTopic topic5 = pubSubTopicRepository.getTopic("1_v1_sr");
    assertNotEquals(topic1, topic5);
    assertNotEquals(topic3, topic5);
    assertNotEquals(topic4, topic5);
    assertFalse(topic5.isRealTime());
    assertFalse(topic5.isVersionTopic());
    assertTrue(topic5.isStreamReprocessingTopic());
    assertTrue(topic5.isVersionTopicOrStreamReprocessingTopic());

    PubSubTopic topic6 = pubSubTopicRepository.getTopic("1_v1_cc");
    assertNotEquals(topic1, topic6);
    assertNotEquals(topic3, topic6);
    assertNotEquals(topic4, topic6);
    assertNotEquals(topic5, topic6);
    assertFalse(topic6.isRealTime());
    assertFalse(topic6.isVersionTopic());
    assertFalse(topic6.isStreamReprocessingTopic());
    assertFalse(topic6.isVersionTopicOrStreamReprocessingTopic());
    assertTrue(topic6.isViewTopic());
  }

}
