package com.linkedin.venice.kafka;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.linkedin.venice.utils.TestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Created by mwise on 6/9/16.
 */
public class TopicManagerTest {

  /** Wait time for {@link #manager} operations, in seconds */
  private static final int WAIT_TIME = 10;

  private KafkaBrokerWrapper kafka;
  private TopicManager manager;

  @BeforeClass
  public void setup() {
    kafka = ServiceFactory.getKafkaBroker();
    manager = new TopicManager(kafka.getZkAddress());
  }

  @AfterClass
  public void teardown() throws IOException {
    kafka.close();
    manager.close();
  }

  @Test
  public void testCreateTopic() throws Exception {
    String topicName = TestUtils.getUniqueString("testCreateTopic");
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.SECONDS, () -> manager.containsTopic(topicName));
    Assert.assertTrue(manager.containsTopic(topicName));
    manager.createTopic(topicName, partitions, replicas); /* should be noop */
    Assert.assertTrue(manager.containsTopic(topicName));
  }

  @Test
  public void testDeleteTopic() throws InterruptedException {

    // Create a topic
    String topicName = TestUtils.getUniqueString("testDeleteTopic");
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.SECONDS,
        () -> manager.containsTopic(topicName));
    Assert.assertTrue(manager.containsTopic(topicName));

    // Delete that topic
    manager.deleteTopic(topicName);
    // Wait for it to go away (delete is async)
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.SECONDS,
        () -> !manager.containsTopic(topicName));
    // Assert that it is gone
    Assert.assertFalse(manager.containsTopic(topicName));
  }

  @Test
  public void testGetLastOffsets() {
    String topic = TestUtils.getUniqueString("topic");
    manager.createTopic(topic, 1, 1);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.SECONDS, () -> manager.containsTopic(topic));
    Map<Integer, Long> lastOffsets = manager.getLatestOffsets(topic);
    Assert.assertTrue(lastOffsets.containsKey(0), "single partition topic has an offset for partition 0");
    Assert.assertEquals(lastOffsets.keySet().size(), 1, "single partition topic has only an offset for one partition");
    Assert.assertEquals(lastOffsets.get(0).longValue(), 0L, "new topic must end at partition 0");
  }

  @Test
  public void testListOffsetsOnEmptyTopic(){
    KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
    doReturn(new HashMap<String, List<PartitionInfo>>()).when(mockConsumer).listTopics();
    Map<Integer, Long> offsets = manager.getLatestOffsets("myTopic");
    Assert.assertEquals(offsets.size(), 0);
  }
}