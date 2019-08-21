package com.linkedin.venice.kafka;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;

import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import kafka.log.LogConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.kafka.TopicManager.*;
import static com.linkedin.venice.offsets.OffsetRecord.*;
import static org.mockito.Mockito.*;

public class TopicManagerTest {

  private static final Logger LOGGER = Logger.getLogger(TopicManagerTest.class);

  /** Wait time for {@link #manager} operations, in seconds */
  private static final int WAIT_TIME = 10;

  private KafkaBrokerWrapper kafka;
  private TopicManager manager;
  private MockTime mockTime;

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    String topicName = TestUtils.getUniqueString(callingFunction);
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(WAIT_TIME, TimeUnit.SECONDS,
        () -> Assert.assertTrue(manager.containsTopic(topicName)));
    return topicName;
  }

  @BeforeClass
  public void setup() {
    mockTime = new MockTime();
    kafka = ServiceFactory.getKafkaBroker(mockTime);
    manager = new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
  }

  @AfterClass
  public void teardown() throws IOException {
    kafka.close();
    manager.close();
  }

  @Test
  public void testCreateTopic() throws Exception {
    String topicNameWithEternalRetentionPolicy = getTopic();
    manager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, true); /* should be noop */
    Assert.assertTrue(manager.containsTopic(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    String topicNameWithDefaultRetentionPolicy = getTopic();
    manager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(manager.containsTopic(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithDefaultRetentionPolicy), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(1, manager.getReplicationFactor(topicNameWithDefaultRetentionPolicy));
  }

  @Test
  public void testCreateTopicWhenTopicExists() throws Exception {
    String topicNameWithEternalRetentionPolicy = getTopic();
    String topicNameWithDefaultRetentionPolicy = getTopic();

    // Create topic with zero retention policy
    manager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, false);
    manager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false);
    manager.updateTopicRetention(topicNameWithEternalRetentionPolicy, 0);
    manager.updateTopicRetention(topicNameWithDefaultRetentionPolicy, 0);
    Assert.assertEquals(manager.getTopicRetention(topicNameWithEternalRetentionPolicy), 0);
    Assert.assertEquals(manager.getTopicRetention(topicNameWithDefaultRetentionPolicy), 0);

    // re-create those topics with different retention policy

    manager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, true); /* should be noop */
    Assert.assertTrue(manager.containsTopic(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    manager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(manager.containsTopic(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithDefaultRetentionPolicy), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  @Test
  public void testDeleteTopic() {
    String topicName = getTopic();
    manager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(manager.containsTopic(topicName));
  }

  @Test
  public void testSyncDeleteTopic() {
    String topicName = getTopic();
    // Delete that topic
    manager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(manager.containsTopic(topicName));
  }

  @Test
  public void testGetLastOffsets() {
    String topic = getTopic();
    Map<Integer, Long> lastOffsets = manager.getLatestOffsets(topic);
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(lastOffsets.containsKey(0), "single partition topic has an offset for partition 0");
      Assert.assertEquals(lastOffsets.keySet().size(), 1, "single partition topic has only an offset for one partition");
      Assert.assertEquals(lastOffsets.get(0).longValue(), 0L, "new topic must end at partition 0");
    });
  }

  @Test
  public void testListOffsetsOnEmptyTopic(){
    KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
    doReturn(new HashMap<String, List<PartitionInfo>>()).when(mockConsumer).listTopics();
    Map<Integer, Long> offsets = manager.getLatestOffsets("myTopic");
    Assert.assertEquals(offsets.size(), 0);
  }

  @Test
  public void testGetOffsetsByTime() throws InterruptedException, ExecutionException {
    final long START_TIME = 10;
    final long TIME_SKIP = 10000;
    final int NUMBER_OF_MESSAGES = 100;
    LOGGER.info("Current time at the start of testGetOffsetsByTime: " + START_TIME);

    // Setup
    mockTime.setTime(START_TIME);
    String topicName = getTopic();
    Properties properties = new Properties();
    properties.put(ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceTestWriterFactory(kafka.getAddress());
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.getBasicVeniceWriter(topicName);

    // Test starting conditions
    assertOffsetsByTime(topicName, 0, LOWEST_OFFSET);

    // Populate messages
    mockTime.addMilliseconds(TIME_SKIP);
    RecordMetadata[] offsetsByMessageNumber = new RecordMetadata[NUMBER_OF_MESSAGES];
    for (int messageNumber = 0; messageNumber < NUMBER_OF_MESSAGES; messageNumber++) {
      byte[] key = ("key" + messageNumber).getBytes();
      byte[] value = ("value" + messageNumber).getBytes();
      offsetsByMessageNumber[messageNumber] = veniceWriter.put(key, value, 0, null).get();
      long offset = offsetsByMessageNumber[messageNumber].offset();
      LOGGER.info("Wrote messageNumber: " + messageNumber + ", at time: " + mockTime + ", offset: " + offset);
      mockTime.addMilliseconds(1);
    }

    assertOffsetsByTime(topicName, 0, 0);
    Map<Integer, Long> latestOffsets = manager.getLatestOffsets(topicName);
    LOGGER.info("latest offsets: " + latestOffsets);
    latestOffsets.forEach((partition, offset) -> Assert.assertTrue(offset >= NUMBER_OF_MESSAGES,
        "When asking the latest offsets, partition " + partition + " has an unexpected offset."));

    // We start at 1, skipping message number 0, because its offset is annoying to guess, because of control messages.
    for (int messageNumber = 1; messageNumber < NUMBER_OF_MESSAGES; messageNumber++) {
      long messageTime = START_TIME + TIME_SKIP + messageNumber;
      long returnedOffset = offsetsByMessageNumber[messageNumber].offset();
      assertOffsetsByTime(topicName, messageTime, returnedOffset);
    }

    long futureTime = 1000*1000*1000;
    assertOffsetsByTime(topicName, futureTime, NUMBER_OF_MESSAGES + 1);
  }

  private void assertOffsetsByTime(String topicName, long time, long expectedOffset) {
    LOGGER.info("Asking for time: " + time + ", expecting offset: " + expectedOffset);
    Map<Integer, Long> offsets = manager.getOffsetsByTime(topicName, time);
    offsets.forEach((partition, offset) -> Assert.assertEquals(offset, new Long(expectedOffset),
        "When asking for timestamp " + time + ", partition " + partition + " has an unexpected offset."));

  }

  @Test
  public void testGetTopicConfig() {
    String topic = TestUtils.getUniqueString("topic");
    manager.createTopic(topic, 1, 1, true);
    Properties topicProperties = manager.getTopicConfig(topic);
    Assert.assertTrue(topicProperties.containsKey(LogConfig.RetentionMsProp()));
    Assert.assertTrue(Long.parseLong(topicProperties.getProperty(LogConfig.RetentionMsProp())) > 0,
        "retention.ms should be positive");
  }

  @Test (expectedExceptions = TopicDoesNotExistException.class)
  public void testGetTopicConfigWithUnknownTopic() {
    String topic = TestUtils.getUniqueString("topic");
    manager.getTopicConfig(topic);
  }

  @Test
  public void testUpdateTopicRetention() {
    String topic = TestUtils.getUniqueString("topic");
    manager.createTopic(topic, 1, 1, true);
    manager.updateTopicRetention(topic, 0);
    Properties topicProperties = manager.getTopicConfig(topic);
    Assert.assertEquals(topicProperties.getProperty(LogConfig.RetentionMsProp()), "0");
  }

  @Test
  public void testGetAllTopicRetentions() {
    String topic1 = TestUtils.getUniqueString("topic");
    String topic2 = TestUtils.getUniqueString("topic");
    String topic3 = TestUtils.getUniqueString("topic");
    manager.createTopic(topic1, 1, 1, true);
    manager.createTopic(topic2, 1, 1, false);
    manager.createTopic(topic3, 1, 1, false);
    manager.updateTopicRetention(topic3, 5000);

    Map<String, Long> topicRetentions = manager.getAllTopicRetentions();
    Assert.assertTrue(topicRetentions.size() > 3, "There should be at least 3 topics, "
        + "which were created by this test");
    Assert.assertEquals(topicRetentions.get(topic1).longValue(), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic2).longValue(), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic3).longValue(), 5000);

    long deprecatedTopicRetentionMaxMs = 5000;
    Assert.assertFalse(manager.isTopicTruncated(topic1, deprecatedTopicRetentionMaxMs),
        "Topic1 should not be deprecated because of unlimited retention policy");
    Assert.assertFalse(manager.isTopicTruncated(topic2, deprecatedTopicRetentionMaxMs),
        "Topic2 should not be deprecated because of unknown retention policy");
    Assert.assertTrue(manager.isTopicTruncated(topic3, deprecatedTopicRetentionMaxMs),
        "Topic3 should be deprecated because of low retention policy");

    Assert.assertFalse(manager.isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs + 1, deprecatedTopicRetentionMaxMs));
    Assert.assertFalse(manager.isRetentionBelowTruncatedThreshold(TopicManager.UNKNOWN_TOPIC_RETENTION, deprecatedTopicRetentionMaxMs));
    Assert.assertTrue(manager.isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs - 1, deprecatedTopicRetentionMaxMs));
  }

  @Test
  public void testUpdateTopicCompactionPolicy() {
    String topic = TestUtils.getUniqueString("topic");
    manager.createTopic(topic, 1, 1, true);
    Assert.assertFalse(manager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction disabled");
    manager.updateTopicCompactionPolicy(topic, true);
    Assert.assertTrue(manager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction enabled");
    manager.updateTopicCompactionPolicy(topic, false);
    Assert.assertFalse(manager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction disabled");
  }
}