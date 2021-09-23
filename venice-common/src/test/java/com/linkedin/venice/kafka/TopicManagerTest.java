package com.linkedin.venice.kafka;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.mockito.Mockito;
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
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

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
        () -> Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicName)));
    return topicName;
  }

  @BeforeClass
  public void setup() {
    mockTime = new MockTime();
    kafka = ServiceFactory.getKafkaBroker(mockTime);
    manager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, TestUtils.getVeniceConsumerFactory(kafka));
    Cache cacheNothingCache = mock(Cache.class);
    Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    manager.setTopicConfigCache(cacheNothingCache);
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
    Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    String topicNameWithDefaultRetentionPolicy = getTopic();
    manager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
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
    Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    manager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(manager.getTopicRetention(topicNameWithDefaultRetentionPolicy), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  @Test
  public void testDeleteTopic() throws ExecutionException {
    String topicName = getTopic();
    manager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(manager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testDeleteTopicWithRetry() throws ExecutionException {
    String topicName = getTopic();
    manager.ensureTopicIsDeletedAndBlockWithRetry(topicName);
    Assert.assertFalse(manager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testDeleteTopicWithTimeout() throws IOException, ExecutionException {

    // Since we're dealing with a mock in this test case, we'll just use a fake topic name
    String topicName = "mockTopicName";
    TopicManager partiallyMockedTopicManager = Mockito.mock(TopicManager.class);
    Mockito.doThrow(VeniceOperationAgainstKafkaTimedOut.class).when(partiallyMockedTopicManager).ensureTopicIsDeletedAndBlock(topicName);
    Mockito.doCallRealMethod().when(partiallyMockedTopicManager).ensureTopicIsDeletedAndBlockWithRetry(topicName);

    // Make sure everything went as planned
    Assert.assertThrows(VeniceOperationAgainstKafkaTimedOut.class, () -> partiallyMockedTopicManager.ensureTopicIsDeletedAndBlockWithRetry(topicName));
    Mockito.verify(partiallyMockedTopicManager, times(MAX_TOPIC_DELETE_RETRIES)).ensureTopicIsDeletedAndBlock(topicName);
  }

  @Test
  public void testSyncDeleteTopic() throws ExecutionException {
    String topicName = getTopic();
    // Delete that topic
    manager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(manager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testGetLastOffsets() {
    String topic = getTopic();
    Map<Integer, Long> lastOffsets = manager.getTopicLatestOffsets(topic);
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
    Map<Integer, Long> offsets = manager.getTopicLatestOffsets("myTopic");
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
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafka.getAddress());
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topicName);

    // Test starting conditions
    assertOffsetsByTime(topicName, 0, LOWEST_OFFSET + 1);

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
    Map<Integer, Long> latestOffsets = manager.getTopicLatestOffsets(topicName);
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

    // A message get produced after future time
    mockTime.setTime(futureTime);
    veniceWriter.put(("key" + futureTime).getBytes(), ("value" + futureTime).getBytes(), 0, null).get();

    /**
     * Check whether {@link TopicManager#getOffsetByTimeIfOutOfRange} could return the right offset or not.
     */
    long offset = manager.getOffsetByTimeIfOutOfRange(new TopicPartition(topicName, 0), futureTime);
    Assert.assertEquals(offset, NUMBER_OF_MESSAGES + 1);

    /**
     * Check the offset with the rewind timestamp, which is older than the first message
     */
    assertOffsetsByTime(topicName, 0, 0);
  }

  private void assertOffsetsByTime(String topicName, long time, long expectedOffset) {
    LOGGER.info("Asking for time: " + time + ", expecting offset: " + expectedOffset);
    Map<Integer, Long> offsets = manager.getTopicOffsetsByTime(topicName, time);
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
  public void testListAllTopics() {
    Set<String> expectTopics = new HashSet<>(manager.listTopics());
    String topic1 = TestUtils.getUniqueString("topic");
    String topic2 = TestUtils.getUniqueString("topic");
    String topic3 = TestUtils.getUniqueString("topic");
    // Create 1 topic, expect 1 topic in total
    manager.createTopic(topic1, 1, 1, true);
    expectTopics.add(topic1);
    Set<String> allTopics = manager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);

    // Create another topic, expect 2 topics in total
    manager.createTopic(topic2, 1, 1, false);
    expectTopics.add(topic2);
    allTopics = manager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);

    // Create another topic, expect 3 topics in total
    manager.createTopic(topic3, 1, 1, false);
    expectTopics.add(topic3);
    allTopics = manager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);
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
    Assert.assertEquals(manager.getTopicMinLogCompactionLagMs(topic), MIN_COMPACTION_LAG);
    manager.updateTopicCompactionPolicy(topic, false);
    Assert.assertFalse(manager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction disabled");
    Assert.assertEquals(manager.getTopicMinLogCompactionLagMs(topic), 0L);
  }

  @Test
  public void testGetConfigForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.getTopicConfig(nonExistingTopic));
  }

  @Test
  public void testGetLatestOffsetForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.getPartitionLatestOffset(nonExistingTopic, 0));
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.getPartitionLatestOffsetAndRetry(nonExistingTopic, 0, 10));
  }

  @Test
  public void testGetLatestProducerTimestampForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.getLatestProducerTimestampAndRetry(nonExistingTopic, 0, 10));
  }

  @Test
  public void testGetAndUpdateTopicRetentionForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.getTopicRetention(nonExistingTopic));
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.updateTopicRetention(nonExistingTopic, TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testUpdateTopicCompactionPolicyForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> manager.updateTopicCompactionPolicy(nonExistingTopic, true));
  }

  @Test
  public void testTimeoutOnGettingMaxOffset() {
    String topic = TestUtils.getUniqueString("topic");

    KafkaClientFactory mockKafkaClientFactory = mock(KafkaClientFactory.class);
    // Mock an admin client to pass topic existence check
    KafkaAdminWrapper mockKafkaAdminWrapper = mock(KafkaAdminWrapper.class);
    doReturn(true).when(mockKafkaAdminWrapper).containsTopicWithRetry(eq(topic), anyInt());
    doReturn(mockKafkaAdminWrapper).when(mockKafkaClientFactory).getKafkaAdminClient(any());
    // Throw Kafka TimeoutException when trying to get max offset
    KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);
    doThrow(new TimeoutException()).when(mockKafkaConsumer).endOffsets(any(), any());
    doReturn(mockKafkaConsumer).when(mockKafkaClientFactory).getKafkaConsumer(any());

    TopicManager topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, mockKafkaClientFactory);
    Assert.assertThrows(VeniceOperationAgainstKafkaTimedOut.class, () -> topicManager.getPartitionLatestOffsetAndRetry(topic, 0, 10));
  }

  @Test
  public void testMinimumExpectedRetentionTime() {
    StoreProperties storeProperties = Store.prefillAvroRecordWithDefaultValue(new StoreProperties());
    storeProperties.name = "storeName";
    storeProperties.owner = "owner";
    storeProperties.createdTime = System.currentTimeMillis();
    storeProperties.bootstrapToOnlineTimeoutInHours = 12;
    Store store = new ZKStore(storeProperties);
    HybridStoreConfig hybridStoreConfig2DayRewind = new HybridStoreConfigImpl(2 * Time.SECONDS_PER_DAY, 20000, -1,
        DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);

    // Since bootstrapToOnlineTimeout + rewind time + buffer (2 days) < 5 days, retention will be set to 5 days
    Assert.assertEquals(TopicManager.getExpectedRetentionTimeInMs(store, hybridStoreConfig2DayRewind), 5 * Time.MS_PER_DAY);
  }

  @Test
  public void testExpectedRetentionTime() {
    StoreProperties storeProperties = Store.prefillAvroRecordWithDefaultValue(new StoreProperties());
    storeProperties.name = "storeName";
    storeProperties.owner = "owner";
    storeProperties.createdTime = System.currentTimeMillis();
    storeProperties.bootstrapToOnlineTimeoutInHours = 3 * Time.HOURS_PER_DAY;
    Store store = new ZKStore(storeProperties);
    HybridStoreConfig hybridStoreConfig2DayRewind = new HybridStoreConfigImpl(2 * Time.SECONDS_PER_DAY, 20000, -1,
        DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);

    // Since bootstrapToOnlineTimeout + rewind time + buffer (2 days) > 5 days, retention will be set to the computed value
    Assert.assertEquals(TopicManager.getExpectedRetentionTimeInMs(store, hybridStoreConfig2DayRewind), 7 * Time.MS_PER_DAY);
  }

  @Test
  public void testContainsTopicAndAllPartitionsAreOnline() {
    String topic = TestUtils.getUniqueString("a-new-topic");
    Assert.assertFalse(manager.containsTopicAndAllPartitionsAreOnline(topic)); // Topic does not exist yet

    manager.createTopic(topic, 1, 1, true);
    Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topic));
  }

  @Test
  public void testGetPartitionOffsetByTimeWithRetrySucceeds() {
    String topic = TestUtils.getUniqueString("topic");
    manager.createTopic(topic, 1, 1, true);
    int[] maxAttempts = {1, 3, 5};

    for (int maxAttempt : maxAttempts) {
      // Fetch the earliest offset which should be 0
      Assert.assertEquals(manager.getPartitionOffsetByTimeWithRetry(topic, 0, 0, maxAttempt, Duration.ofMillis(1)), 0);
    }
  }

  @Test
  public void testGetPartitionOffsetByTimeWithRetryFails() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    int[] maxAttempts = {1, 3, 5};
    // Create a new topic manager instance so that we can set the kafkaOperationTimeoutMs parameter to be shorter to make
    // this test case run more efficiently.
    TopicManager topicManager = new TopicManager(TimeUnit.SECONDS.toMillis(1), 100, MIN_COMPACTION_LAG, TestUtils.getVeniceConsumerFactory(kafka));

    for (int maxAttempt : maxAttempts) {
      // Still fail even with retry
      Assert.assertThrows(org.apache.kafka.common.errors.TimeoutException.class,
          () -> topicManager.getPartitionOffsetByTimeWithRetry(nonExistingTopic, 0, 0, maxAttempt, Duration.ofMillis(1)));
    }
  }
}
