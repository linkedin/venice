package com.linkedin.venice.kafka;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcherImpl;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import kafka.log.LogConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

  private static final Logger logger = Logger.getLogger(TopicManagerTest.class);

  /** Wait time for {@link #topicManager} operations, in seconds */
  private static final int WAIT_TIME_IN_SECONDS = 10;
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  private KafkaBrokerWrapper kafka;
  private TopicManager topicManager;
  private MockTime mockTime;

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    String topicName = TestUtils.getUniqueString(callingFunction);
    int partitions = 1;
    int replicas = 1;
    topicManager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(WAIT_TIME_IN_SECONDS, TimeUnit.SECONDS,
        () -> Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicName)));
    return topicName;
  }

  @BeforeClass
  public void setup() {
    mockTime = new MockTime();
    kafka = ServiceFactory.getKafkaBroker(mockTime);
    topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, TestUtils.getVeniceConsumerFactory(kafka));
    Cache cacheNothingCache = mock(Cache.class);
    Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    topicManager.setTopicConfigCache(cacheNothingCache);
  }

  @AfterClass
  public void teardown() throws IOException {
    kafka.close();
    topicManager.close();
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecord() throws ExecutionException, InterruptedException {
    final String topic = getTopic();
    final long timestamp = System.currentTimeMillis();
    produceToKafka(topic, true, timestamp - 1000);
    produceToKafka(topic, true, timestamp); // This timestamp is expected to be retrieved

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataRecord(topic, 0, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordWithControlMessage() throws ExecutionException, InterruptedException {
    final String topic = getTopic();
    long timestamp = System.currentTimeMillis();
    produceToKafka(topic, true, timestamp); // This timestamp is expected to be retrieved
    produceToKafka(topic, false, timestamp + 1000); // produce a control message

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataRecord(topic, 0, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);

    // Produce more data records to this topic partition
    for (int i = 0; i < 10; i++) {
      timestamp += 1000;
      produceToKafka(topic, true, timestamp);
    }
    // Produce several control messages at the end
    for (int i = 1; i <= 3; i++) {
      produceToKafka(topic, false, timestamp + i * 1000);
    }
    retrievedTimestamp = topicManager.getProducerTimestampOfLastDataRecord(topic, 0, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordOnEmptyTopic() {
    final String emptyTopic = getTopic();
    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataRecord(emptyTopic, 0, 1);
    Assert.assertEquals(retrievedTimestamp, PartitionOffsetFetcherImpl.NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordWithOnlyControlMessages() throws ExecutionException, InterruptedException {
    final String topic = getTopic();
    long timestamp = System.currentTimeMillis();

    // Produce only control messages
    for (int i = 0; i < 10; i++) {
      produceToKafka(topic, false, timestamp);
      timestamp += 10;
    }

    Assert.assertThrows(VeniceException.class, () -> topicManager.getProducerTimestampOfLastDataRecord(topic, 0, 1));
  }

  /**
   * This method produces either an random data record or a control message/record to Kafka with a given producer timestamp.
   *
   * @param topic
   * @param isDataRecord
   * @param producerTimestamp
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void produceToKafka(String topic, boolean isDataRecord, long producerTimestamp) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer = new KafkaProducer(props);
    final byte[] randomBytes = new byte[]{0, 1};

    // Prepare record key
    KafkaKey recordKey = new KafkaKey(
        isDataRecord ? MessageType.PUT : MessageType.CONTROL_MESSAGE,
        randomBytes
    );

    // Prepare record value
    KafkaMessageEnvelope recordValue = new KafkaMessageEnvelope();
    recordValue.producerMetadata = new ProducerMetadata();
    recordValue.producerMetadata.producerGUID = new GUID();
    recordValue.producerMetadata.messageTimestamp = producerTimestamp;
    recordValue.leaderMetadataFooter = new LeaderMetadata();
    recordValue.leaderMetadataFooter.hostName = "localhost";

    if (isDataRecord) {
      Put put = new Put();
      put.putValue = ByteBuffer.wrap(new byte[]{0, 1});
      put.replicationMetadataPayload = ByteBuffer.wrap(randomBytes);
      recordValue.payloadUnion = put;
    } else {
      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = new EndOfPush();
      controlMessage.debugInfo = Collections.emptyMap();
      recordValue.payloadUnion = controlMessage;
    }
    ProducerRecord<KafkaKey, KafkaMessageEnvelope> record = new ProducerRecord<>(topic, recordKey, recordValue);
    producer.send(record).get();
  }

  @Test
  public void testCreateTopic() throws Exception {
    String topicNameWithEternalRetentionPolicy = getTopic();
    topicManager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, true); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    String topicNameWithDefaultRetentionPolicy = getTopic();
    topicManager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithDefaultRetentionPolicy), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(1, topicManager.getReplicationFactor(topicNameWithDefaultRetentionPolicy));
  }

  @Test
  public void testCreateTopicWhenTopicExists() throws Exception {
    String topicNameWithEternalRetentionPolicy = getTopic();
    String topicNameWithDefaultRetentionPolicy = getTopic();

    // Create topic with zero retention policy
    topicManager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, false);
    topicManager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false);
    topicManager.updateTopicRetention(topicNameWithEternalRetentionPolicy, 0);
    topicManager.updateTopicRetention(topicNameWithDefaultRetentionPolicy, 0);
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithEternalRetentionPolicy), 0);
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithDefaultRetentionPolicy), 0);

    // re-create those topics with different retention policy

    topicManager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, true); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithEternalRetentionPolicy), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    topicManager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(topicManager.getTopicRetention(topicNameWithDefaultRetentionPolicy), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  @Test
  public void testDeleteTopic() throws ExecutionException {
    String topicName = getTopic();
    topicManager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testDeleteTopicWithRetry() throws ExecutionException {
    String topicName = getTopic();
    topicManager.ensureTopicIsDeletedAndBlockWithRetry(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
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
    topicManager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testGetLastOffsets() {
    String topic = getTopic();
    Map<Integer, Long> lastOffsets = topicManager.getTopicLatestOffsets(topic);
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
    Map<Integer, Long> offsets = topicManager.getTopicLatestOffsets("myTopic");
    Assert.assertEquals(offsets.size(), 0);
  }

  @Test
  public void testGetOffsetsByTime() throws InterruptedException, ExecutionException {
    final long START_TIME = 10;
    final long TIME_SKIP = 10000;
    final int NUMBER_OF_MESSAGES = 100;
    logger.info("Current time at the start of testGetOffsetsByTime: " + START_TIME);

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
      logger.info("Wrote messageNumber: " + messageNumber + ", at time: " + mockTime + ", offset: " + offset);
      mockTime.addMilliseconds(1);
    }

    assertOffsetsByTime(topicName, 0, 0);
    Map<Integer, Long> latestOffsets = topicManager.getTopicLatestOffsets(topicName);
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
    long offset = topicManager.getOffsetByTimeIfOutOfRange(new TopicPartition(topicName, 0), futureTime);
    Assert.assertEquals(offset, NUMBER_OF_MESSAGES + 1);

    /**
     * Check the offset with the rewind timestamp, which is older than the first message
     */
    assertOffsetsByTime(topicName, 0, 0);
  }

  private void assertOffsetsByTime(String topicName, long time, long expectedOffset) {
    logger.info("Asking for time: " + time + ", expecting offset: " + expectedOffset);
    Map<Integer, Long> offsets = topicManager.getTopicOffsetsByTime(topicName, time);
    offsets.forEach((partition, offset) -> Assert.assertEquals(offset, new Long(expectedOffset),
        "When asking for timestamp " + time + ", partition " + partition + " has an unexpected offset."));
  }

  @Test
  public void testGetTopicConfig() {
    String topic = TestUtils.getUniqueString("topic");
    topicManager.createTopic(topic, 1, 1, true);
    Properties topicProperties = topicManager.getTopicConfig(topic);
    Assert.assertTrue(topicProperties.containsKey(LogConfig.RetentionMsProp()));
    Assert.assertTrue(Long.parseLong(topicProperties.getProperty(LogConfig.RetentionMsProp())) > 0,
        "retention.ms should be positive");
  }

  @Test (expectedExceptions = TopicDoesNotExistException.class)
  public void testGetTopicConfigWithUnknownTopic() {
    String topic = TestUtils.getUniqueString("topic");
    topicManager.getTopicConfig(topic);
  }

  @Test
  public void testUpdateTopicRetention() {
    String topic = TestUtils.getUniqueString("topic");
    topicManager.createTopic(topic, 1, 1, true);
    topicManager.updateTopicRetention(topic, 0);
    Properties topicProperties = topicManager.getTopicConfig(topic);
    Assert.assertEquals(topicProperties.getProperty(LogConfig.RetentionMsProp()), "0");
  }

  @Test
  public void testListAllTopics() {
    Set<String> expectTopics = new HashSet<>(topicManager.listTopics());
    String topic1 = TestUtils.getUniqueString("topic");
    String topic2 = TestUtils.getUniqueString("topic");
    String topic3 = TestUtils.getUniqueString("topic");
    // Create 1 topic, expect 1 topic in total
    topicManager.createTopic(topic1, 1, 1, true);
    expectTopics.add(topic1);
    Set<String> allTopics = topicManager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);

    // Create another topic, expect 2 topics in total
    topicManager.createTopic(topic2, 1, 1, false);
    expectTopics.add(topic2);
    allTopics = topicManager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);

    // Create another topic, expect 3 topics in total
    topicManager.createTopic(topic3, 1, 1, false);
    expectTopics.add(topic3);
    allTopics = topicManager.listTopics();
    Assert.assertEquals(allTopics, expectTopics);
  }

  @Test
  public void testGetAllTopicRetentions() {
    String topic1 = TestUtils.getUniqueString("topic");
    String topic2 = TestUtils.getUniqueString("topic");
    String topic3 = TestUtils.getUniqueString("topic");
    topicManager.createTopic(topic1, 1, 1, true);
    topicManager.createTopic(topic2, 1, 1, false);
    topicManager.createTopic(topic3, 1, 1, false);
    topicManager.updateTopicRetention(topic3, 5000);

    Map<String, Long> topicRetentions = topicManager.getAllTopicRetentions();
    Assert.assertTrue(topicRetentions.size() > 3, "There should be at least 3 topics, "
        + "which were created by this test");
    Assert.assertEquals(topicRetentions.get(topic1).longValue(), TopicManager.ETERNAL_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic2).longValue(), TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic3).longValue(), 5000);

    long deprecatedTopicRetentionMaxMs = 5000;
    Assert.assertFalse(topicManager.isTopicTruncated(topic1, deprecatedTopicRetentionMaxMs),
        "Topic1 should not be deprecated because of unlimited retention policy");
    Assert.assertFalse(topicManager.isTopicTruncated(topic2, deprecatedTopicRetentionMaxMs),
        "Topic2 should not be deprecated because of unknown retention policy");
    Assert.assertTrue(topicManager.isTopicTruncated(topic3, deprecatedTopicRetentionMaxMs),
        "Topic3 should be deprecated because of low retention policy");

    Assert.assertFalse(
        topicManager.isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs + 1, deprecatedTopicRetentionMaxMs));
    Assert.assertFalse(topicManager.isRetentionBelowTruncatedThreshold(TopicManager.UNKNOWN_TOPIC_RETENTION, deprecatedTopicRetentionMaxMs));
    Assert.assertTrue(
        topicManager.isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs - 1, deprecatedTopicRetentionMaxMs));
  }

  @Test
  public void testUpdateTopicCompactionPolicy() {
    String topic = TestUtils.getUniqueString("topic");
    topicManager.createTopic(topic, 1, 1, true);
    Assert.assertFalse(topicManager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction disabled");
    topicManager.updateTopicCompactionPolicy(topic, true);
    Assert.assertTrue(topicManager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction enabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), MIN_COMPACTION_LAG);
    topicManager.updateTopicCompactionPolicy(topic, false);
    Assert.assertFalse(topicManager.isTopicCompactionEnabled(topic), "topic: " + topic + " should be with compaction disabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), 0L);
  }

  @Test
  public void testGetConfigForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.getTopicConfig(nonExistingTopic));
  }

  @Test
  public void testGetLatestOffsetForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.getPartitionLatestOffset(nonExistingTopic, 0));
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.getPartitionLatestOffsetAndRetry(nonExistingTopic, 0, 10));
  }

  @Test
  public void testGetLatestProducerTimestampForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.getProducerTimestampOfLastDataRecord(nonExistingTopic, 0, 10));
  }

  @Test
  public void testGetAndUpdateTopicRetentionForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.getTopicRetention(nonExistingTopic));
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.updateTopicRetention(nonExistingTopic, TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testUpdateTopicCompactionPolicyForNonExistingTopic() {
    String nonExistingTopic = TestUtils.getUniqueString("non-existing-topic");
    Assert.assertThrows(TopicDoesNotExistException.class, () -> topicManager.updateTopicCompactionPolicy(nonExistingTopic, true));
  }

  @Test
  public void testTimeoutOnGettingMaxOffset() {
    String topic = TestUtils.getUniqueString("topic");

    KafkaClientFactory mockKafkaClientFactory = mock(KafkaClientFactory.class);
    // Mock an admin client to pass topic existence check
    KafkaAdminWrapper mockKafkaAdminWrapper = mock(KafkaAdminWrapper.class);
    doReturn(true).when(mockKafkaAdminWrapper).containsTopicWithExpectationAndRetry(eq(topic), anyInt(), eq(true));
    doReturn(mockKafkaAdminWrapper).when(mockKafkaClientFactory).getKafkaAdminClient(any());
    // Throw Kafka TimeoutException when trying to get max offset
    KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);
    doThrow(new TimeoutException()).when(mockKafkaConsumer).endOffsets(any(), any());
    doReturn(mockKafkaConsumer).when(mockKafkaClientFactory).getRawBytesKafkaConsumer();

    TopicManager topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, mockKafkaClientFactory);
    Assert.assertThrows(VeniceOperationAgainstKafkaTimedOut.class, () -> topicManager.getPartitionLatestOffsetAndRetry(topic, 0, 10));
  }

  @Test
  public void testContainsTopicWithExpectationAndRetry() throws InterruptedException {
    // Case 1: topic does not exist
    String nonExistingTopic = TestUtils.getUniqueString("topic");
    Assert.assertFalse(topicManager.containsTopicWithExpectationAndRetry(nonExistingTopic, 3, true));

    // Case 2: topic exists
    topicManager.createTopic(nonExistingTopic, 1, 1, false);
    String existingTopic = nonExistingTopic;
    Assert.assertTrue(topicManager.containsTopicWithExpectationAndRetry(existingTopic, 3, true));

    // Case 3: topic does not exist initially but topic is created later.
    //         This test case is to simulate the situation where the contains topic check fails on initial attempt(s) but succeeds eventually.
    String initiallyNotExistTopic = TestUtils.getUniqueString("topic");

    final long delayedTopicCreationInSeconds = 1;
    CountDownLatch delayedTopicCreationStartedSignal = new CountDownLatch(1);

    Thread delayedTopicCreationThread = new Thread(() -> {
      delayedTopicCreationStartedSignal.countDown();
      logger.info(String.format("Thread started and it will create topic %s in %s second(s)", initiallyNotExistTopic, delayedTopicCreationInSeconds));
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(delayedTopicCreationInSeconds));
      } catch (InterruptedException e) {
        Assert.fail("Got unexpected exception...", e);
      }
      topicManager.createTopic(initiallyNotExistTopic, 1, 1, false);
      logger.info("Created this initially-not-exist topic: " + initiallyNotExistTopic);
    });
    delayedTopicCreationThread.setDaemon(true);
    delayedTopicCreationThread.start();
    // Just because start() is called does not mean thread is actually started running. Wait for a significant amount of time
    delayedTopicCreationStartedSignal.await(5, TimeUnit.SECONDS);

    Duration initialBackoff = Duration.ofSeconds(delayedTopicCreationInSeconds + 2);
    Duration maxBackoff = Duration.ofSeconds(initialBackoff.getSeconds() + 1);
    Duration maxDuration = Duration.ofSeconds(3 * maxBackoff.getSeconds());
    Assert.assertTrue(topicManager.containsTopicWithExpectationAndRetry(
        initiallyNotExistTopic,
        3,
        true,
        initialBackoff,
        maxBackoff,
        maxDuration
    ));
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
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topic)); // Topic does not exist yet

    topicManager.createTopic(topic, 1, 1, true);
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topic));
  }

  @Test
  public void testGetPartitionOffsetByTimeWithRetrySucceeds() {
    String topic = TestUtils.getUniqueString("topic");
    topicManager.createTopic(topic, 1, 1, true);
    int[] maxAttempts = {1, 3, 5};

    for (int maxAttempt : maxAttempts) {
      // Fetch the earliest offset which should be 0
      Assert.assertEquals(topicManager.getPartitionOffsetByTimeWithRetry(topic, 0, 0, maxAttempt, Duration.ofMillis(1)), 0);
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
