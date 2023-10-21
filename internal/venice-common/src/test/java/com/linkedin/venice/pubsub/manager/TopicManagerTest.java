package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_DELETE_RETRY_TIMES;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.MockInMemoryAdminAdapter;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TopicManagerTest {
  private static final Logger LOGGER = LogManager.getLogger(TopicManagerTest.class);

  /** Wait time for {@link #topicManager} operations, in seconds */
  private static final int WAIT_TIME_IN_SECONDS = 10;
  protected static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;
  protected final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  protected TopicManager topicManager;

  @BeforeClass
  public void setUp() {
    createTopicManager();
    Cache cacheNothingCache = mock(Cache.class);
    Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    topicManager.setTopicConfigCache(cacheNothingCache);
  }

  private InMemoryKafkaBroker inMemoryKafkaBroker;

  protected void createTopicManager() {
    inMemoryKafkaBroker = new InMemoryKafkaBroker("local");
    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = mock(ApacheKafkaAdminAdapterFactory.class);
    MockInMemoryAdminAdapter mockInMemoryAdminAdapter = new MockInMemoryAdminAdapter(inMemoryKafkaBroker);
    doReturn(mockInMemoryAdminAdapter).when(pubSubAdminAdapterFactory).create(any(), eq(pubSubTopicRepository));
    MockInMemoryConsumer mockInMemoryConsumer =
        new MockInMemoryConsumer(inMemoryKafkaBroker, new RandomPollStrategy(), mock(PubSubConsumerAdapter.class));
    mockInMemoryConsumer.setMockInMemoryAdminAdapter(mockInMemoryAdminAdapter);
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory = mock(PubSubConsumerAdapterFactory.class);
    doReturn(mockInMemoryConsumer).when(pubSubConsumerAdapterFactory).create(any(), anyBoolean(), any(), anyString());

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> VeniceProperties.empty())
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubConsumerAdapterFactory(pubSubConsumerAdapterFactory)
            .setPubSubAdminAdapterFactory(pubSubAdminAdapterFactory)
            .setPubSubOperationTimeoutMs(500L)
            .setTopicDeletionStatusPollIntervalMs(100L)
            .setTopicMinLogCompactionLagMs(MIN_COMPACTION_LAG)
            .build();
    topicManager = new TopicManagerRepository(topicManagerContext, "localhost:1234").getLocalTopicManager();
  }

  protected PubSubProducerAdapter createPubSubProducerAdapter() {
    return new MockInMemoryProducerAdapter(inMemoryKafkaBroker);
  }

  @AfterClass
  public void cleanUp() throws IOException {
    topicManager.close();
  }

  /**
   * This method produces either a random data record or a control message/record to Kafka with a given producer timestamp.
   *
   * @param topic
   * @param isDataRecord
   * @param producerTimestamp
   * @throws ExecutionException
   * @throws InterruptedException
   */
  protected void produceRandomPubSubMessage(PubSubTopic topic, boolean isDataRecord, long producerTimestamp)
      throws ExecutionException, InterruptedException {
    PubSubProducerAdapter producer = createPubSubProducerAdapter();

    final byte[] randomBytes = new byte[] { 0, 1 };

    // Prepare record key
    KafkaKey recordKey = new KafkaKey(isDataRecord ? MessageType.PUT : MessageType.CONTROL_MESSAGE, randomBytes);

    // Prepare record value
    KafkaMessageEnvelope recordValue = new KafkaMessageEnvelope();
    recordValue.producerMetadata = new ProducerMetadata();
    recordValue.producerMetadata.producerGUID = new GUID();
    recordValue.producerMetadata.messageTimestamp = producerTimestamp;
    recordValue.leaderMetadataFooter = new LeaderMetadata();
    recordValue.leaderMetadataFooter.hostName = "localhost";

    if (isDataRecord) {
      Put put = new Put();
      put.putValue = ByteBuffer.wrap(new byte[] { 0, 1 });
      put.replicationMetadataPayload = ByteBuffer.wrap(randomBytes);
      recordValue.payloadUnion = put;
    } else {
      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = new EndOfPush();
      controlMessage.debugInfo = Collections.emptyMap();
      recordValue.payloadUnion = controlMessage;
    }
    producer.sendMessage(topic.getName(), 0, recordKey, recordValue, null, mock(PubSubProducerCallback.class)).get();
  }

  protected PubSubTopic getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    PubSubTopic topicName = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString(callingFunction));
    int partitions = 1;
    int replicas = 1;
    topicManager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME_IN_SECONDS,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicName)));
    return topicName;
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecord() throws ExecutionException, InterruptedException {
    final PubSubTopic topic = getTopic();
    final PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);
    final long timestamp = System.currentTimeMillis();
    produceRandomPubSubMessage(topic, true, timestamp - 1000);
    produceRandomPubSubMessage(topic, true, timestamp); // This timestamp is expected to be retrieved

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordWithControlMessage()
      throws ExecutionException, InterruptedException {
    final PubSubTopic topic = getTopic();
    final PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);

    long timestamp = System.currentTimeMillis();
    produceRandomPubSubMessage(topic, true, timestamp); // This timestamp is expected to be retrieved
    produceRandomPubSubMessage(topic, false, timestamp + 1000); // produce a control message

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);

    // Produce more data records to this topic partition
    for (int i = 0; i < 10; i++) {
      timestamp += 1000;
      produceRandomPubSubMessage(topic, true, timestamp);
    }
    // Produce several control messages at the end
    for (int i = 1; i <= 3; i++) {
      produceRandomPubSubMessage(topic, false, timestamp + i * 1000L);
    }
    retrievedTimestamp = topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordOnEmptyTopic() {
    final PubSubTopicPartition emptyTopicPartition = new PubSubTopicPartitionImpl(getTopic(), 0);
    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataMessageWithRetries(emptyTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, PubSubConstants.PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION);
  }

  @Test
  public void testGetProducerTimestampOfLastDataRecordWithOnlyControlMessages()
      throws ExecutionException, InterruptedException {
    final PubSubTopic topic = getTopic();
    long timestamp = System.currentTimeMillis();

    // Produce only control messages
    for (int i = 0; i < 10; i++) {
      produceRandomPubSubMessage(topic, false, timestamp);
      timestamp += 10;
    }
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);
    Assert.assertThrows(
        VeniceException.class,
        () -> topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1));
  }

  @Test
  public void testCreateTopic() throws Exception {
    PubSubTopic topicNameWithEternalRetentionPolicy = getTopic();
    topicManager.createTopic(topicNameWithEternalRetentionPolicy, 1, 1, true); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithEternalRetentionPolicy));
    Assert.assertEquals(
        topicManager.getTopicRetention(topicNameWithEternalRetentionPolicy),
        PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    PubSubTopic topicNameWithDefaultRetentionPolicy = getTopic();
    topicManager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(
        topicManager.getTopicRetention(topicNameWithDefaultRetentionPolicy),
        PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  @Test
  public void testCreateTopicWhenTopicExists() throws Exception {
    PubSubTopic topicNameWithEternalRetentionPolicy = getTopic();
    PubSubTopic topicNameWithDefaultRetentionPolicy = getTopic();

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
    Assert.assertEquals(
        topicManager.getTopicRetention(topicNameWithEternalRetentionPolicy),
        PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS);

    topicManager.createTopic(topicNameWithDefaultRetentionPolicy, 1, 1, false); /* should be noop */
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicNameWithDefaultRetentionPolicy));
    Assert.assertEquals(
        topicManager.getTopicRetention(topicNameWithDefaultRetentionPolicy),
        PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }

  @Test
  public void testDeleteTopic() throws ExecutionException {
    PubSubTopic topicName = getTopic();
    topicManager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testDeleteTopicWithRetry() throws ExecutionException {
    PubSubTopic topicName = getTopic();
    topicManager.ensureTopicIsDeletedAndBlockWithRetry(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testDeleteTopicWithTimeout() throws ExecutionException {

    // Since we're dealing with a mock in this test case, we'll just use a fake topic name
    PubSubTopic topicName = pubSubTopicRepository.getTopic("mockTopicName_v1");
    // Without using mockito spy, the LOGGER inside TopicManager cannot be prepared.
    TopicManager partiallyMockedTopicManager = Mockito.spy(topicManager);
    Mockito.doThrow(PubSubOpTimeoutException.class)
        .when(partiallyMockedTopicManager)
        .ensureTopicIsDeletedAndBlock(topicName);
    Mockito.doCallRealMethod().when(partiallyMockedTopicManager).ensureTopicIsDeletedAndBlockWithRetry(topicName);

    // Make sure everything went as planned
    Assert.assertThrows(
        PubSubOpTimeoutException.class,
        () -> partiallyMockedTopicManager.ensureTopicIsDeletedAndBlockWithRetry(topicName));
    Mockito.verify(partiallyMockedTopicManager, times(PUBSUB_TOPIC_DELETE_RETRY_TIMES))
        .ensureTopicIsDeletedAndBlock(topicName);
  }

  @Test
  public void testSyncDeleteTopic() throws ExecutionException {
    PubSubTopic topicName = getTopic();
    // Delete that topic
    topicManager.ensureTopicIsDeletedAndBlock(topicName);
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topicName));
  }

  @Test
  public void testGetLastOffsets() {
    PubSubTopic topic = getTopic();
    Map<Integer, Long> lastOffsets = topicManager.getTopicLatestOffsets(topic);
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(lastOffsets.containsKey(0), "single partition topic has an offset for partition 0");
      Assert
          .assertEquals(lastOffsets.keySet().size(), 1, "single partition topic has only an offset for one partition");
      Assert.assertEquals(lastOffsets.get(0).longValue(), 0L, "new topic must end at partition 0");
    });
  }

  @Test
  public void testListOffsetsOnEmptyTopic() {
    Map<Integer, Long> offsets = topicManager.getTopicLatestOffsets(pubSubTopicRepository.getTopic("myTopic_v1"));
    Assert.assertEquals(offsets.size(), 0);
  }

  @Test
  public void testGetTopicConfig() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.createTopic(topic, 1, 1, true);
    PubSubTopicConfiguration topicProperties = topicManager.getTopicConfig(topic);
    Assert.assertTrue(topicProperties.retentionInMs().isPresent());
    Assert.assertTrue(topicProperties.retentionInMs().get() > 0, "retention.ms should be positive");
  }

  @Test(expectedExceptions = PubSubTopicDoesNotExistException.class)
  public void testGetTopicConfigWithUnknownTopic() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.getTopicConfig(topic);
  }

  @Test
  public void testUpdateTopicRetention() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.createTopic(topic, 1, 1, true);
    topicManager.updateTopicRetention(topic, 0);
    PubSubTopicConfiguration topicProperties = topicManager.getTopicConfig(topic);
    Assert.assertTrue(topicProperties.retentionInMs().isPresent());
    Assert.assertTrue(topicProperties.retentionInMs().get() == 0);
  }

  @Test
  public void testListAllTopics() {
    Set<PubSubTopic> expectTopics = new HashSet<>(topicManager.listTopics());
    PubSubTopic topic1 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    PubSubTopic topic2 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    PubSubTopic topic3 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    // Create 1 topic, expect 1 topic in total
    topicManager.createTopic(topic1, 1, 1, true);
    expectTopics.add(topic1);
    Set<PubSubTopic> allTopics = topicManager.listTopics();
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
    PubSubTopic topic1 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    PubSubTopic topic2 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    PubSubTopic topic3 = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.createTopic(topic1, 1, 1, true);
    topicManager.createTopic(topic2, 1, 1, false);
    topicManager.createTopic(topic3, 1, 1, false);
    topicManager.updateTopicRetention(topic3, 5000);

    Map<PubSubTopic, Long> topicRetentions = topicManager.getAllTopicRetentions();
    Assert.assertTrue(
        topicRetentions.size() > 3,
        "There should be at least 3 topics, " + "which were created by this test");
    Assert.assertEquals(topicRetentions.get(topic1).longValue(), PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic2).longValue(), PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS);
    Assert.assertEquals(topicRetentions.get(topic3).longValue(), 5000);

    long deprecatedTopicRetentionMaxMs = 5000;
    Assert.assertFalse(
        topicManager.isTopicTruncated(topic1, deprecatedTopicRetentionMaxMs),
        "Topic1 should not be deprecated because of unlimited retention policy");
    Assert.assertFalse(
        topicManager.isTopicTruncated(topic2, deprecatedTopicRetentionMaxMs),
        "Topic2 should not be deprecated because of unknown retention policy");
    Assert.assertTrue(
        topicManager.isTopicTruncated(topic3, deprecatedTopicRetentionMaxMs),
        "Topic3 should be deprecated because of low retention policy");

    Assert.assertFalse(
        topicManager
            .isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs + 1, deprecatedTopicRetentionMaxMs));
    Assert.assertFalse(
        topicManager.isRetentionBelowTruncatedThreshold(
            PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION,
            deprecatedTopicRetentionMaxMs));
    Assert.assertTrue(
        topicManager
            .isRetentionBelowTruncatedThreshold(deprecatedTopicRetentionMaxMs - 1, deprecatedTopicRetentionMaxMs));
  }

  @Test
  public void testUpdateTopicCompactionPolicy() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.createTopic(topic, 1, 1, true);
    Assert.assertFalse(
        topicManager.isTopicCompactionEnabled(topic),
        "topic: " + topic + " should be with compaction disabled");
    topicManager.updateTopicCompactionPolicy(topic, true);
    Assert.assertTrue(
        topicManager.isTopicCompactionEnabled(topic),
        "topic: " + topic + " should be with compaction enabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), MIN_COMPACTION_LAG);
    topicManager.updateTopicCompactionPolicy(topic, false);
    Assert.assertFalse(
        topicManager.isTopicCompactionEnabled(topic),
        "topic: " + topic + " should be with compaction disabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), 0L);

    topicManager.updateTopicCompactionPolicy(topic, true, 100, Optional.of(Long.valueOf(200l)));
    Assert.assertTrue(
        topicManager.isTopicCompactionEnabled(topic),
        "topic: " + topic + " should be with compaction enabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), 100L);
    Assert.assertEquals(topicManager.getTopicMaxLogCompactionLagMs(topic).get(), Long.valueOf(200L));

    topicManager.updateTopicCompactionPolicy(topic, true, 1000, Optional.of(Long.valueOf(2000L)));
    Assert.assertTrue(
        topicManager.isTopicCompactionEnabled(topic),
        "topic: " + topic + " should be with compaction enabled");
    Assert.assertEquals(topicManager.getTopicMinLogCompactionLagMs(topic), 1000L);
    Assert.assertEquals(topicManager.getTopicMaxLogCompactionLagMs(topic).get(), Long.valueOf(2000L));
  }

  @Test
  public void testGetConfigForNonExistingTopic() {
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("non-existing-topic"));
    Assert.assertThrows(PubSubTopicDoesNotExistException.class, () -> topicManager.getTopicConfig(nonExistingTopic));
  }

  @Test
  public void testGetLatestOffsetForNonExistingTopic() {
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("non-existing-topic"));
    Assert.assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(nonExistingTopic, 0), 10));
  }

  @Test
  public void testGetLatestProducerTimestampForNonExistingTopic() {
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("non-existing-topic"));
    Assert.assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager
            .getProducerTimestampOfLastDataMessageWithRetries(new PubSubTopicPartitionImpl(nonExistingTopic, 0), 10));
  }

  @Test
  public void testGetAndUpdateTopicRetentionForNonExistingTopic() {
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("non-existing-topic"));
    Assert.assertThrows(PubSubTopicDoesNotExistException.class, () -> topicManager.getTopicRetention(nonExistingTopic));
    Assert.assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.updateTopicRetention(nonExistingTopic, TimeUnit.DAYS.toMillis(1)));
  }

  @Test
  public void testUpdateTopicCompactionPolicyForNonExistingTopic() {
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("non-existing-topic"));
    Assert.assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.updateTopicCompactionPolicy(nonExistingTopic, true));
  }

  @Test
  public void testTimeoutOnGettingMaxOffset() throws IOException {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);
    // Mock an admin client to pass topic existence check
    PubSubAdminAdapter mockPubSubAdminAdapter = mock(PubSubAdminAdapter.class);
    doReturn(true).when(mockPubSubAdminAdapter).containsTopic(eq(topic));
    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    doThrow(new PubSubOpTimeoutException("Timed out while fetching end offsets")).when(mockPubSubConsumer)
        .endOffsets(any(), any());
    // Throw Kafka TimeoutException when trying to get max offset
    String localPubSubBrokerAddress = "localhost:1234";

    PubSubAdminAdapterFactory adminAdapterFactory = mock(PubSubAdminAdapterFactory.class);
    PubSubConsumerAdapterFactory consumerAdapterFactory = mock(PubSubConsumerAdapterFactory.class);
    doReturn(mockPubSubConsumer).when(consumerAdapterFactory).create(any(), anyBoolean(), any(), anyString());
    doReturn(mockPubSubAdminAdapter).when(adminAdapterFactory).create(any(), eq(pubSubTopicRepository));

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> VeniceProperties.empty())
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubAdminAdapterFactory(adminAdapterFactory)
            .setPubSubConsumerAdapterFactory(consumerAdapterFactory)
            .setTopicDeletionStatusPollIntervalMs(100)
            .setTopicMetadataFetcherConsumerPoolSize(1)
            .setTopicMetadataFetcherThreadPoolSize(1)
            .setTopicMinLogCompactionLagMs(MIN_COMPACTION_LAG)
            .build();

    try (TopicManager topicManagerForThisTest =
        new TopicManagerRepository(topicManagerContext, localPubSubBrokerAddress).getLocalTopicManager()) {
      Assert.assertThrows(
          PubSubOpTimeoutException.class,
          () -> topicManagerForThisTest.getLatestOffsetWithRetries(pubSubTopicPartition, 10));
    }
  }

  @Test
  public void testContainsTopicWithExpectationAndRetry() throws InterruptedException {
    // Case 1: topic does not exist
    PubSubTopic nonExistingTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistingTopic"));
    Assert.assertFalse(topicManager.containsTopicWithExpectationAndRetry(nonExistingTopic, 3, true));

    // Case 2: topic exists
    topicManager.createTopic(nonExistingTopic, 1, 1, false);
    PubSubTopic existingTopic = nonExistingTopic;
    Assert.assertTrue(topicManager.containsTopicWithExpectationAndRetry(existingTopic, 3, true));

    // Case 3: topic does not exist initially but topic is created later.
    // This test case is to simulate the situation where the contains topic check fails on initial attempt(s) but
    // succeeds eventually.
    PubSubTopic initiallyNotExistTopic =
        pubSubTopicRepository.getTopic(Utils.getUniqueString("initiallyNotExistTopic"));

    final long delayedTopicCreationInSeconds = 1;
    CountDownLatch delayedTopicCreationStartedSignal = new CountDownLatch(1);

    CompletableFuture delayedTopicCreationFuture = CompletableFuture.runAsync(() -> {
      delayedTopicCreationStartedSignal.countDown();
      LOGGER.info(
          "Thread started and it will create topic {} in {} second(s)",
          initiallyNotExistTopic,
          delayedTopicCreationInSeconds);
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(delayedTopicCreationInSeconds));
      } catch (InterruptedException e) {
        Assert.fail("Got unexpected exception...", e);
      }
      topicManager.createTopic(initiallyNotExistTopic, 1, 1, false);
      LOGGER.info("Created this initially-not-exist topic: {}", initiallyNotExistTopic);
    });
    Assert.assertTrue(delayedTopicCreationStartedSignal.await(5, TimeUnit.SECONDS));

    Duration initialBackoff = Duration.ofSeconds(delayedTopicCreationInSeconds + 2);
    Duration maxBackoff = Duration.ofSeconds(initialBackoff.getSeconds() + 1);
    Duration maxDuration = Duration.ofSeconds(3 * maxBackoff.getSeconds());
    Assert.assertFalse(delayedTopicCreationFuture.isDone());
    Assert.assertTrue(
        topicManager.containsTopicWithExpectationAndRetry(
            initiallyNotExistTopic,
            3,
            true,
            initialBackoff,
            maxBackoff,
            maxDuration));
    Assert.assertTrue(delayedTopicCreationFuture.isDone());
  }

  @Test
  public void testMinimumExpectedRetentionTime() {
    StoreProperties storeProperties = AvroRecordUtils.prefillAvroRecordWithDefaultValue(new StoreProperties());
    storeProperties.name = "storeName";
    storeProperties.owner = "owner";
    storeProperties.createdTime = System.currentTimeMillis();
    storeProperties.bootstrapToOnlineTimeoutInHours = 12;
    Store store = new ZKStore(storeProperties);
    HybridStoreConfig hybridStoreConfig2DayRewind = new HybridStoreConfigImpl(
        2 * Time.SECONDS_PER_DAY,
        20000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    // Since bootstrapToOnlineTimeout + rewind time + buffer (2 days) < 5 days, retention will be set to 5 days
    Assert
        .assertEquals(StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig2DayRewind), 5 * Time.MS_PER_DAY);
  }

  @Test
  public void testExpectedRetentionTime() {
    StoreProperties storeProperties = AvroRecordUtils.prefillAvroRecordWithDefaultValue(new StoreProperties());
    storeProperties.name = "storeName";
    storeProperties.owner = "owner";
    storeProperties.createdTime = System.currentTimeMillis();
    storeProperties.bootstrapToOnlineTimeoutInHours = 3 * Time.HOURS_PER_DAY;
    Store store = new ZKStore(storeProperties);
    HybridStoreConfig hybridStoreConfig2DayRewind = new HybridStoreConfigImpl(
        2 * Time.SECONDS_PER_DAY,
        20000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    // Since bootstrapToOnlineTimeout + rewind time + buffer (2 days) > 5 days, retention will be set to the computed
    // value
    Assert
        .assertEquals(StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig2DayRewind), 7 * Time.MS_PER_DAY);
  }

  @Test
  public void testContainsTopicAndAllPartitionsAreOnline() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("a-new-topic"));
    Assert.assertFalse(topicManager.containsTopicAndAllPartitionsAreOnline(topic)); // Topic does not exist yet

    topicManager.createTopic(topic, 1, 1, true);
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topic));
  }

  @Test
  public void testUpdateTopicMinISR() {
    PubSubTopic topic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic"));
    topicManager.createTopic(topic, 1, 1, true);
    PubSubTopicConfiguration pubSubTopicConfiguration = topicManager.getTopicConfig(topic);
    Assert.assertTrue(pubSubTopicConfiguration.minInSyncReplicas().get() == 1);
    // Update minISR to 2
    topicManager.updateTopicMinInSyncReplica(topic, 2);
    pubSubTopicConfiguration = topicManager.getTopicConfig(topic);
    Assert.assertTrue(pubSubTopicConfiguration.minInSyncReplicas().get() == 2);
  }

}
