package com.linkedin.venice.pubsub.api.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests to verify the contract of {@link PubSubConsumerAdapter}
 */
public class PubSubConsumerAdapterTest {
  public static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(15);

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private PubSubConsumerAdapter pubSubConsumerAdapter;
  private Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
  private Lazy<PubSubProducerAdapter> pubSubProducerAdapterLazy;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubClientsFactory pubSubClientsFactory;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubClientsFactory = pubSubBrokerWrapper.getPubSubClientsFactory();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    String clientId = Utils.getUniqueString("test-consumer-");
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    pubSubConsumerAdapter = pubSubClientsFactory.getConsumerAdapterFactory()
        .create(veniceProperties, false, pubSubMessageDeserializer, clientId);
    pubSubProducerAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getProducerAdapterFactory().create(veniceProperties, clientId, null));
    pubSubAdminAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapter);
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0, false);
    }
    if (pubSubAdminAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapterLazy.get());
    }
  }

  // Test: When partitionsFor is called on a non-existent topic, it should return null
  @Test
  public void testPartitionsForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    List<PubSubTopicPartitionInfo> partitions = pubSubConsumerAdapter.partitionsFor(nonExistentPubSubTopic);
    assertNull(partitions, "Partitions should be null for a non-existent topic");
  }

  // Test: When partitionsFor is called on an existing topic, it should return a list of partitions
  @Test
  public void testPartitionsForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    List<PubSubTopicPartitionInfo> partitions = pubSubConsumerAdapter.partitionsFor(existingPubSubTopic);
    assertNotNull(partitions, "Partitions should not be null for an existing topic");
    assertFalse(partitions.isEmpty(), "Partitions should not be empty for an existing topic");
    assertEquals(partitions.size(), numPartitions, "Number of partitions does not match");
  }

  // Test: When endOffsets is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testEndOffsetsForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 1));

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
  }

  // Test: When endOffsets is called on an existing topic, it should return a map of partition to end offset
  @Test
  public void testEndOffsetsForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));
    Map<PubSubTopicPartition, Long> endOffsets = pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT);
    assertNotNull(endOffsets, "End offsets should not be null for an existing topic");
    assertFalse(endOffsets.isEmpty(), "End offsets should not be empty for an existing topic");
    assertEquals(endOffsets.size(), partitions.size(), "Number of end offsets does not match");
    assertTrue(endOffsets.values().stream().allMatch(offset -> offset == 0), "End offsets should be 0 for a new topic");
  }

  // Test: When endOffsets is called for a non-existent and existing topic in the same API call,
  // it should throw a PubSubOpTimeoutException.
  @Test
  public void testEndOffsetsForNonExistentAndExistingTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
  }

  // Test: When endOffsets is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testEndOffsetsForExistingTopicWithNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
  }

  // Test: When beginningOffset is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testBeginningOffsetForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT));
  }

  // Test: When beginningOffset is called on an existing topic, it should return an offset
  @Test
  public void testBeginningOffsetForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 2);
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
  }

  // Test: When beginningOffset is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testBeginningOffsetForExistingTopicWithNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");

    PubSubTopicPartition nonExistentPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.beginningOffset(nonExistentPartition, PUBSUB_OP_TIMEOUT));
  }

  // Test: When offsetForTime is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(partition, 0, PUBSUB_OP_TIMEOUT));
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition but no messages, it should return
  // null
  @Test
  public void testOffsetForTimeForExistingTopicWithValidPartitionsButNoMessages() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with no messages");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with no messages");
  }

  // Test: When offsetForTime is called on an existing topic with invalid partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeForExistingTopicWithInvalidPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = true;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);
    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with no messages");

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(invalidPartition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT));
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition and messages, it should return an
  // offset
  @Test
  public void testOffsetForTimeForExistingTopicWithValidPartitionsAndMessages()
      throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;
    int replicationFactor = 1;
    long retentionInMs = Duration.ofDays(2).toMillis();
    boolean isLogCompacted = false;
    int minInSyncReplicas = 1;
    long minLogCompactionLagMs = Duration.ofDays(1).toMillis();
    int numMessages = 10;

    PubSubTopicConfiguration topicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionInMs),
        isLogCompacted,
        Optional.of(minInSyncReplicas),
        minLogCompactionLagMs);

    pubSubAdminAdapterLazy.get().createTopic(existingPubSubTopic, numPartitions, replicationFactor, topicConfiguration);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    Map<Integer, Long> timestamps = new HashMap<>(10);
    Map<Integer, Long> offsets = new HashMap<>(10);

    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    /*
     * Here we send messages to the topic such that message m0 is stored after time t0 but before t1, m1 is stored
     * after t1 but before t2 and so on. This is to ensure that our test is not flaky and the timestamps are different.
     * A 1ms sleep interval is introduced between each message to guarantee timestamp uniqueness.
     * +-t0-----+-t1-----+-t2-----+-t3-----+-t4-----+-t5-----+-t6-----+-t7-----+-t8-----+-t9-----+
     * |   m0   |   m1   |   m2   |   m3   |   m4   |   m5   |   m6   |   m7   |   m8   |   m9   |
     * +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
     */
    for (int i = 0; i < numMessages; i++) {
      KafkaMessageEnvelope value = PubSubHelper.getDummyValue();
      long timestamp = System.currentTimeMillis();
      value.producerMetadata.messageTimestamp = timestamp;
      timestamps.put(i, timestamp);
      PubSubProduceResult pubSubProduceResult = pubSubProducerAdapter
          .sendMessage(existingPubSubTopic.getName(), 0, PubSubHelper.getDummyKey(), value, null, null)
          .get(15, TimeUnit.SECONDS);
      offsets.put(i, pubSubProduceResult.getOffset());
      Thread.sleep(1);
    }

    PubSubTopicPartition partitionWitMessages = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    // iterate over the messages and verify the offset for each timestamp
    for (int i = 0; i < numMessages; i++) {
      Long offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, timestamps.get(i), PUBSUB_OP_TIMEOUT);
      assertNotNull(offset, "Offset should not be null for an existing topic with messages");
      assertEquals(offset, offsets.get(i), "Offset should match for an existing topic with messages");
    }

    // try 0 as timestamp; this should return the first offset
    Long offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, 0, PUBSUB_OP_TIMEOUT);
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");

    // check one month before the first message timestamp
    long oneMonthBeforeFirstMessageTimestamp = timestamps.get(0) - Duration.ofDays(30).toMillis();
    offset = pubSubConsumerAdapter
        .offsetForTime(partitionWitMessages, oneMonthBeforeFirstMessageTimestamp, PUBSUB_OP_TIMEOUT);
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");

    // check for a timestamp that is after the last message
    long currentTimestamp = System.currentTimeMillis();
    assertTrue(
        currentTimestamp > timestamps.get(numMessages - 1),
        "Current timestamp should be greater than the last message timestamp");
    offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, currentTimestamp, PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with out of range timestamp");

    // check one month from the last message timestamp
    long oneMonthFromLastMessageTimestamp = timestamps.get(numMessages - 1) + Duration.ofDays(30).toMillis();
    offset =
        pubSubConsumerAdapter.offsetForTime(partitionWitMessages, oneMonthFromLastMessageTimestamp, PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with out of range timestamp");
  }
}
