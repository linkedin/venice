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
import com.linkedin.venice.pubsub.PubSubConstants;
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
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
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
  private static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(15);
  private static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = 10_000;
  private static final int REPLICATION_FACTOR = 1;
  private static final boolean IS_LOG_COMPACTED = false;
  private static final long RETENTION_IN_MS = Duration.ofDays(2).toMillis();
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS);

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
    properties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS));
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
    long start = System.currentTimeMillis();
    List<PubSubTopicPartitionInfo> partitions = pubSubConsumerAdapter.partitionsFor(nonExistentPubSubTopic);
    long elapsed = System.currentTimeMillis() - start;
    assertNull(partitions, "Partitions should be null for a non-existent topic");
    assertTrue(elapsed <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "PartitionsFor should not block");
  }

  // Test: When partitionsFor is called on an existing topic, it should return a list of partitions
  @Test
  public void testPartitionsForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    long startTime = System.currentTimeMillis();
    List<PubSubTopicPartitionInfo> partitions = pubSubConsumerAdapter.partitionsFor(existingPubSubTopic);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(partitions, "Partitions should not be null for an existing topic");
    assertFalse(partitions.isEmpty(), "Partitions should not be empty for an existing topic");
    assertEquals(partitions.size(), numPartitions, "Number of partitions does not match");
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "PartitionsFor should not block");
  }

  // Test: When endOffsets is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testEndOffsetsForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 1));

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called on an existing topic, it should return a map of partition to end offset
  @Test
  public void testEndOffsetsForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));
    long startTime = System.currentTimeMillis();
    Map<PubSubTopicPartition, Long> endOffsets = pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(endOffsets, "End offsets should not be null for an existing topic");
    assertFalse(endOffsets.isEmpty(), "End offsets should not be empty for an existing topic");
    assertEquals(endOffsets.size(), partitions.size(), "Number of end offsets does not match");
    assertTrue(endOffsets.values().stream().allMatch(offset -> offset == 0), "End offsets should be 0 for a new topic");
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called for a non-existent and existing topic in the same API call,
  // it should throw a PubSubOpTimeoutException.
  @Test
  public void testEndOffsetsForNonExistentAndExistingTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));

    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testEndOffsetsForExistingTopicWithNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));

    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffset (without explicit timeout) is called on a non-existent partition,
  // it should throw PubSubOpTimeoutException after the default API timeout.
  @Test
  public void testEndOffsetWithoutExplicitTimeoutForNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(existingPubSubTopic, 1));

    // try to get the end offset for an existing topic with a valid partition but no messages
    long startTime = System.currentTimeMillis();
    Long endOffset = pubSubConsumerAdapter.endOffset(partitions.get(0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(endOffset, "End offset should not be null for an existing topic partition");
    assertEquals(endOffset, Long.valueOf(0), "End offset should be 0 for an existing topic partition");
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "endOffset should not block");

    // try to get the end offset for a non-existent topic partition
    startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffset(partitions.get(1)));
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater
    assertTrue(elapsedTime <= 2 * PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "endOffset should not block");
  }

  // Test: When beginningOffset is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testBeginningOffsetForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When beginningOffset is called on an existing topic, it should return an offset
  @Test
  public void testBeginningOffsetForExistingTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 3;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long startTime = System.currentTimeMillis();
    long beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "beginningOffset should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "beginningOffset should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 2);
    startTime = System.currentTimeMillis();
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "beginningOffset should not block");
  }

  // Test: When beginningOffset is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testBeginningOffsetForExistingTopicWithNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");

    PubSubTopicPartition nonExistentPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    long startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.beginningOffset(nonExistentPartition, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(partition, 0, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition but no messages, it should return
  // null
  @Test
  public void testOffsetForTimeForExistingTopicWithValidPartitionsButNoMessages() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long startTime = System.currentTimeMillis();
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");
  }

  // Test: When offsetForTime is called on an existing topic with invalid partition, it should throw
  // PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeForExistingTopicWithInvalidPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    long startTime = System.currentTimeMillis();
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(invalidPartition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT));
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis() + 5000,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition and messages, it should return an
  // offset
  @Test
  public void testOffsetForTimeForExistingTopicWithValidPartitionsAndMessages()
      throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;
    int numMessages = 10;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
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

    long startTime;
    long elapsedTime;
    PubSubTopicPartition partitionWitMessages = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    // iterate over the messages and verify the offset for each timestamp
    for (int i = 0; i < numMessages; i++) {
      startTime = System.currentTimeMillis();
      Long offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, timestamps.get(i), PUBSUB_OP_TIMEOUT);
      elapsedTime = System.currentTimeMillis() - startTime;
      assertNotNull(offset, "Offset should not be null for an existing topic with messages");
      assertEquals(offset, offsets.get(i), "Offset should match for an existing topic with messages");
      assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");
    }

    // try 0 as timestamp; this should return the first offset
    startTime = System.currentTimeMillis();
    Long offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, 0, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");

    // check one month before the first message timestamp; this should return the first offset
    long oneMonthBeforeFirstMessageTimestamp = timestamps.get(0) - Duration.ofDays(30).toMillis();
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter
        .offsetForTime(partitionWitMessages, oneMonthBeforeFirstMessageTimestamp, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");

    // check for a timestamp that is after the last message; this should return null
    long currentTimestamp = System.currentTimeMillis();
    assertTrue(
        currentTimestamp > timestamps.get(numMessages - 1),
        "Current timestamp should be greater than the last message timestamp");
    offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, currentTimestamp, PUBSUB_OP_TIMEOUT);
    assertNull(offset, "Offset should be null for an existing topic with out of range timestamp");

    // check one month from the last message timestamp; this should return null
    long oneMonthFromLastMessageTimestamp = timestamps.get(numMessages - 1) + Duration.ofDays(30).toMillis();
    startTime = System.currentTimeMillis();
    offset =
        pubSubConsumerAdapter.offsetForTime(partitionWitMessages, oneMonthFromLastMessageTimestamp, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with out of range timestamp");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT.toMillis(), "offsetForTime should not block");
  }

  // Test: When offsetForTime (without explicit timeout) is called on a non-existent topic,
  // it should throw PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeWithoutExplicitTimeoutForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.offsetForTime(partition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be greater than the default timeout but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime >= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS
            && elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000,
        "Timeout should be greater than the default timeout but not too much greater");
  }

  // Test: When offsetForTime (without explicit timeout) is called on an existing topic with invalid partition,
  // it should throw PubSubOpTimeoutException
  @Test
  public void testOffsetForTimeWithoutExplicitTimeoutForExistingTopicWithInvalidPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis());
    assertNull(offset, "Offset should be null for an existing topic with no messages");

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.offsetForTime(invalidPartition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be greater than the default timeout but not too much greater; so add variance of 5 seconds
    assertTrue(
        elapsedTime >= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS
            && elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000,
        "Timeout should be greater than the default timeout but not too much greater");
  }

  // Test: When offsetForTime (without explicit timeout) is called on an existing topic with a valid partition but no
  // messages, it should return null
  @Test
  public void testOffsetForTimeWithoutExplicitTimeoutForExistingTopicWithValidPartitionsButNoMessages() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    long startTime = System.currentTimeMillis();
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    Long offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis());
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "OffsetForTime should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis());
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, "OffsetForTime should not block");
  }

  // Test: Subscribe to non-existent topic should throw PubSubTopicDoesNotExistException
  @Test
  public void testSubscribeForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubTopicDoesNotExistException.class, () -> pubSubConsumerAdapter.subscribe(partition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(partition), "Should not be subscribed to any topic");
  }

  // Test: Subscribe to an existing topic with a non-existent partition should throw PubSubTopicDoesNotExistException
  @Test
  public void testSubscribeForExistingTopicWithNonExistentPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 2);
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubTopicDoesNotExistException.class, () -> pubSubConsumerAdapter.subscribe(invalidPartition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");
  }

  // Test: Subscribe to an existing topic with an existing partition should not take longer than the default timeout
  @Test
  public void testSubscribeForExistingTopicWithExistingPartition() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 1;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    long startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.subscribe(partition, 0);
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");

    // re-subscribe to the same topic and partition; this should not take longer than the default timeout
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.subscribe(partition, 0);
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");

    // check subscription status
    startTime = System.currentTimeMillis();
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        "Timeout should be less than the default timeout");

    startTime = System.currentTimeMillis();
    assertTrue(pubSubConsumerAdapter.hasSubscription(partition), "Should be subscribed to the topic and partition");
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        "Timeout should be less than the default timeout");
  }

  // Test: Unsubscribe to an existing topic and a non-existent topic should not take longer than the default timeout
  @Test
  public void testUnsubscribeForExistingAndNonExistentTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));

    int numPartitions = 1;
    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");

    // subscribe to an existing topic and partition
    pubSubConsumerAdapter.subscribe(partition, 0);

    // unsubscribe from an existing topic and partition
    long startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.unSubscribe(partition);
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");

    // unsubscribe from a non-existent topic and partition
    PubSubTopicPartition nonExistentPartition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.unSubscribe(nonExistentPartition);
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 3 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 3000,
        "Timeout should be less than the default timeout");
  }

  // Test: Batch unsubscribe to an existing topic and a non-existent topic should not take longer than the default
  // timeout
  @Test
  public void testBatchUnsubscribeForExistingAndNonExistentTopic() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));

    int numPartitions = 1;
    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition validPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");
    assertEquals(
        pubSubConsumerAdapter.partitionsFor(existingPubSubTopic).size(),
        1,
        "Topic should have only 1 partition");
    // check non-existent topic does not exist

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");

    // subscribe to an existing topic and partition
    pubSubConsumerAdapter.subscribe(validPartition, 0);
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(
        pubSubConsumerAdapter.hasSubscription(validPartition),
        "Should be subscribed to the topic and partition");

    Set<PubSubTopicPartition> partitions = new HashSet<>(Arrays.asList(validPartition, invalidPartition));
    // batch unsubscribe from an existing topic and partition
    long startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.batchUnsubscribe(partitions);
    long elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout; add variance of 5 seconds
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000,
        "Timeout should be less than the default timeout");

    // verify that the subscription is removed
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(validPartition), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(invalidPartition), "Should not be subscribed to any topic");
  }
}
