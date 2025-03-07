package com.linkedin.venice.pubsub.api.consumer;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POSITION_RESET_STRATEGY;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POSITION_RESET_STRATEGY_DEFAULT_VALUE;
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
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
  // timeout for pub-sub operations
  private static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(15);
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_OP_TIMEOUT_WITH_VARIANCE = PUBSUB_OP_TIMEOUT.toMillis() + 5000;
  // timeout for pub-sub consumer APIs which do not have a timeout parameter
  private static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = 10_000;
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE =
      PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000;
  private static final int REPLICATION_FACTOR = 1;
  private static final boolean IS_LOG_COMPACTED = false;
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long RETENTION_IN_MS = Duration.ofDays(3).toMillis();
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final long MAX_LOG_COMPACTION_LAG_MS = Duration.ofDays(2).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS,
      Optional.of(MAX_LOG_COMPACTION_LAG_MS));

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
    Properties pubSubProperties = getPubSubProperties();
    pubSubProperties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    pubSubProperties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(pubSubProperties);
    pubSubConsumerAdapter = pubSubClientsFactory.getConsumerAdapterFactory()
        .create(veniceProperties, false, pubSubMessageDeserializer, clientId);
    pubSubProducerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getProducerAdapterFactory()
            .create(
                new PubSubProducerAdapterContext.Builder().setVeniceProperties(veniceProperties)
                    .setProducerName(clientId)
                    .setBrokerAddress(pubSubBrokerWrapper.getAddress())
                    .build()));
    pubSubAdminAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository));
  }

  protected Properties getPubSubProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS));
    properties.setProperty(PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE, "true");
    properties
        .setProperty(PUBSUB_CONSUMER_POSITION_RESET_STRATEGY, PUBSUB_CONSUMER_POSITION_RESET_STRATEGY_DEFAULT_VALUE);
    return properties;
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapter);
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0);
    }
    if (pubSubAdminAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapterLazy.get());
    }
  }

  // Test: When partitionsFor is called on a non-existent topic, it should return null
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testPartitionsForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long start = System.currentTimeMillis();
    List<PubSubTopicPartitionInfo> partitions = pubSubConsumerAdapter.partitionsFor(nonExistentPubSubTopic);
    long elapsed = System.currentTimeMillis() - start;
    assertNull(partitions, "Partitions should be null for a non-existent topic");
    assertTrue(elapsed <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "PartitionsFor should not block");
  }

  // Test: When partitionsFor is called on an existing topic, it should return a list of partitions
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "PartitionsFor should not block");
  }

  // Test: When endOffsets is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testEndOffsetsForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    List<PubSubTopicPartition> partitions = new ArrayList<>(2);
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0));
    partitions.add(new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 1));

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffsets(partitions, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called on an existing topic, it should return a map of partition to end offset
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called for a non-existent and existing topic in the same API call,
  // it should throw a PubSubOpTimeoutException.
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffsets is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When endOffset (without explicit timeout) is called on a non-existent partition,
  // it should throw PubSubOpTimeoutException after the default API timeout.
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "endOffset should not block");

    // try to get the end offset for a non-existent topic partition
    startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.endOffset(partitions.get(1)));
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be around PUBSUB_OP_TIMEOUT but not too much greater
    assertTrue(elapsedTime <= 2 * PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "endOffset should not block");
  }

  // Test: When beginningOffset is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testBeginningOffsetForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When beginningOffset is called on an existing topic, it should return an offset
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "beginningOffset should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "beginningOffset should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 2);
    startTime = System.currentTimeMillis();
    beginningOffset = pubSubConsumerAdapter.beginningOffset(partition, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(beginningOffset, 0, "Beginning offset should be 0 for an existing topic");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "beginningOffset should not block");
  }

  // Test: When beginningOffset is called on an existing topic with a non-existent partition, it should throw
  // PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on a non-existent topic, it should throw PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testOffsetForTimeForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(partition, 0, PUBSUB_OP_TIMEOUT));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition but no messages, it should return
  // null
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");
  }

  // Test: When offsetForTime is called on an existing topic with invalid partition, it should throw
  // PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");

    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> pubSubConsumerAdapter.offsetForTime(invalidPartition, System.currentTimeMillis(), PUBSUB_OP_TIMEOUT));
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Timeout should be around the specified timeout but not too much greater");
  }

  // Test: When offsetForTime is called on an existing topic with a valid partition and messages, it should return an
  // offset
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
      assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");
    }

    // try 0 as timestamp; this should return the first offset
    startTime = System.currentTimeMillis();
    Long offset = pubSubConsumerAdapter.offsetForTime(partitionWitMessages, 0, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");

    // check one month before the first message timestamp; this should return the first offset
    long oneMonthBeforeFirstMessageTimestamp = timestamps.get(0) - Duration.ofDays(30).toMillis();
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter
        .offsetForTime(partitionWitMessages, oneMonthBeforeFirstMessageTimestamp, PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNotNull(offset, "Offset should not be null for an existing topic with messages");
    assertEquals(offset, Long.valueOf(0), "Offset should match for an existing topic with messages");
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");

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
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "offsetForTime should not block");
  }

  // Test: When offsetForTime (without explicit timeout) is called on a non-existent topic,
  // it should throw PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testOffsetForTimeWithoutExplicitTimeoutForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubOpTimeoutException.class, () -> pubSubConsumerAdapter.offsetForTime(partition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be around the default timeout but not too much greater");
  }

  // Test: When offsetForTime (without explicit timeout) is called on an existing topic with invalid partition,
  // it should throw PubSubOpTimeoutException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be around the default timeout but not too much greater");
  }

  // Test: When offsetForTime (without explicit timeout) is called on an existing topic with a valid partition but no
  // messages, it should return null
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "offsetForTime should not block");

    partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 1);
    startTime = System.currentTimeMillis();
    offset = pubSubConsumerAdapter.offsetForTime(partition, System.currentTimeMillis());
    elapsedTime = System.currentTimeMillis() - startTime;
    assertNull(offset, "Offset should be null for an existing topic with no messages");
    assertTrue(elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE, "OffsetForTime should not block");
  }

  // Test: Subscribe to non-existent topic should throw PubSubTopicDoesNotExistException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testSubscribeForNonExistentTopic() {
    PubSubTopic nonExistentPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("non-existent-topic-"));
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);

    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(nonExistentPubSubTopic), "Topic should not exist");
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubTopicDoesNotExistException.class, () -> pubSubConsumerAdapter.subscribe(partition, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(partition), "Should not be subscribed to any topic");
  }

  // Test: Subscribe to an existing topic with a non-existent partition should throw PubSubTopicDoesNotExistException
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");
  }

  // Test: Subscribe to an existing topic with an existing partition should not take longer than the default timeout
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    // re-subscribe to the same topic and partition; this should not take longer than the default timeout
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.subscribe(partition, 0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    // check subscription status
    startTime = System.currentTimeMillis();
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    startTime = System.currentTimeMillis();
    assertTrue(pubSubConsumerAdapter.hasSubscription(partition), "Should be subscribed to the topic and partition");
    elapsedTime = System.currentTimeMillis() - startTime;
    // elapsed time should be less than the default timeout
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");
  }

  // Test: Unsubscribe to an existing topic and a non-existent topic should not take longer than the default timeout
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    // unsubscribe from a non-existent topic and partition
    PubSubTopicPartition nonExistentPartition = new PubSubTopicPartitionImpl(nonExistentPubSubTopic, 0);
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.unSubscribe(nonExistentPartition);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");
  }

  // Test: Batch unsubscribe to an existing topic and a non-existent topic should not take longer than the default
  // timeout
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
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
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    // verify that the subscription is removed
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(validPartition), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(invalidPartition), "Should not be subscribed to any topic");
  }

  // Test: resetOffset should not take longer than the default timeout when called on an existing topic with a valid
  // partition and subscription
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testResetOffsetForExistingTopicWithValidPartitionAndSubscription()
      throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;
    int numMessages = 10;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    CompletableFuture<PubSubProduceResult> lastMessageFuture = null;
    for (int i = 0; i < numMessages; i++) {
      lastMessageFuture = pubSubProducerAdapter.sendMessage(
          existingPubSubTopic.getName(),
          0,
          PubSubHelper.getDummyKey(),
          PubSubHelper.getDummyValue(),
          null,
          null);
    }

    assertNotNull(lastMessageFuture, "Last message future should not be null");
    lastMessageFuture.get(10, TimeUnit.SECONDS);

    // subscribe to the topic and partition
    pubSubConsumerAdapter.subscribe(partition, 0);
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partition), "Should be subscribed to the topic and partition");

    // consume 5 messages
    int minRecordsToConsume = 5;
    long offsetOfLastConsumedMessage = -1;
    while (minRecordsToConsume > 0) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumerAdapter.poll(15);
      assertNotNull(messages, "Messages should not be null");
      List<DefaultPubSubMessage> partitionMessages = messages.get(partition);
      if (partitionMessages != null && !partitionMessages.isEmpty()) {
        minRecordsToConsume -= partitionMessages.size();
        offsetOfLastConsumedMessage =
            partitionMessages.get(partitionMessages.size() - 1).getPosition().getNumericOffset();
      }
    }
    assertTrue(offsetOfLastConsumedMessage > 0, "Offset of last consumed message should be greater than 0");

    // reset offset to the beginning
    long startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.resetOffset(partition);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");

    minRecordsToConsume = 1;
    long offsetOfFirstConsumedMessage = -1L;
    while (minRecordsToConsume > 0) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumerAdapter.poll(15);
      List<DefaultPubSubMessage> partitionMessages = messages.get(partition);
      if (partitionMessages != null && !partitionMessages.isEmpty()) {
        offsetOfFirstConsumedMessage = partitionMessages.get(0).getPosition().getNumericOffset();
        minRecordsToConsume--;
      }
    }
    // verify that the offset of the first consumed message is 0
    assertEquals(offsetOfFirstConsumedMessage, 0, "Offset of first consumed message should be 0 after resetOffset");
  }

  // Test: resetOffset should throw PubSubUnsubscribedTopicPartitionException when called on an existing topic with a
  // valid partition but no subscription
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testResetOffsetForExistingTopicWithValidPartitionButNoSubscription() {
    PubSubTopic existingPubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existing-topic-"));
    int numPartitions = 2;

    pubSubAdminAdapterLazy.get()
        .createTopic(existingPubSubTopic, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingPubSubTopic, 0);
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(existingPubSubTopic), "Topic should exist");

    // verify that there is no subscription
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should not be subscribed to any topic");
    assertFalse(pubSubConsumerAdapter.hasSubscription(partition), "Should not be subscribed to any topic");

    // reset offset to the beginning
    long startTime = System.currentTimeMillis();
    assertThrows(PubSubUnsubscribedTopicPartitionException.class, () -> pubSubConsumerAdapter.resetOffset(partition));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "Timeout should be less than the default timeout");
  }

  // Test: poll works as expected when called on an existing topic with a valid partition and subscription.
  // poll should not block for longer than the specified timeout even when consumer is subscribed to multiple
  // topic-partitions and some topic-partitions do not exist.
  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testPollPauseResume() throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic topicA = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));
    PubSubTopic topicB = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    int numPartitions = 2;
    int numMessages = 2048;

    pubSubAdminAdapterLazy.get().createTopic(topicA, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    pubSubAdminAdapterLazy.get().createTopic(topicB, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition partitionA0 = new PubSubTopicPartitionImpl(topicA, 0);
    PubSubTopicPartition partitionA1 = new PubSubTopicPartitionImpl(topicA, 1);
    PubSubTopicPartition partitionB0 = new PubSubTopicPartitionImpl(topicB, 0);
    PubSubTopicPartition partitionB1 = new PubSubTopicPartitionImpl(topicB, 1);

    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(topicA), "Topic should exist");
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(topicB), "Topic should exist");

    // subscribe to the topic and partition
    pubSubConsumerAdapter.subscribe(partitionA0, -1);
    pubSubConsumerAdapter.subscribe(partitionA1, -1);
    pubSubConsumerAdapter.subscribe(partitionB0, -1);
    pubSubConsumerAdapter.subscribe(partitionB1, -1);
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA0), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA1), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB0), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic and partition");

    // produce messages to the topic
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    Map<PubSubTopicPartition, CompletableFuture<PubSubProduceResult>> lastMessageFutures = new HashMap<>(numPartitions);
    for (int i = 0; i < numMessages; i++) {
      lastMessageFutures.put(
          partitionA0,
          pubSubProducerAdapter
              .sendMessage(topicA.getName(), 0, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionA1,
          pubSubProducerAdapter
              .sendMessage(topicA.getName(), 1, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionB0,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 0, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionB1,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 1, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
    }

    CompletableFuture.allOf(lastMessageFutures.values().toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
    // check end offsets
    long startTime = System.currentTimeMillis();
    Map<PubSubTopicPartition, Long> endOffsets = pubSubConsumerAdapter.endOffsets(
        new HashSet<>(Arrays.asList(partitionA0, partitionA1, partitionB0, partitionB1)),
        PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "endOffsets should not block");

    assertEquals(endOffsets.get(partitionA0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionA1), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB1), Long.valueOf(numMessages), "End offset should match");

    // poll at lest "minRecordsToConsume" messages per topic-partition; since poll does not guarantee even distribution
    // of messages across topic-partitions, we poll until we get at least "minRecordsToConsume" messages per
    // topic-partition
    int minRecordsToConsume = 2;
    long pollTimeout = 1;
    long pollTimeoutWithVariance = pollTimeout + 3000;
    // keep track of the last consumed offset for each topic-partition
    Map<PubSubTopicPartition, Long> lastConsumedOffsetMap = new HashMap<>(4);
    // keep track of the number of messages consumed for each topic-partition
    Map<PubSubTopicPartition, Integer> numMessagesConsumedMap = new HashMap<>(4);
    // first consumed message offset map
    Map<PubSubTopicPartition, Long> firstConsumedOffsetMap = new HashMap<>(4);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = null;
    Set<PubSubTopicPartition> consumptionBarMet = new HashSet<>();
    while (consumptionBarMet.size() != 4) {
      startTime = System.currentTimeMillis();
      messages = pubSubConsumerAdapter.poll(pollTimeout);
      elapsedTime = System.currentTimeMillis() - startTime;
      // check that poll did not block for longer than the timeout; add variance of 3 seconds
      assertTrue(elapsedTime <= pollTimeoutWithVariance, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");

      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // update first consumed offset
        Long oldVal =
            firstConsumedOffsetMap.putIfAbsent(partition, partitionMessages.get(0).getPosition().getNumericOffset());
        if (oldVal == null) {
          // assert offset is zero since this is the first time we are consuming from this topic-partition and
          // and we started consuming from the beginning (-1 last consumed offset)
          assertEquals(firstConsumedOffsetMap.get(partition), Long.valueOf(0), "First consumed offset should be 0");
        }
        // update last consumed offset
        lastConsumedOffsetMap
            .put(partition, partitionMessages.get(partitionMessages.size() - 1).getPosition().getNumericOffset());
        // update number of messages consumed so far
        numMessagesConsumedMap
            .compute(partition, (k, v) -> v == null ? partitionMessages.size() : v + partitionMessages.size());
        long consumedCountSoFar = numMessagesConsumedMap.get(partition);
        // verify lastConsumedOffset matches records consumed so far
        assertEquals(
            lastConsumedOffsetMap.get(partition),
            Long.valueOf(consumedCountSoFar - 1),
            "Last consumed offset should match records consumed so far");
        if (consumedCountSoFar >= minRecordsToConsume) {
          consumptionBarMet.add(partition);
        }
      }
    }

    // pause subscription to A0, B0
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.pause(partitionA0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "Pause should not block for longer than the timeout");

    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.pause(partitionB0);
    elapsedTime = System.currentTimeMillis() - startTime;
    // check that pause did not block for longer than the timeout; add variance of 5 seconds
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "Pause should not block for longer than the timeout");

    // subscription should still be active for all
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA0), "Should be subscribed to the topic-partition: A0");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA1), "Should be subscribed to the topic-partition: A1");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB0), "Should be subscribed to the topic-partition: B0");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic-partition: B1");

    // consume all messages for A1; and check that no messages are consumed for A0, B0, and B1
    long endOffsetOfA1 = endOffsets.get(partitionA1);
    while (lastConsumedOffsetMap.get(partitionA1) != (endOffsetOfA1 - 1)) {
      System.out.println(lastConsumedOffsetMap);
      startTime = System.currentTimeMillis();
      messages = pubSubConsumerAdapter.poll(pollTimeout);
      elapsedTime = System.currentTimeMillis() - startTime;
      // check that poll did not block for longer than the timeout; add variance of 3 seconds
      assertTrue(elapsedTime <= pollTimeout + 3000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");
      // verify that no messages are consumed for A0, B0
      assertNull(messages.get(partitionA0), "Messages should be null for paused topic-partition: A0");
      assertNull(messages.get(partitionB0), "Messages should be null for paused topic-partition: B0");

      // Update A1 and B1
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // update last consumed offset
        lastConsumedOffsetMap
            .put(partition, partitionMessages.get(partitionMessages.size() - 1).getPosition().getNumericOffset());
        int consumedCountSoFar = numMessagesConsumedMap.getOrDefault(partition, 0) + partitionMessages.size();
        // verify lastConsumedOffset matches records consumed so far
        assertEquals(
            lastConsumedOffsetMap.get(partition),
            Long.valueOf(consumedCountSoFar - 1),
            "Last consumed offset should match records consumed so far");

        numMessagesConsumedMap.put(partition, consumedCountSoFar);
      }
    }

    // check A1's last consumed offset and consumed count
    assertEquals(
        lastConsumedOffsetMap.get(partitionA1),
        Long.valueOf(numMessages - 1),
        "Last consumed offset should match number of messages");
    lastConsumedOffsetMap.remove(partitionA1);
    numMessagesConsumedMap.remove(partitionA1);
    firstConsumedOffsetMap.remove(partitionA1);
    pubSubConsumerAdapter.resetOffset(partitionA1);

    // resume subscription to A0
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.resume(partitionA0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "Resume should not block for longer than the timeout");

    // resume subscription to B0
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.resume(partitionB0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "Resume should not block for longer than the timeout");

    consumptionBarMet.clear(); // reset consumption bar

    while (consumptionBarMet.size() != 4) {
      startTime = System.currentTimeMillis();
      messages = pubSubConsumerAdapter.poll(pollTimeout);
      elapsedTime = System.currentTimeMillis() - startTime;
      // check that poll did not block for longer than the timeout; add variance of 3 seconds
      assertTrue(elapsedTime <= pollTimeout + 3000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");

      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // check A1 starts from 0
        Long oldVal =
            firstConsumedOffsetMap.putIfAbsent(partition, partitionMessages.get(0).getPosition().getNumericOffset());
        if (oldVal == null) {
          // we reset the offset for A1 to beginning; so the first consumed offset should be 0
          assertEquals(firstConsumedOffsetMap.get(partition), Long.valueOf(0), "First consumed offset should be 0");
        }

        // update last consumed offset
        lastConsumedOffsetMap
            .put(partition, partitionMessages.get(partitionMessages.size() - 1).getPosition().getNumericOffset());
        int consumedCountSoFar = numMessagesConsumedMap.getOrDefault(partition, 0) + partitionMessages.size();
        // verify lastConsumedOffset matches records consumed so far
        assertEquals(
            lastConsumedOffsetMap.get(partition),
            Long.valueOf(consumedCountSoFar - 1),
            "Last consumed offset should match records consumed so far");
        numMessagesConsumedMap.put(partition, consumedCountSoFar);
      }

      // loop through all topic-partitions and check if consumption bar is met
      for (PubSubTopicPartition partition: lastConsumedOffsetMap.keySet()) {
        if (numMessagesConsumedMap.get(partition) >= numMessages) {
          consumptionBarMet.add(partition);
        }
      }
    }

    firstConsumedOffsetMap.clear();
    lastConsumedOffsetMap.clear();
    numMessagesConsumedMap.clear();
    consumptionBarMet.clear();

    // we will reset the offset for A0, A1, and B0 to beginning; then delete topic B; reset offset for B1 to
    // the 10th message and then finish consuming A0 and A1. We will unpause B0 and B1.

    // reset offset for A0 to beginning
    pubSubConsumerAdapter.resetOffset(partitionA0);
    // reset offset for A1 to beginning
    pubSubConsumerAdapter.resetOffset(partitionA1);
    // reset offset for B0 to beginning
    pubSubConsumerAdapter.resetOffset(partitionB0);
    // delete topic B
    pubSubAdminAdapterLazy.get().deleteTopic(topicB, Duration.ofMinutes(3)); // poison pill
    // check that topic B is deleted
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(topicB), "Topic should not exist");
    // check B0 and B1 are still subscribed
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB0), "Should be subscribed to the topic-partition: B0");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic-partition: B1");

    // reset offset for B1
    pubSubConsumerAdapter.resetOffset(partitionB1);

    // consume all messages for A0 and A1; and check that no messages are consumed for B0 and B1
    numMessagesConsumedMap.put(partitionA0, 0);
    numMessagesConsumedMap.put(partitionA1, 0);
    numMessagesConsumedMap.put(partitionB0, 0);
    numMessagesConsumedMap.put(partitionB1, 0);

    while (numMessagesConsumedMap.get(partitionA0) != numMessages
        || numMessagesConsumedMap.get(partitionA1) != numMessages) {
      startTime = System.currentTimeMillis();
      messages = pubSubConsumerAdapter.poll(pollTimeout);
      elapsedTime = System.currentTimeMillis() - startTime;
      // check that poll did not block for longer than the timeout; add variance of 3 seconds
      assertTrue(elapsedTime <= pollTimeout + 3000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");
      // verify that no messages are consumed for B0, B1
      assertNull(messages.get(partitionB0), "Messages should be null for deleted topic-partition: B0");
      assertNull(messages.get(partitionB1), "Messages should be null for deleted topic-partition: B1");

      // Update A0 and A1
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // update last consumed offset
        lastConsumedOffsetMap
            .put(partition, partitionMessages.get(partitionMessages.size() - 1).getPosition().getNumericOffset());
        int consumedCountSoFar = numMessagesConsumedMap.getOrDefault(partition, 0) + partitionMessages.size();
        // verify lastConsumedOffset matches records consumed so far
        assertEquals(
            lastConsumedOffsetMap.get(partition),
            Long.valueOf(consumedCountSoFar - 1),
            "Last consumed offset should match records consumed so far");

        numMessagesConsumedMap.put(partition, consumedCountSoFar);
      }
    }

    // unsubscribe B0
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.unSubscribe(partitionB0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Unsubscribe should not block for longer than the timeout");

    // check that B0 is unsubscribed
    assertFalse(
        pubSubConsumerAdapter.hasSubscription(partitionB0),
        "Should not be subscribed to the topic-partition: B0");
    // check that B1 is still subscribed
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic-partition: B1");

    // batch unsubscribe A0, A1, and B1
    Set<PubSubTopicPartition> partitions = new HashSet<>(Arrays.asList(partitionA0, partitionA1, partitionB1));
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.batchUnsubscribe(partitions);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Batch unsubscribe should not block for longer than the timeout");

    // We use the "earliest" offset reset policy in prod. This means that if we subscribe with an offset
    // greater than the end offset, the consumer will seek to the beginning of the partition. Subsequent polls
    // will return the first message in the partition.
    assertFalse(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    pubSubConsumerAdapter.subscribe(partitionA0, endOffsets.get(partitionA0)); // sub will add +1 to the offset
    messages = Collections.emptyMap();
    while (messages.isEmpty()) {
      messages = pubSubConsumerAdapter.poll(pollTimeout);
      if (messages.isEmpty() || messages.get(partitionA0) == null || messages.get(partitionA0).isEmpty()) {
        messages = Collections.emptyMap();
      }
    }
    assertTrue(messages.containsKey(partitionA0), "Should have messages for A0");
    assertTrue(messages.get(partitionA0).size() > 0, "Should have messages for A0");
    // offset should be at the first message
    assertEquals(
        messages.get(partitionA0).get(0).getPosition().getNumericOffset(),
        0,
        "Poll should start from the beginning");
  }

  // Note: The following test may not work for non-Kafka PubSub implementations.
  // Therefore, it is in Venice's best interest to unsubscribe from a topic-partition before deleting it,
  // and then subscribe again after recreating the topic.
  // If we rely on the consumer to keep polling data when the topic is recreated, we may observe different behaviors
  // with different PubSub implementations. This should be avoided at all costs.
  // The purpose of the test is to examine how the consumer behaves when a topic is deleted
  @Test(timeOut = 3 * Time.MS_PER_MINUTE, enabled = false)
  public void testResetOffsetDeleteTopicRecreateTopic()
      throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic topicA = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));
    PubSubTopic topicB = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    int numPartitions = 2;
    int numMessages = 1000;

    pubSubAdminAdapterLazy.get().createTopic(topicA, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    pubSubAdminAdapterLazy.get().createTopic(topicB, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);

    PubSubTopicPartition partitionA0 = new PubSubTopicPartitionImpl(topicA, 0);
    PubSubTopicPartition partitionA1 = new PubSubTopicPartitionImpl(topicA, 1);
    PubSubTopicPartition partitionB0 = new PubSubTopicPartitionImpl(topicB, 0);
    PubSubTopicPartition partitionB1 = new PubSubTopicPartitionImpl(topicB, 1);

    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(topicA), "Topic should exist");
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(topicB), "Topic should exist");

    // subscribe to the topic and partition
    pubSubConsumerAdapter.subscribe(partitionA0, -1);
    pubSubConsumerAdapter.subscribe(partitionA1, -1);
    pubSubConsumerAdapter.subscribe(partitionB0, -1);
    pubSubConsumerAdapter.subscribe(partitionB1, -1);
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA0), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionA1), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB0), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic and partition");

    // produce messages to the topic
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    Map<PubSubTopicPartition, CompletableFuture<PubSubProduceResult>> lastMessageFutures = new HashMap<>(numPartitions);
    for (int i = 0; i < numMessages; i++) {
      lastMessageFutures.put(
          partitionA0,
          pubSubProducerAdapter
              .sendMessage(topicA.getName(), 0, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionA1,
          pubSubProducerAdapter
              .sendMessage(topicA.getName(), 1, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionB0,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 0, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionB1,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 1, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
    }

    CompletableFuture.allOf(lastMessageFutures.values().toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
    // check end offsets
    long startTime = System.currentTimeMillis();
    Map<PubSubTopicPartition, Long> endOffsets = pubSubConsumerAdapter.endOffsets(
        new HashSet<>(Arrays.asList(partitionA0, partitionA1, partitionB0, partitionB1)),
        PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "endOffsets should not block");

    assertEquals(endOffsets.get(partitionA0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionA1), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB1), Long.valueOf(numMessages), "End offset should match");

    Set<PubSubTopicPartition> consumptionBarMet = new HashSet<>();
    while (consumptionBarMet.size() != 4) {
      startTime = System.currentTimeMillis();
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumerAdapter.poll(1);
      elapsedTime = System.currentTimeMillis() - startTime;
      // roughly pollTimeout * retries + (retries - 1) * backoff. Let's use 10 seconds as the upper bound
      assertTrue(elapsedTime <= 10000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");

      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // check offset of the last consumed message and see if it is one less than the end offset; if yes bar is met
        if (partitionMessages.get(partitionMessages.size() - 1)
            .getPosition()
            .getNumericOffset() == endOffsets.get(partition) - 1) {
          consumptionBarMet.add(partition);
        }
      }
    }

    // reset offset for A0 to beginning
    pubSubConsumerAdapter.resetOffset(partitionA0);
    // reset offset for B0 to beginning
    pubSubConsumerAdapter.resetOffset(partitionB0);
    // delete topic B
    pubSubAdminAdapterLazy.get().deleteTopic(topicB, Duration.ofMinutes(3)); // poison pill
    // check that topic B is deleted
    assertFalse(pubSubAdminAdapterLazy.get().containsTopic(topicB), "Topic should not exist");
    // check B0 and B1 are still subscribed
    assertTrue(pubSubConsumerAdapter.hasAnySubscription(), "Should be subscribed to the topic and partition");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB0), "Should be subscribed to the topic-partition: B0");
    assertTrue(pubSubConsumerAdapter.hasSubscription(partitionB1), "Should be subscribed to the topic-partition: B1");

    consumptionBarMet.remove(partitionA0); // reset consumption bar for A0
    consumptionBarMet.remove(partitionB0); // reset consumption bar for B0
    consumptionBarMet.remove(partitionB1); // reset consumption bar for B1

    while (consumptionBarMet.size() != 2) {
      startTime = System.currentTimeMillis();
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumerAdapter.poll(1);
      elapsedTime = System.currentTimeMillis() - startTime;
      // check that poll did not block for longer than the timeout; add variance of 3 seconds
      assertTrue(elapsedTime <= 1000 + 3000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");
      // check that no messages are consumed for B0, B1, and A1
      assertNull(messages.get(partitionB0), "Messages should be null for deleted topic-partition: B0");
      assertNull(messages.get(partitionB1), "Messages should be null for deleted topic-partition: B1");
      assertNull(messages.get(partitionA1), "Messages should be null for deleted topic-partition: A1");

      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // check offset of the last consumed message and see if it is one less than the end offset; if yes bar is met
        if (partitionMessages.get(partitionMessages.size() - 1)
            .getPosition()
            .getNumericOffset() == endOffsets.get(partition) - 1) {
          consumptionBarMet.add(partition);
        }
      }
    }

    // recreate topic B
    pubSubAdminAdapterLazy.get().createTopic(topicB, numPartitions, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    // check that topic B is created
    assertTrue(pubSubAdminAdapterLazy.get().containsTopic(topicB), "Topic should exist");
    // produce messages to the B
    for (int i = 0; i < numMessages; i++) {
      lastMessageFutures.put(
          partitionB0,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 0, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
      lastMessageFutures.put(
          partitionB1,
          pubSubProducerAdapter
              .sendMessage(topicB.getName(), 1, PubSubHelper.getDummyKey(), PubSubHelper.getDummyValue(), null, null));
    }

    CompletableFuture.allOf(lastMessageFutures.values().toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
    // check end offsets
    startTime = System.currentTimeMillis();
    endOffsets = pubSubConsumerAdapter.endOffsets(
        new HashSet<>(Arrays.asList(partitionA0, partitionA1, partitionB0, partitionB1)),
        PUBSUB_OP_TIMEOUT);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE, "endOffsets should not block");

    assertEquals(endOffsets.get(partitionA0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionA1), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB0), Long.valueOf(numMessages), "End offset should match");
    assertEquals(endOffsets.get(partitionB1), Long.valueOf(numMessages), "End offset should match");

    // only messages for B0 should be consumed as B1 reached end offset before deletion and recreation; A0 and A1
    // should not consume any messages as they already consumed all messages
    while (consumptionBarMet.size() != 3) {
      startTime = System.currentTimeMillis();
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumerAdapter.poll(1);
      elapsedTime = System.currentTimeMillis() - startTime;
      // roughly pollTimeout * retries + (retries - 1) * backoff. Let's use 10 seconds as the upper bound
      assertTrue(elapsedTime <= 10000, "Poll should not block for longer than the timeout");
      assertNotNull(messages, "Messages should not be null");
      // check that no messages are consumed for A0, A1, B1
      assertNull(messages.get(partitionA0), "Messages should be null for topic-partition: A0");
      assertNull(messages.get(partitionA1), "Messages should be null for topic-partition: A1");
      assertNull(messages.get(partitionB1), "Messages should be null for topic-partition: B1");

      // Update B0
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        PubSubTopicPartition partition = entry.getKey();
        List<DefaultPubSubMessage> partitionMessages = entry.getValue();
        if (partitionMessages == null || partitionMessages.isEmpty()) {
          continue;
        }
        // check offset of the last consumed message and see if it is one less than the end offset; if yes bar is met
        if (partitionMessages.get(partitionMessages.size() - 1)
            .getPosition()
            .getNumericOffset() == endOffsets.get(partition) - 1) {
          consumptionBarMet.add(partition);
        }
      }
    }

    // unsubscribe B0
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.unSubscribe(partitionB0);
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Unsubscribe should not block for longer than the timeout");

    // batch unsubscribe A0, A1, and B1
    startTime = System.currentTimeMillis();
    pubSubConsumerAdapter.batchUnsubscribe(new HashSet<>(Arrays.asList(partitionA0, partitionA1, partitionB1)));
    elapsedTime = System.currentTimeMillis() - startTime;
    assertTrue(
        elapsedTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "Unsubscribe should not block for longer than the timeout");
  }
}
