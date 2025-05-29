package com.linkedin.venice.pubsub.api.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubAdminAdapterContext;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link PubSubAdminAdapter} implementations.
 */
public class PubSubAdminAdapterTest {
  public static final long TEST_TIMEOUT_MS = 180_000;

  // timeout for pub-sub operations
  private static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(25);
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_OP_TIMEOUT_WITH_VARIANCE = PUBSUB_OP_TIMEOUT.toMillis() + 5000;
  // timeout for pub-sub admin APIs which do not have a timeout parameter
  private static final int PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS = 15_000;
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE =
      PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS + 5000;
  private static final int REPLICATION_FACTOR = 1;
  private static final int PARTITION_COUNT = 3;
  private static final boolean IS_LOG_COMPACTED = false;
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final long MAX_LOG_COMPACTION_LAG_MS = Duration.ofDays(2).toMillis();
  private static final long RETENTION_IN_MS = Duration.ofDays(3).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS,
      Optional.of(MAX_LOG_COMPACTION_LAG_MS));

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private PubSubAdminAdapter pubSubAdminAdapter;
  private Lazy<PubSubConsumerAdapter> pubSubConsumerAdapterLazy;
  private Lazy<PubSubProducerAdapter> pubSubProducerAdapterLazy;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubClientsFactory pubSubClientsFactory;

  @BeforeClass(alwaysRun = true)
  public void setUp() {

  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubMessageDeserializer = PubSubMessageDeserializer.createDefaultDeserializer();
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubClientsFactory = pubSubBrokerWrapper.getPubSubClientsFactory();

    String clientId = Utils.getUniqueString("test-admin-");
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(
        PubSubConstants.PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS));

    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    pubSubAdminAdapter = pubSubClientsFactory.getAdminAdapterFactory()
        .create(
            new PubSubAdminAdapterContext.Builder().setAdminClientName(clientId)
                .setVeniceProperties(veniceProperties)
                .setPubSubTopicRepository(pubSubTopicRepository)
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .build());
    pubSubConsumerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getConsumerAdapterFactory()
            .create(
                new PubSubConsumerAdapterContext.Builder().setVeniceProperties(veniceProperties)
                    .setPubSubMessageDeserializer(pubSubMessageDeserializer)
                    .setConsumerName(clientId)
                    .build()));
    pubSubProducerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getProducerAdapterFactory()
            .create(
                new PubSubProducerAdapterContext.Builder().setVeniceProperties(veniceProperties)
                    .setProducerName(clientId)
                    .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                    .build()));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapter);
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0);
    }
    if (pubSubConsumerAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapterLazy.get());
    }

    if (pubSubBrokerWrapper.isRunning()) {
      Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
    }
  }

  // Test: createTopic should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testCreateAndDeleteTopic() {
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // createTopic should succeed and should be completed within the timeout if the broker is running
    assertFalse(pubSubAdminAdapter.containsTopic(pubSubTopic));
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    long elapsedTime = System.currentTimeMillis();
    assertTrue(pubSubAdminAdapter.containsTopic(pubSubTopic));
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "createTopic should finish within the timeout");

    PubSubTopic pubSubTopicB = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    assertFalse(pubSubAdminAdapter.containsTopic(pubSubTopicB));

    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubClientRetriableException.class,
        () -> pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime < PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "createTopic should finish within the timeout");
  }

  // Test: deleteTopic should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testDeleteTopic() {
    PubSubTopic existentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // deleteTopic should succeed and should be completed within the timeout if the broker is running
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic));
    pubSubAdminAdapter.createTopic(existentTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapter.containsTopic(existentTopic));
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.deleteTopic(existentTopic, PUBSUB_OP_TIMEOUT);
    long elapsedTime = System.currentTimeMillis();
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic), "deleteTopic should succeed");
    assertTrue(
        elapsedTime - startTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "deleteTopic should finish within the timeout");

    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    assertFalse(pubSubAdminAdapter.containsTopic(nonExistentTopic), "Topic should not exist");

    // deleting a non-existent topic should fail and should be completed within the timeout if the broker is running
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> pubSubAdminAdapter.deleteTopic(nonExistentTopic, PUBSUB_OP_TIMEOUT));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "deleteTopic should finish within the timeout");

    // deleteTopic should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // deleteTopic should fail and should be completed within the timeout if the broker is not running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubClientRetriableException.class,
        () -> pubSubAdminAdapter.deleteTopic(existentTopic, PUBSUB_OP_TIMEOUT));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_OP_TIMEOUT_WITH_VARIANCE,
        "deleteTopic should finish within the timeout");
  }

  // Test: getTopicConfig should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testGetTopicConfig() {
    PubSubTopic existentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // getTopicConfig should succeed and should be completed within the timeout if the broker is running
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic));
    pubSubAdminAdapter.createTopic(existentTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapter.containsTopic(existentTopic));
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.getTopicConfig(existentTopic);
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "getTopicConfig should finish within the timeout");

    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    assertFalse(pubSubAdminAdapter.containsTopic(nonExistentTopic), "Topic should not exist");

    // getTopicConfig should fail and should be completed within the timeout if the topic does not exist
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.getTopicConfig(nonExistentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "getTopicConfig should finish within the timeout");

    // getTopicConfig should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // getTopicConfig should fail and should be completed within the timeout if the broker is not running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.getTopicConfig(existentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "getTopicConfig should finish within the timeout");
  }

  // Test: setTopicConfig should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testSetTopicConfig() {
    PubSubTopic existentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // setTopicConfig should succeed and should be completed within the timeout if the broker is running
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic));
    pubSubAdminAdapter.createTopic(existentTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapter.containsTopic(existentTopic));
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.setTopicConfig(existentTopic, TOPIC_CONFIGURATION);
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "setTopicConfig should finish within the timeout");

    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    assertFalse(pubSubAdminAdapter.containsTopic(nonExistentTopic), "Topic should not exist");

    // setTopicConfig should fail and should be completed within the timeout if the topic does not exist
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> pubSubAdminAdapter.setTopicConfig(nonExistentTopic, TOPIC_CONFIGURATION));
    elapsedTime = System.currentTimeMillis();
    // wait for 4 times the default timeout to account for the time taken topic existence checks and retries
    assertTrue(
        elapsedTime - startTime <= (4 * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "setTopicConfig should finish within the timeout");

    // setTopicConfig should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // setTopicConfig should fail and should be completed within the timeout if the broker is not running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubClientRetriableException.class,
        () -> pubSubAdminAdapter.setTopicConfig(existentTopic, TOPIC_CONFIGURATION));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= (4 * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "setTopicConfig should finish within the timeout");
  }

  // Test: containsTopic should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testContainsTopic() {
    PubSubTopic existentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // containsTopic should succeed and should be completed within the timeout if the broker is running
    long startTime = System.currentTimeMillis();
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic));
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopic should finish within the timeout");

    pubSubAdminAdapter.createTopic(existentTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    startTime = System.currentTimeMillis();
    assertTrue(pubSubAdminAdapter.containsTopic(existentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopic should finish within the timeout");

    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    startTime = System.currentTimeMillis();
    assertFalse(pubSubAdminAdapter.containsTopic(nonExistentTopic), "Topic should not exist");
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopic should finish within the timeout");

    // containsTopic should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // containsTopic should fail and should be completed within the timeout if the broker is not running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.containsTopic(existentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopic should finish within the timeout");
  }

  // Test: containsTopicWithPartitionCheck should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testContainsTopicWithPartitionCheck() {
    PubSubTopic existentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));

    // containsTopicWithPartitionCheck should succeed and should be completed within the timeout if the broker is
    // running
    assertFalse(pubSubAdminAdapter.containsTopic(existentTopic));
    pubSubAdminAdapter.createTopic(existentTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    assertTrue(pubSubAdminAdapter.containsTopic(existentTopic));

    PubSubTopicPartition validPartition = new PubSubTopicPartitionImpl(existentTopic, 0);
    PubSubTopicPartition invalidPartition = new PubSubTopicPartitionImpl(existentTopic, PARTITION_COUNT + 1);

    // check for a valid partition
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.containsTopicWithPartitionCheck(validPartition);
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopicWithPartitionCheck should finish within the timeout");

    // check for an invalid partition
    startTime = System.currentTimeMillis();
    pubSubAdminAdapter.containsTopicWithPartitionCheck(invalidPartition);
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopicWithPartitionCheck should finish within the timeout");

    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("diminishing-delight-"));
    assertFalse(pubSubAdminAdapter.containsTopic(nonExistentTopic), "Topic should not exist");

    PubSubTopicPartition invalidPartitionForNonExistentTopic = new PubSubTopicPartitionImpl(nonExistentTopic, 0);

    // containsTopicWithPartitionCheck should fail and should be completed within the timeout if the topic does not
    // exist
    startTime = System.currentTimeMillis();
    assertFalse(pubSubAdminAdapter.containsTopicWithPartitionCheck(invalidPartitionForNonExistentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopicWithPartitionCheck should finish within the timeout");

    // containsTopicWithPartitionCheck should fail and should be completed within the timeout if the broker is not
    // running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // containsTopicWithPartitionCheck should fail and should be completed within the timeout if the broker is not
    // running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(
        PubSubClientRetriableException.class,
        () -> pubSubAdminAdapter.containsTopicWithPartitionCheck(invalidPartitionForNonExistentTopic));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "containsTopicWithPartitionCheck should finish within the timeout");
  }

  // Test: listAllTopics should not block indefinitely
  @Test(timeOut = 5 * TEST_TIMEOUT_MS)
  public void testListAllTopics() {
    // listAllTopics should succeed and should be completed within the timeout if the broker is running
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.listAllTopics();
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "listAllTopics should finish within the timeout");

    int numTopics = 5;
    for (int i = 0; i < numTopics; i++) {
      PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));
      pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    }
    // listAllTopics should succeed and should be completed within the timeout if the broker is running
    startTime = System.currentTimeMillis();
    Set<PubSubTopic> topics = pubSubAdminAdapter.listAllTopics();
    elapsedTime = System.currentTimeMillis();
    assertEquals(topics.size(), numTopics);
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "listAllTopics should finish within the timeout");

    // listAllTopics should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    // listAllTopics should fail and should be completed within the timeout if the broker is not running
    // When explicitly specifying a timeout, the explicit timeout should be honored over the default timeout
    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.listAllTopics());
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "listAllTopics should finish within the timeout");
  }

  // Test: getAllTopicRetentions should not block indefinitely
  @Test(timeOut = 5 * TEST_TIMEOUT_MS)
  public void testGetAllTopicRetentions() {
    // getAllTopicRetentions should succeed and should be completed within the timeout if the broker is running
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.getAllTopicRetentions();
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "getAllTopicRetentions should finish within the timeout");

    int numTopics = 5;
    for (int i = 0; i < numTopics; i++) {
      PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));
      pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    }
    // getAllTopicRetentions should succeed and should be completed within the timeout if the broker is running
    startTime = System.currentTimeMillis();
    Map<PubSubTopic, Long> topicRetentions = pubSubAdminAdapter.getAllTopicRetentions();
    elapsedTime = System.currentTimeMillis();
    assertEquals(topicRetentions.size(), numTopics);
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "getAllTopicRetentions should finish within the timeout");

    // getAllTopicRetentions should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.getAllTopicRetentions());
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "getAllTopicRetentions should finish within the timeout");
  }

  // Test: getSomeTopicConfigs should not block indefinitely
  @Test(timeOut = 5 * TEST_TIMEOUT_MS)
  public void testGetSomeTopicConfigs() {
    Set<PubSubTopic> pubSubTopics = new HashSet<>();
    // getSomeTopicConfigs should succeed and should be completed within the timeout if the broker is running
    long startTime = System.currentTimeMillis();
    pubSubAdminAdapter.getSomeTopicConfigs(pubSubTopics);
    long elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE,
        "getSomeTopicConfigs should finish within the timeout");

    int numTopics = 5;
    for (int i = 0; i < numTopics; i++) {
      PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("timeless-treasure-"));
      pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
      pubSubTopics.add(pubSubTopic);
    }

    // getSomeTopicConfigs should succeed and should be completed within the timeout if the broker is running
    startTime = System.currentTimeMillis();
    Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs = pubSubAdminAdapter.getSomeTopicConfigs(pubSubTopics);
    elapsedTime = System.currentTimeMillis();
    assertEquals(topicConfigs.size(), pubSubTopics.size());
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "getSomeTopicConfigs should finish within the timeout");

    // getSomeTopicConfigs should fail and should be completed within the timeout if the broker is not running
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper); // stop the broker

    assertFalse(pubSubBrokerWrapper.isRunning());
    startTime = System.currentTimeMillis();
    assertThrows(PubSubClientRetriableException.class, () -> pubSubAdminAdapter.getSomeTopicConfigs(pubSubTopics));
    elapsedTime = System.currentTimeMillis();
    assertTrue(
        elapsedTime - startTime <= (numTopics * PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE),
        "getSomeTopicConfigs should finish within the timeout");
  }

  // Test: multithreaded access to PubSubAdminAdapter
  @Test(timeOut = 5 * TEST_TIMEOUT_MS)
  public void testMultithreadedAccess() {
    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier cyclicBarrier = new CyclicBarrier(numThreads);
    List<CompletableFuture<Void>> futures = new ArrayList<>(numThreads);

    for (int i = 0; i < numThreads; i++) {
      futures.add(CompletableFuture.runAsync(() -> {
        try {
          cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new VeniceException("Error while waiting for barrier", e);
        }
        for (int j = 0; j < 20; j++) {
          PubSubTopic pubSubTopic = pubSubTopicRepository
              .getTopic(Utils.getUniqueString("topic-" + Thread.currentThread().getName()) + "-" + j);
          pubSubAdminAdapter.createTopic(pubSubTopic, PARTITION_COUNT, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
          assertTrue(pubSubAdminAdapter.containsTopic(pubSubTopic));
          // getTopicConfig
          pubSubAdminAdapter.getTopicConfig(pubSubTopic);
          // setTopicConfig
          pubSubAdminAdapter.setTopicConfig(pubSubTopic, TOPIC_CONFIGURATION);
          pubSubAdminAdapter.deleteTopic(pubSubTopic, PUBSUB_OP_TIMEOUT);
          assertFalse(pubSubAdminAdapter.containsTopic(pubSubTopic));
        }
      }, executorService));
    }
    CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
      allFutures.get(5, TimeUnit.MINUTES);
    } catch (ExecutionException e) {
      if (!ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)
          && !ExceptionUtils.recursiveClassEquals(e, BrokenBarrierException.class)) {
        fail("Error while executing futures", e);
      }
    } catch (InterruptedException | TimeoutException e) {
      throw new VeniceException("Error while waiting for futures to complete", e);
    }
  }
}
