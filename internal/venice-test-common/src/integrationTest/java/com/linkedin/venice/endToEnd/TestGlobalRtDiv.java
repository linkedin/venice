package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DIV_PRODUCER_STATE_MAX_AGE_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_COMBINER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestGlobalRtDiv {
  private static final Logger LOGGER = LogManager.getLogger(TestGlobalRtDiv.class);

  private VeniceClusterWrapper venice;
  private final String RT_BEFORE = "rt_before_";
  private final String RT_AFTER = "rt_after_";
  private final String VALUE_PREFIX = TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;

  @BeforeClass
  public void setUp() {
    int serverCount = 3;
    final Properties extraProperties = createExtraProperties();
    venice = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(0) // created below
            .numberOfRouters(0) // created below
            .replicationFactor(serverCount) // set RF to number of servers so that all servers have all partitions
            .partitionSize(1000000)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(extraProperties)
            .build());

    Properties routerProperties = new Properties();
    venice.addVeniceRouter(routerProperties);

    Properties serverProperties = new Properties();
    serverProperties.setProperty(KAFKA_OVER_SSL, "false");
    for (int i = 0; i < serverCount; i++) {
      venice.addVeniceServer(serverProperties, extraProperties);
    }

    LOGGER.info("Finished creating VeniceClusterWrapper in setUp()");
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  /**
   * Data provider for parameterized server restart tests.
   *
   * @return Object[][] with the following parameters:
   *   - testName: String identifier for the test case
   *   - isHybridStore: boolean, true for hybrid store, false for batch store
   *   - restartDuringIngestion: boolean, true to restart during ingestion, false to restart after ingestion
   *   - writeRtDataBeforeRestart: boolean, true to write RT data before server restart (hybrid only)
   *   - writeRtDataAfterRestart: boolean, true to write RT data after server restart (hybrid only)
   */
  @DataProvider(name = "serverRestartTestParams")
  public Object[][] serverRestartTestParams() {
    return new Object[][] {
        // testBatchStoreServerRestartDuringIngestion
        { "BatchStoreServerRestartDuringIngestion", false, true, false, false },
        // testBatchStoreServerRestartAfterIngestion
        { "BatchStoreServerRestartAfterIngestion", false, false, false, false },
        // testHybridStoreServerRestartDuringBatchIngestion
        { "HybridStoreServerRestartDuringBatchIngestion", true, true, false, true },
        // testHybridStoreServerRestartDuringRTConsumption
        { "HybridStoreServerRestartDuringRTConsumption", true, false, true, true } };
  }

  /**
   * Unified parameterized test for server restart scenarios.
   * This test combines the functionality of:
   * - testBatchStoreServerRestartDuringIngestion
   * - testBatchStoreServerRestartAfterIngestion
   * - testHybridStoreServerRestartDuringBatchIngestion
   * - testHybridStoreServerRestartDuringRTConsumption
   *
   * Additionally, it can perform a Kafka input re-push after the server restart test,
   * similar to testRepush and testKafkaInputBatchJob in TestBatch.
   *
   * @param testName String identifier for the test case
   * @param isHybridStore true for hybrid store, false for batch store
   * @param restartDuringIngestion true to restart during ingestion, false to restart after ingestion
   * @param writeRtDataBeforeRestart true to write RT data before server restart (hybrid only)
   * @param writeRtDataAfterRestart true to write RT data after server restart (hybrid only)
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND, dataProvider = "serverRestartTestParams")
  public void testServerRestart(
      String testName,
      boolean isHybridStore,
      boolean restartDuringIngestion,
      boolean writeRtDataBeforeRestart,
      boolean writeRtDataAfterRestart) throws Exception {

    LOGGER.info("Running parameterized test: {}", testName);

    // Common test parameters
    String storeName = Utils.getUniqueString(testName.toLowerCase());
    int batchRecordCount = 100;
    int partitionCount = 1;

    // RT data parameters (only used for hybrid tests)
    int rtRecordCountBeforeRestart = 50;
    int rtRecordCountAfterRestart = 50;

    // Use different key ranges for RT data to avoid overwrites
    int rtKeysStartBefore = 101; // Start after batch data (1-100)
    int rtKeysEndBefore = rtKeysStartBefore + rtRecordCountBeforeRestart - 1;
    int rtKeysStartAfter = rtKeysEndBefore + 1; // Start after rt_before keys
    int rtKeysEndAfter = rtKeysStartAfter + rtRecordCountAfterRestart - 1;

    // Setup for batch push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

    // Setup for RT writes (only used for hybrid tests)
    PubSubBrokerWrapper brokerWrapper = venice.getPubSubBrokerWrapper();
    Properties writerProperties = new Properties();
    writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, brokerWrapper.getAddress());
    writerProperties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(brokerWrapper)));
    PubSubProducerAdapterFactory producerFactory = brokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory writerFactory = TestUtils
        .getVeniceWriterFactory(writerProperties, producerFactory, brokerWrapper.getPubSubPositionTypeRegistry());

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {

      // Create store with appropriate configuration (batch or hybrid)
      UpdateStoreQueryParams updateParams = new UpdateStoreQueryParams().setPartitionCount(partitionCount);
      if (isHybridStore) {
        updateParams.setHybridRewindSeconds(10L).setHybridOffsetLagThreshold(2L);
      }
      ControllerResponse response = controllerClient.updateStore(storeName, updateParams);
      assertFalse(response.isError(), "Updating store should succeed");

      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
      String topicName = Version.composeKafkaTopic(storeName, 1);
      String rtTopicName = isHybridStore ? Utils.getRealTimeTopicName(storeInfo) : null;

      Thread pushJobThread = null;
      try {
        if (restartDuringIngestion) {
          // Start push job in a separate thread for "during ingestion" tests
          pushJobThread = new Thread(() -> runVPJ(vpjProperties, 1, controllerClient));
          pushJobThread.start();

          // Wait for ingestion to start
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
            HelixExternalViewRepository routingDataRepo = getRoutingDataRepository();
            assertTrue(routingDataRepo.containsKafkaTopic(topicName), topicName + " should exist");
            Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, 0);
            assertNotNull(leaderNode, "Leader should be assigned");
          });
        } else {
          // Run push job and wait for completion for "after ingestion" tests
          runVPJ(vpjProperties, 1, controllerClient);

          // Wait for version to become current (after EOP)
          TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
            int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
            return currentVersion == 1;
          });

          // Verify batch data
          verifyAllDataCanBeQueried(client, 1, batchRecordCount, VALUE_PREFIX);
          LOGGER.info("Batch data verified before server restart");
        }

        // For hybrid tests with RT data before restart
        if (isHybridStore && writeRtDataBeforeRestart && !restartDuringIngestion) {
          // Write RT data before restart
          LOGGER.info("Writing RT data before restart...");
          writeRTData(rtTopicName, rtKeysStartBefore, rtKeysEndBefore, RT_BEFORE, writerFactory);

          // Verify RT data before restart
          verifyAllDataCanBeQueried(client, rtKeysStartBefore, rtKeysEndBefore, RT_BEFORE);
          LOGGER.info("RT data before restart verified");
        }

        // Get the leader node
        HelixExternalViewRepository routingDataRepo = getRoutingDataRepository();
        Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, 0);
        assertNotNull(leaderNode, "Leader should exist");
        LOGGER.info("Stopping leader server: {}", leaderNode.getNodeId());

        // Stop the leader server
        venice.stopVeniceServer(leaderNode.getPort());

        // Wait for a new leader to be elected
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Instance newLeader = routingDataRepo.getLeaderInstance(topicName, 0);
          assertNotNull(newLeader, "New leader should be elected");
          assertNotEquals(
              newLeader.getNodeId(),
              leaderNode.getNodeId(),
              "New leader should be different from old leader");
        });

        // For hybrid tests with RT data during server down
        if (isHybridStore && writeRtDataAfterRestart && !restartDuringIngestion) {
          // Write RT data while server is down
          LOGGER.info("Writing RT data while server is down...");
          writeRTData(rtTopicName, rtKeysStartAfter, rtKeysEndAfter, RT_AFTER, writerFactory);
        }

        // Restart the old leader server
        LOGGER.info("Restarting old leader server: {}", leaderNode.getNodeId());
        venice.restartVeniceServer(leaderNode.getPort());

        // Wait for server to be fully operational
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          VeniceServerWrapper server = venice.getVeniceServers()
              .stream()
              .filter(s -> s.getPort() == leaderNode.getPort())
              .findFirst()
              .orElse(null);
          assertNotNull(server, "Server should be found");
          assertTrue(server.isRunning(), "Server should be running");
        });

        if (restartDuringIngestion) {
          // For "during ingestion" tests, wait for push to complete
          TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
            int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
            assertEquals(currentVersion, 1, "Current version should become 1");

            // Verify batch data
            for (int i = 1; i <= batchRecordCount; i++) {
              String key = Integer.toString(i);
              try {
                Object value = client.get(key).get();
                assertNotNull(value, "Key " + i + " should not be missing! Data loss detected.");
                assertEquals(value.toString(), VALUE_PREFIX + key, "Value mismatch for key " + i);
              } catch (Exception e) {
                throw new VeniceException("Failed to get key " + i + ": " + e.getMessage(), e);
              }
            }
          });
        }

        // For hybrid tests with RT data after restart
        if (isHybridStore && writeRtDataAfterRestart && restartDuringIngestion) {
          // Write RT data after restart
          LOGGER.info("Writing RT data after restart...");
          writeRTData(rtTopicName, rtKeysStartBefore, rtKeysEndBefore, RT_BEFORE, writerFactory);
        }

        // Final verification of all data
        verifyAllDataCanBeQueried(client, 1, batchRecordCount, VALUE_PREFIX);
        LOGGER.info("Batch data verified after server restart");

        // Verify RT data if applicable
        if (isHybridStore && writeRtDataBeforeRestart && !restartDuringIngestion) {
          verifyAllDataCanBeQueried(client, rtKeysStartBefore, rtKeysEndBefore, RT_BEFORE);
          LOGGER.info("RT data before restart verified after server restart");
        }

        if (isHybridStore && writeRtDataAfterRestart) {
          if (restartDuringIngestion) {
            verifyAllDataCanBeQueried(client, rtKeysStartBefore, rtKeysEndBefore, RT_BEFORE);
          } else {
            verifyAllDataCanBeQueried(client, rtKeysStartAfter, rtKeysEndAfter, RT_AFTER);
          }
          LOGGER.info("RT data after restart verified");
        }

        LOGGER.info("Successfully completed parameterized test: {}", testName);

        // Create properties for Kafka input re-push
        LOGGER.info("Starting Kafka input re-push for test: {} store: {}", testName, storeName);
        vpjProperties.setProperty(SOURCE_KAFKA, "true");
        vpjProperties.setProperty(VENICE_STORE_NAME_PROP, storeName);
        vpjProperties.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, venice.getPubSubBrokerWrapper().getAddress());
        vpjProperties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
        vpjProperties.setProperty(KAFKA_INPUT_COMBINER_ENABLED, "true");

        // Run the Kafka input re-push and verify that a new version (ver = 2) is pushed successfully.
        runVPJ(vpjProperties, 2, controllerClient);

        // Verify all data can be queried after re-push
        verifyAllDataCanBeQueried(client, 1, batchRecordCount, VALUE_PREFIX);
        LOGGER.info("Successfully verified all data after Kafka input re-push");

        LOGGER.info("Successfully completed Kafka input re-push for test: {}", testName);
      } finally {
        if (pushJobThread != null) {
          pushJobThread.interrupt();
        }
      }
    }
  }

  /**
   * Verifies that a batch-only store with Global RT DIV enabled has correctly populated vtSegments
   * after batch ingestion. VPJ producer states should be in vtSegments (VERSION_TOPIC type), not rtSegments.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testBatchOnlyStoreWithGlobalRtDiv() throws Exception {
    int PARTITION = 0;
    int batchRecordCount = 100;
    int partitionCount = 1;

    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchOnlyGlobalRtDiv");
    String topicName = Version.composeKafkaTopic(storeName, 1);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {

      // Create a BATCH-ONLY store with Global RT DIV enabled (no hybrid config)
      UpdateStoreQueryParams updateParams =
          new UpdateStoreQueryParams().setGlobalRtDivEnabled(true).setPartitionCount(partitionCount);
      ControllerResponse response = controllerClient.updateStore(storeName, updateParams);
      assertFalse(response.isError(), "Updating store should succeed: " + response.getError());

      // Run batch push
      runVPJ(vpjProperties, 1, controllerClient);

      // Wait for version to become current
      TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        return currentVersion == 1;
      });

      // Verify batch data is readable
      verifyAllDataCanBeQueried(client, 1, batchRecordCount, VALUE_PREFIX);

      // Find the leader and verify DIV state on all servers
      HelixExternalViewRepository routingDataRepo = getRoutingDataRepository();
      Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, PARTITION);
      assertNotNull(leaderNode, "Leader should be assigned for partition " + PARTITION);

      // Verify DIV state: leader and followers should all have VT DIV state for batch data
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        venice.getVeniceServers().forEach(server -> {
          if (!server.isRunning()) {
            return;
          }
          StoreIngestionTask sit =
              server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
          assertNotNull(sit, "StoreIngestionTask should exist on server: " + server.getAddress());

          DataIntegrityValidator div = sit.getDataIntegrityValidator();
          assertNotNull(div, "DIV should be initialized on server: " + server.getAddress());

          boolean isLeader = server.getPort() == leaderNode.getPort();
          LOGGER.info(
              "Checking DIV state on {} ({}): hasVtDivState={}, hasGlobalRtDivState={}",
              server.getAddress(),
              isLeader ? "leader" : "follower",
              div.hasVtDivState(PARTITION),
              div.hasGlobalRtDivState(PARTITION));

          // For a batch-only store, VT DIV state MUST exist (VPJ producer states)
          assertTrue(
              div.hasVtDivState(PARTITION),
              "VT DIV state should exist on " + (isLeader ? "leader" : "follower") + " server: " + server.getAddress()
                  + " for batch-only store with Global RT DIV enabled");

          // For a batch-only store, there should be NO Global RT DIV state (no RT writers)
          assertFalse(
              div.hasGlobalRtDivState(PARTITION),
              "Global RT DIV state should NOT exist on " + (isLeader ? "leader" : "follower") + " server: "
                  + server.getAddress() + " for batch-only store (no RT data)");
        });
      });
    }
  }

  /**
   * Reproduces a bug where cloneVtProducerStates destructively evicts VT producer states from the consumer DIV
   * due to the wall-clock-based maxAge staleness check. When DIV_PRODUCER_STATE_MAX_AGE_MS is configured,
   * each sync (triggered by control messages or size threshold) runs:
   *   earliestAllowableTimestamp = System.currentTimeMillis() - maxAgeInMs
   * Any segment with lastRecordProducerTimestamp < earliestAllowableTimestamp is evicted from BOTH the clone
   * and the source vtSegments. For batch-only stores, once the last data record is consumed and a sync is
   * triggered (e.g., at EOP), the time gap between the last record's timestamp and the sync may exceed maxAge,
   * causing all entries to be evicted.
   *
   * This test uses a tiny maxAge (1ms) to reliably trigger this eviction, demonstrating the mechanism
   * that causes size=0 in the "event=globalRtDiv Syncing LCVP" log for batch-only stores in production.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testBatchOnlyStoreWithGlobalRtDivAndMaxAge() throws Exception {
    int PARTITION = 0;
    int batchRecordCount = 100;
    int partitionCount = 1;
    int serverCount = 2;

    // Create a dedicated cluster with DIV_PRODUCER_STATE_MAX_AGE_MS set to a tiny value.
    // Use a large sync bytes interval to prevent size-based syncs during data ingestion.
    // This ensures the ONLY sync trigger is the EOP control message, which guarantees a time gap
    // between the last data record and the sync — making the 1ms maxAge eviction reliable.
    Properties extraProps = createExtraProperties();
    extraProps.setProperty(DIV_PRODUCER_STATE_MAX_AGE_MS, "1"); // 1ms maxAge to force staleness eviction
    extraProps.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, "104857600"); // 100MB

    try (VeniceClusterWrapper maxAgeCluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(serverCount)
            .partitionSize(1000000)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(extraProps)
            .build())) {

      maxAgeCluster.addVeniceRouter(new Properties());
      Properties serverProperties = new Properties();
      serverProperties.setProperty(KAFKA_OVER_SSL, "false");
      for (int i = 0; i < serverCount; i++) {
        maxAgeCluster.addVeniceServer(serverProperties, extraProps);
      }

      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String storeName = Utils.getUniqueString("batchOnlyMaxAge");
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
      Properties vpjProperties = defaultVPJProps(maxAgeCluster, inputDirPath, storeName);

      try (
          ControllerClient controllerClient =
              createStoreForJob(maxAgeCluster.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(maxAgeCluster.getRandomRouterURL()))) {

        // Create a BATCH-ONLY store with Global RT DIV enabled (no hybrid config)
        UpdateStoreQueryParams updateParams =
            new UpdateStoreQueryParams().setGlobalRtDivEnabled(true).setPartitionCount(partitionCount);
        ControllerResponse response = controllerClient.updateStore(storeName, updateParams);
        assertFalse(response.isError(), "Updating store should succeed: " + response.getError());

        // Run batch push
        runVPJ(vpjProperties, 1, controllerClient);

        // Wait for version to become current
        TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
          int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
          return currentVersion == 1;
        });

        // Verify batch data is still readable (data integrity is not affected)
        verifyAllDataCanBeQueried(client, 1, batchRecordCount, VALUE_PREFIX);

        // Find the leader
        HelixExternalViewRepository routingDataRepo = maxAgeCluster.getLeaderVeniceController()
            .getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(maxAgeCluster.getClusterName())
            .getRoutingDataRepository();
        Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, PARTITION);
        assertNotNull(leaderNode, "Leader should be assigned for partition " + PARTITION);

        // With a 1ms maxAge, the cloneVtProducerStates staleness filter evicts VT producer states
        // during SOP-triggered sync (promotion delay > 1ms makes entries stale). Data records
        // may repopulate vtSegments, and the final EOP sync may or may not evict again (timing-dependent).
        // The key evidence is in server logs: "event=globalRtDiv Removed N stale VT producer state(s)"
        // and "event=globalRtDiv Syncing LCVP ... size: 0" — verifiable in test report HTML.
        maxAgeCluster.getVeniceServers().forEach(server -> {
          if (!server.isRunning()) {
            return;
          }
          StoreIngestionTask sit =
              server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
          if (sit == null) {
            return;
          }
          DataIntegrityValidator div = sit.getDataIntegrityValidator();
          if (div == null) {
            return;
          }
          boolean isLeader = server.getPort() == leaderNode.getPort();
          boolean hasVtState = div.hasVtDivState(PARTITION);
          boolean hasRtState = div.hasGlobalRtDivState(PARTITION);
          // Log the state for debugging. The actual bug evidence is in the server logs showing
          // "Removed N stale VT producer state(s)" during the SOP-triggered sync.
          LOGGER.info(
              "maxAge test: {} ({}) hasVtDivState={} hasGlobalRtDivState={}",
              server.getAddress(),
              isLeader ? "leader" : "follower",
              hasVtState,
              hasRtState);

          // For a batch-only store, there should be NO Global RT DIV state (no RT writers)
          assertFalse(
              hasRtState,
              "Global RT DIV state should NOT exist for batch-only store on " + server.getAddress());
        });
      }
    }
  }

  private static Properties createExtraProperties() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4");
    extraProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, "500");
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    extraProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    return extraProperties;
  }

  /**
   * This test verifies functionality of sending chunked/non-chunked div messages:
   *
   * 1. Create a hybrid store and create a store version.
   * 2. Send a non-chunked div message to the version topic.
   * 3. Send a chunked div message to the version topic.
   * 4. Verify the messages are sent successfully.
   * 5. TODO: Add more verification steps on the server side later.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testChunkedDiv() {
    String storeName = Utils.getUniqueString("store");
    final int partitionCount = 1;
    final int keyCount = 10;

    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
        .setHybridRewindSeconds(2000000)
        .setHybridOffsetLagThreshold(10)
        .setPartitionCount(partitionCount);

    venice.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });

    // Create store version 1 by writing keyCount records.
    Stream<Map.Entry> batchData = IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i));
    venice.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, batchData);

    Properties writerProperties = new Properties();
    writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());

    // Set max segment elapsed time to 0 to enforce creating small segments aggressively
    writerProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, "0");
    writerProperties.putAll(
        PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testGlobalRtDiv() throws Exception {
    int PARTITION = 0;
    int NUM_WRITERS = 2;
    int MESSAGE_COUNT = 100;
    int perWriterMessageCount = MESSAGE_COUNT / NUM_WRITERS;
    boolean isChunkingEnabled = false; // TODO: test with chunking
    String VALUE_PREFIX = "testGlobalRtDiv_";
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testGlobalRtDiv");
    String topicName = Version.composeKafkaTopic(storeName, 1);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
    PubSubBrokerWrapper brokerWrapper = venice.getPubSubBrokerWrapper();
    String globalRtDivKey =
        LeaderFollowerStoreIngestionTask.getGlobalRtDivKeyName(PARTITION, brokerWrapper.getAddress());
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    Properties writerProperties = new Properties();
    writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, brokerWrapper.getAddress());
    writerProperties.put(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "100"); // force chunking
    AvroSerializer<String> stringSerializer = new AvroSerializer<>(STRING_SCHEMA);
    PubSubProducerAdapterFactory producerFactory = brokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory writerFactory = TestUtils
        .getVeniceWriterFactory(writerProperties, producerFactory, brokerWrapper.getPubSubPositionTypeRegistry());

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      // All keys are produced to the same partition, since there is only 1 partition
      UpdateStoreQueryParams updateParams = createUpdateParams(isChunkingEnabled, PARTITION + 1);
      ControllerResponse response = controllerClient.updateStore(storeName, updateParams);
      assertFalse(response.isError(), "Updating store should succeed");
      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();

      runVPJ(vpjProperties, 1, controllerClient); // do a batch push

      for (int i = 0; i < NUM_WRITERS; i++) { // chunk the nearline data into multiple parts sent by different producers
        VeniceWriterOptions options = new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build();
        try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter = writerFactory.createVeniceWriter(options)) {
          for (int j = i * perWriterMessageCount + 1; j <= i * perWriterMessageCount + perWriterMessageCount; j++) {
            realTimeTopicWriter
                .put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize(VALUE_PREFIX + j), 1);
          }
        }
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
          String key = Integer.toString(i);
          Object value = client.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
          assertEquals(value.toString(), VALUE_PREFIX + key);
        }
      });
    }

    // Sanity check that Global RT DIV State can be successfully loaded from StorageEngine on all servers
    venice.getVeniceServers().forEach(server -> {
      TestVeniceServer testVeniceServer = server.getVeniceServer();
      StorageEngine storageEngine = testVeniceServer.getStorageService().getStorageEngine(topicName);
      if (storageEngine == null) {
        return;
      }
      GlobalRtDivState globalRtDiv = getGlobalRtDivState(testVeniceServer, topicName, PARTITION, globalRtDivKey);
      LOGGER.info("Global RT DIV State: {}", globalRtDiv);
      validateGlobalDivState(globalRtDiv);
    });

    /*
    * Restart leader server (server1) to trigger leadership handover during batch consumption.
    * When server1 stops, server2 will be promoted to leader. When server1 starts, due to full-auto rebalance,
    server2:
    * 1) Will be demoted to follower. Leader->standby transition during remote consumption will be tested.
    * 2) Or remain as leader. In this case, Leader->standby transition during remote consumption won't be tested.
    * TODO: Use semi-auto rebalance and assign a server as the leader to make sure leader->standby always happen.
    */
    HelixExternalViewRepository routingDataRepo = getRoutingDataRepository();
    assertTrue(routingDataRepo.containsKafkaTopic(topicName), topicName + " should exist");
    Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, PARTITION);
    assertNotNull(leaderNode);

    // Before shutting down the leader, verify that the leader has the Global RT DIV State, but not the followers.
    Instance oldLeaderNode = verifyGlobalDivStateOnAllServers(topicName, PARTITION);

    // Shutdown leader to trigger a FOLLOWER -> LEADER transition.
    LOGGER.info("Stopping leader server: {}", leaderNode.getNodeId());
    venice.stopVeniceServer(leaderNode.getPort());

    // Wait for a new leader (different from the stopped one) to be elected.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Instance leader = routingDataRepo.getLeaderInstance(topicName, PARTITION);
      assertNotNull(leader, "New leader should be elected");
      assertNotEquals(leader.getNodeId(), oldLeaderNode.getNodeId(), "New leader should be different from old leader");
    });

    // Verify that the other server is promoted to leader and load the Global RT DIV State correctly.
    Instance newLeader = verifyGlobalDivStateOnAllServers(topicName, PARTITION);
    LOGGER.info("New leader server: {}", newLeader.getNodeId());

    // Restart the old leader server to test if it can load the Global RT DIV State correctly as a follower.
    LOGGER.info("Restarting old leader server: {}", oldLeaderNode.getNodeId());
    venice.restartVeniceServer(oldLeaderNode.getPort());

    // Wait oldLeaderNode to be the leader again after restart.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Instance leader = routingDataRepo.getLeaderInstance(topicName, 0);
      if (leader == null) {
        throw new VeniceException("Leader not found yet");
      }
      assertEquals(leader.getNodeId(), oldLeaderNode.getNodeId());
    });

    // Verify the state transition LEADER -> FOLLOWER happened on the old leader node.
    LOGGER.info("Old leader server: {} is now the current leader", oldLeaderNode.getNodeId());
    Instance curLeader = verifyGlobalDivStateOnAllServers(topicName, PARTITION);
    assertEquals(curLeader.getNodeId(), oldLeaderNode.getNodeId());
  }

  Instance verifyGlobalDivStateOnAllServers(String topicName, int partition) {
    // Verify that the other server is promoted to leader and load the Global RT DIV State correctly.
    HelixExternalViewRepository routingDataRepo = getRoutingDataRepository();
    AtomicReference<Instance> LeaderNode = new AtomicReference<>();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      LeaderNode.set(routingDataRepo.getLeaderInstance(topicName, partition)); // Find the leader node
      LOGGER.info("Leader server: {}", LeaderNode.get().getNodeId());
      venice.getVeniceServers().forEach(server -> {
        if (LeaderNode.get() == null) {
          throw new VeniceException("Leader not found yet");
        }
        if (!server.isRunning()) {
          LOGGER.info("Server: {} is not running", server.getVeniceServer());
          return;
        }
        boolean isLeader = server.getPort() == LeaderNode.get().getPort();
        verifyGlobalDivState(server, topicName, partition, isLeader);
      });
    });
    return LeaderNode.get();
  }

  private void verifyGlobalDivState(VeniceServerWrapper server, String topicName, int partition, boolean isLeader) {
    StoreIngestionTask sit = server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
    DataIntegrityValidator consumerDiv = sit.getDataIntegrityValidator();
    if (consumerDiv == null) {
      throw new VeniceException("consumerDiv on server: " + server.getAddress() + " is not initialized yet");
    }
    if (isLeader) {
      LOGGER.info("Verifying Global RT DIV State on leader: {}", server.getAddress());
      assertTrue(consumerDiv.hasGlobalRtDivState(partition));
      assertTrue(consumerDiv.hasVtDivState(partition));
    } else {
      LOGGER.info("Verifying Global RT DIV State on follower: {}", server.getAddress());
      assertFalse(consumerDiv.hasGlobalRtDivState(partition));
      assertTrue(consumerDiv.hasVtDivState(partition));
    }
  }

  private void validateGlobalDivState(GlobalRtDivState state) {
    assertNotNull(state);
    assertNotNull(state.getSrcUrl());
    assertNotNull(state.getProducerStates());
    assertFalse(state.getProducerStates().isEmpty());
    state.getProducerStates().forEach((producerId, producerState) -> {
      assertNotNull(producerId);
      assertNotNull(producerState);
      assertTrue(producerState.getSegmentNumber() >= 0, "Segment number should be non-negative");
      assertTrue(producerState.getMessageSequenceNumber() >= 0, "Message sequence number should be non-negative");
      assertTrue(producerState.getMessageTimestamp() >= 0, "Message timestamp should be non-negative");
      CheckSumType.valueOf(producerState.getChecksumType()); // throws VeniceMessageException if invalid
      assertNotNull(producerState.getChecksumState(), "Checksum state should not be null");
      assertNotNull(producerState.getAggregates(), "Aggregates should not be null");
      assertNotNull(producerState.getDebugInfo(), "Debug info should not be null");
    });
  }

  private static GlobalRtDivState getGlobalRtDivState(
      TestVeniceServer testVeniceServer,
      String topicName,
      int partition,
      String globalRtDivKey) {
    InternalAvroSpecificSerializer<GlobalRtDivState> globalRtDivStateSerializer =
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getSerializer();
    int schemaVersion = AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion();

    StorageEngine storageEngine = testVeniceServer.getStorageService().getStorageEngine(topicName);
    assertNotNull(storageEngine, "Storage engine should exist for topic: " + topicName);
    byte[] keyBytes = globalRtDivKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    final java.util.function.BiFunction<Integer, ByteBuffer, byte[]> getter =
        (part, keyBuf) -> storageEngine.getGlobalRtDivMetadata(ByteUtils.extractByteArray(keyBuf));
    ByteBuffer assembledBytes = RawBytesChunkingAdapter.INSTANCE.get(
        getter,
        storageEngine.getStoreVersionName(),
        partition,
        ByteBuffer.wrap(keyBytes),
        true,
        null,
        null,
        NoOpReadResponseStats.SINGLETON,
        schemaVersion,
        RawBytesStoreDeserializerCache.getInstance(),
        new NoopCompressor(),
        null);
    if (assembledBytes == null) {
      return null;
    }
    return globalRtDivStateSerializer.deserialize(ByteUtils.extractByteArray(assembledBytes), schemaVersion);
  }

  private static UpdateStoreQueryParams createUpdateParams(boolean isChunkingEnabled, int partitionCount) {
    return new UpdateStoreQueryParams().setGlobalRtDivEnabled(true)
        .setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L)
        .setChunkingEnabled(isChunkingEnabled)
        .setCompressionStrategy(CompressionStrategy.NO_OP)
        .setPartitionCount(partitionCount);
  }

  private HelixExternalViewRepository getRoutingDataRepository() {
    return venice.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(venice.getClusterName())
        .getRoutingDataRepository();
  }

  /**
   * Helper method to verify that all data can be queried from the store (no data loss).
   */
  private void verifyAllDataCanBeQueried(
      AvroGenericStoreClient<Object, Object> client,
      int startKey,
      int endKey,
      String valuePrefix) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      for (int i = startKey; i <= endKey; i++) {
        String key = Integer.toString(i);
        Object value = client.get(key).get();
        assertNotNull(value, "Key " + i + " should not be missing! Data loss detected.");
        assertEquals(value.toString(), valuePrefix + key, "Value mismatch for key " + i);
      }
    });
  }

  /**
   * Helper method to write RT data to a hybrid store.
   */
  private void writeRTData(
      String topicName,
      int startKey,
      int endKey,
      String valuePrefix,
      VeniceWriterFactory writerFactory) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).build();
    AvroSerializer<String> stringSerializer = new AvroSerializer<>(STRING_SCHEMA);
    try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter = writerFactory.createVeniceWriter(options)) {
      for (int j = startKey; j <= endKey; j++) {
        realTimeTopicWriter
            .put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize(valuePrefix + j), 1);
      }
    }
  }

  /**
   * Verifies that GlobalRtDivState is correctly stored when the serialized state is large enough to be split
   * into multiple chunks by the server's VeniceWriter.
   *
   * <p>This test uses a dedicated cluster with a very small {@code MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   * (200 bytes) on the server so that the GlobalRtDivState, which accumulates one {@code ProducerPartitionState}
   * entry per unique RT writer, is chunked when the leader produces it to the version topic.  With
   * {@code NUM_WRITERS} independent VeniceWriters each contributing a distinct ProducerGUID, the serialized
   * state easily exceeds the 200-byte limit and is split across several chunk messages followed by a manifest.
   *
   * <p>After ingestion completes, the test verifies on every server that:
   * <ol>
   *   <li>The GlobalRtDivState was reassembled correctly from the chunks stored in the metadata partition.
   *   <li>The reassembled state is fully populated (non-null, non-empty producerStates map, etc.).
   * </ol>
   */
  @Test(timeOut = 360 * Time.MS_PER_SECOND)
  public void testChunkedGlobalRtDiv() throws Exception {
    int PARTITION = 0;
    int NUM_WRITERS = 5; // More writers → larger ProducerPartitionState map → more chunks
    int MESSAGE_COUNT = 100;
    int serverCount = 2;

    // Use a very small max message size so the server's VeniceWriter chunks the GlobalRtDivState.
    // A GlobalRtDivState with 5+ producers serializes to ~1 KB, well above the 200-byte limit.
    Properties chunkingExtraProperties = createExtraProperties();
    chunkingExtraProperties.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "200");

    try (VeniceClusterWrapper chunkingVenice = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(serverCount)
            .partitionSize(1000000)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(chunkingExtraProperties)
            .build())) {

      chunkingVenice.addVeniceRouter(new Properties());

      Properties serverProperties = new Properties();
      serverProperties.setProperty(KAFKA_OVER_SSL, "false");
      for (int i = 0; i < serverCount; i++) {
        chunkingVenice.addVeniceServer(serverProperties, chunkingExtraProperties);
      }

      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String storeName = Utils.getUniqueString("testChunkedGlobalRtDiv");
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);

      PubSubBrokerWrapper brokerWrapper = chunkingVenice.getPubSubBrokerWrapper();
      String brokerUrl = brokerWrapper.getAddress();
      String globalRtDivKey = LeaderFollowerStoreIngestionTask.getGlobalRtDivKeyName(PARTITION, brokerUrl);

      Properties vpjProperties = defaultVPJProps(chunkingVenice, inputDirPath, storeName);
      Properties writerProperties = new Properties();
      writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, brokerUrl);
      writerProperties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(brokerWrapper)));

      PubSubProducerAdapterFactory producerFactory =
          brokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
      VeniceWriterFactory writerFactory = TestUtils
          .getVeniceWriterFactory(writerProperties, producerFactory, brokerWrapper.getPubSubPositionTypeRegistry());
      AvroSerializer<String> stringSerializer = new AvroSerializer<>(STRING_SCHEMA);

      try (
          ControllerClient controllerClient =
              createStoreForJob(chunkingVenice.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(chunkingVenice.getRandomRouterURL()))) {

        // Enable both chunking and GlobalRtDiv on the store.
        // Chunking is required so the VeniceWriter is allowed to split large messages.
        UpdateStoreQueryParams updateParams = createUpdateParams(true /* isChunkingEnabled */, PARTITION + 1);
        ControllerResponse response = controllerClient.updateStore(storeName, updateParams);
        assertFalse(response.isError(), "Updating store should succeed");
        StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();

        runVPJ(vpjProperties, 1, controllerClient);

        // Write RT data using NUM_WRITERS independent VeniceWriters.
        // Each writer has a unique ProducerGUID, so the GlobalRtDivState's producerStates map grows
        // with each writer, making the total serialized size large enough to require chunking.
        int perWriterCount = MESSAGE_COUNT / NUM_WRITERS;
        for (int i = 0; i < NUM_WRITERS; i++) {
          VeniceWriterOptions options = new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build();
          try (VeniceWriter<byte[], byte[], byte[]> rtWriter = writerFactory.createVeniceWriter(options)) {
            int start = i * perWriterCount + 1;
            int end = start + perWriterCount - 1;
            for (int j = start; j <= end; j++) {
              rtWriter.put(
                  stringSerializer.serialize(String.valueOf(j)),
                  stringSerializer.serialize("chunked_value_" + j),
                  1);
            }
          }
        }

        // Wait for all RT data to be readable, confirming ingestion (including GlobalRtDiv assembly) finished.
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 1; i <= MESSAGE_COUNT; i++) {
            Object value = client.get(String.valueOf(i)).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), "chunked_value_" + i);
          }
        });
      }

      // Verify on every server that the GlobalRtDivState is correctly accessible — either via the non-chunked
      // metadata path or by assembling it from the chunks stored in the metadata partition at read time.
      chunkingVenice.getVeniceServers().forEach(server -> {
        TestVeniceServer testVeniceServer = server.getVeniceServer();
        StorageEngine storageEngine = testVeniceServer.getStorageService().getStorageEngine(topicName);
        if (storageEngine == null) {
          return;
        }
        GlobalRtDivState globalRtDiv = getGlobalRtDivState(testVeniceServer, topicName, PARTITION, globalRtDivKey);
        LOGGER.info("Chunked Global RT DIV State (assembled from chunks): {}", globalRtDiv);
        validateGlobalDivState(globalRtDiv);
      });
    }
  }

  /**
   * Verifies that the "last GlobalRtDiv write wins" when both non-chunked and chunked messages
   * are produced to the version topic for the same key.
   *
   * <p>Because {@code putGlobalRtDivStateInMetadata} stores every message directly in the metadata
   * partition under its serialized key, both a non-chunked write (positive schema ID at the manifest
   * key) and a chunked write (CHUNK_MANIFEST_SCHEMA_ID at the same manifest key plus chunk keys)
   * overwrite whatever was previously stored at that manifest key. The read path uses
   * {@link GenericChunkingAdapter} with {@code isChunked=true}: it reads the manifest key, dispatches
   * on the stored schema ID, and either returns the value directly (non-chunked) or assembles from
   * chunk keys (chunked). Stale chunk keys from a superseded chunked write are orphaned but harmless.
   *
   * <p>The test exercises the <em>non-chunked → chunked</em> direction:
   * <ol>
   *   <li><b>Phase 1</b> — one RT writer → small GlobalRtDivState (~200 bytes) → fits in a single
   *       message (non-chunked). The manifest key is written with a positive schema ID.
   *   <li><b>Phase 2</b> — five additional RT writers → the state grows to ~1 KB, exceeding the
   *       400-byte {@code MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES} on the server's
   *       VeniceWriter → chunked. The manifest key is overwritten with the chunk manifest.
   *   <li><b>Assertion</b> — the final state read via {@link GenericChunkingAdapter} contains more
   *       producers than the phase-1 snapshot, proving the phase-2 (chunked) write won.
   * </ol>
   *
   * <p>The <em>chunked → non-chunked</em> direction is covered by the same write-path logic: a
   * non-chunked message always overwrites the manifest key with a positive schema ID, causing
   * {@link GenericChunkingAdapter} to return the new value directly and ignore any orphaned chunk keys.
   */
  @Test(timeOut = 360 * Time.MS_PER_SECOND)
  public void testChunkedAndNonChunkedDivLastOneWins() throws Exception {
    int PARTITION = 0;
    int NUM_WRITERS_PHASE_2 = 5;
    int MESSAGES_PER_WRITER = 50;
    int serverCount = 2;

    // With MAX_SIZE_FOR_USER_PAYLOAD = 400 bytes:
    // Phase 1 (1 writer, state ~200 bytes) → non-chunked (fits in a single message)
    // Phase 2 (6 total writers, state ~1 KB) → chunked (split across multiple messages)
    Properties extraProps = createExtraProperties();
    extraProps.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "400");

    try (VeniceClusterWrapper chunkingVenice = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(serverCount)
            .partitionSize(1000000)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(extraProps)
            .build())) {

      chunkingVenice.addVeniceRouter(new Properties());
      Properties serverProperties = new Properties();
      serverProperties.setProperty(KAFKA_OVER_SSL, "false");
      for (int i = 0; i < serverCount; i++) {
        chunkingVenice.addVeniceServer(serverProperties, extraProps);
      }

      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String storeName = Utils.getUniqueString("testLastOneWins");
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);

      PubSubBrokerWrapper brokerWrapper = chunkingVenice.getPubSubBrokerWrapper();
      String brokerUrl = brokerWrapper.getAddress();
      String globalRtDivKey = LeaderFollowerStoreIngestionTask.getGlobalRtDivKeyName(PARTITION, brokerUrl);

      Properties vpjProperties = defaultVPJProps(chunkingVenice, inputDirPath, storeName);
      Properties writerProperties = new Properties();
      writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, brokerUrl);
      writerProperties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(brokerWrapper)));

      PubSubProducerAdapterFactory producerFactory =
          brokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
      VeniceWriterFactory writerFactory = TestUtils
          .getVeniceWriterFactory(writerProperties, producerFactory, brokerWrapper.getPubSubPositionTypeRegistry());
      AvroSerializer<String> stringSerializer = new AvroSerializer<>(STRING_SCHEMA);

      HelixExternalViewRepository routingDataRepo = chunkingVenice.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixVeniceClusterResources(chunkingVenice.getClusterName())
          .getRoutingDataRepository();

      try (
          ControllerClient controllerClient =
              createStoreForJob(chunkingVenice.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(chunkingVenice.getRandomRouterURL()))) {

        UpdateStoreQueryParams updateParams = createUpdateParams(true /* isChunkingEnabled */, PARTITION + 1);
        assertFalse(controllerClient.updateStore(storeName, updateParams).isError());
        StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
        String rtTopic = Utils.getRealTimeTopicName(storeInfo);

        runVPJ(vpjProperties, 1, controllerClient);

        // ---- Phase 1: one writer → non-chunked GlobalRtDiv ----
        VeniceWriterOptions rtOptions = new VeniceWriterOptions.Builder(rtTopic).build();
        try (VeniceWriter<byte[], byte[], byte[]> writer = writerFactory.createVeniceWriter(rtOptions)) {
          for (int j = 1; j <= MESSAGES_PER_WRITER; j++) {
            writer.put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize("phase1_" + j), 1);
          }
        }

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 1; i <= MESSAGES_PER_WRITER; i++) {
            assertEquals(client.get(String.valueOf(i)).get().toString(), "phase1_" + i);
          }
        });

        // Wait for the phase-1 GlobalRtDiv to be committed to the metadata partition.
        AtomicReference<GlobalRtDivState> phase1State = new AtomicReference<>();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Instance leader = routingDataRepo.getLeaderInstance(topicName, PARTITION);
          assertNotNull(leader, "Leader should be elected");
          VeniceServerWrapper leaderServer = chunkingVenice.getVeniceServers()
              .stream()
              .filter(s -> s.isRunning() && s.getPort() == leader.getPort())
              .findFirst()
              .orElseThrow(() -> new VeniceException("Leader server not found"));
          GlobalRtDivState state =
              getGlobalRtDivState(leaderServer.getVeniceServer(), topicName, PARTITION, globalRtDivKey);
          assertNotNull(state, "Phase-1 GlobalRtDiv state should be persisted");
          phase1State.set(state);
        });

        int phase1ProducerCount = phase1State.get().getProducerStates().size();
        LOGGER.info("Phase 1 GlobalRtDiv producer count: {}", phase1ProducerCount);

        // ---- Phase 2: five more writers → chunked GlobalRtDiv ----
        for (int i = 0; i < NUM_WRITERS_PHASE_2; i++) {
          try (VeniceWriter<byte[], byte[], byte[]> writer = writerFactory.createVeniceWriter(rtOptions)) {
            int start = MESSAGES_PER_WRITER + i * MESSAGES_PER_WRITER + 1;
            int end = start + MESSAGES_PER_WRITER - 1;
            for (int j = start; j <= end; j++) {
              writer.put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize("phase2_" + j), 1);
            }
          }
        }

        int totalMessages = MESSAGES_PER_WRITER * (1 + NUM_WRITERS_PHASE_2);
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (int i = MESSAGES_PER_WRITER + 1; i <= totalMessages; i++) {
            assertNotNull(client.get(String.valueOf(i)).get(), "Key " + i + " should not be missing");
          }
        });

        // The phase-2 (chunked) GlobalRtDiv should have overwritten the phase-1 (non-chunked) entry.
        // The final state must contain producers from all writers, not just the phase-1 writer.
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Instance leader = routingDataRepo.getLeaderInstance(topicName, PARTITION);
          assertNotNull(leader, "Leader should be elected");
          VeniceServerWrapper leaderServer = chunkingVenice.getVeniceServers()
              .stream()
              .filter(s -> s.isRunning() && s.getPort() == leader.getPort())
              .findFirst()
              .orElseThrow(() -> new VeniceException("Leader server not found"));
          GlobalRtDivState finalState =
              getGlobalRtDivState(leaderServer.getVeniceServer(), topicName, PARTITION, globalRtDivKey);
          assertNotNull(finalState, "Final GlobalRtDiv state should be present");
          assertTrue(
              finalState.getProducerStates().size() > phase1ProducerCount,
              "Phase-2 (chunked) GlobalRtDiv should have overwritten phase-1 (non-chunked): " + "expected more than "
                  + phase1ProducerCount + " producers but got " + finalState.getProducerStates().size());
          validateGlobalDivState(finalState);
        });
      }
    }
  }

  /**
   * Reproduces a bug where a batch-only store with Global RT DIV enabled and native replication (NR)
   * ends up with empty vtSegments (size: 0) in the consumer DIV.
   *
   * Root cause: When NR is enabled, the leader in the remote fabric (dc-1) consumes from the source
   * fabric's (dc-0) VT. This sets {@code consumeRemotely = true} on the PCS. In
   * {@code validateAndFilterOutDuplicateMessagesFromLeaderTopic}, the topicType selection is:
   * <pre>
   *   if (isGlobalRtDivEnabled() && shouldProduceToVersionTopic(pcs)) {
   *       topicType = TopicType.of(REALTIME_TOPIC_TYPE, kafkaUrl);
   *   }
   * </pre>
   * And {@code shouldProduceToVersionTopic} returns:
   * <pre>
   *   return (!versionTopic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely());
   * </pre>
   * For NR batch-only: {@code leaderTopic == versionTopic} (set during promotion), but
   * {@code consumeRemotely() == true}. So it returns {@code true}, routing VT messages to
   * {@code REALTIME_TOPIC_TYPE} → entries go into rtSegments instead of vtSegments.
   * When {@code cloneVtProducerStates} runs, vtSegments is empty → size: 0.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testBatchOnlyNRStoreWithGlobalRtDivHasEmptyVtSegments() throws Exception {
    int PARTITION = 0;
    int recordCount = 100;
    int partitionCount = 1;
    String clusterName = "venice-cluster0";

    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, "500");
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 4);

    try (VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegion =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(2)
                .numberOfClusters(1)
                .numberOfParentControllers(1)
                .numberOfChildControllers(1)
                .numberOfServers(2)
                .numberOfRouters(1)
                .replicationFactor(2)
                .serverProperties(serverProperties)
                .childControllerProperties(controllerProps)
                .parentControllerProperties(controllerProps)
                .build())) {

      List<VeniceMultiClusterWrapper> childDatacenters = multiRegion.getChildRegions();
      VeniceControllerWrapper parentController = multiRegion.getParentControllers().get(0);

      File inputDir = getTempDataDirectory();
      String inputDirPath = "file:" + inputDir.getAbsolutePath();
      String storeName = Utils.getUniqueString("batchOnlyNrGlobalRtDiv");
      String topicName = Version.composeKafkaTopic(storeName, 1);

      Properties vpjProps = IntegrationTestPushUtils.defaultVPJProps(multiRegion, inputDirPath, storeName);
      vpjProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount);
      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setGlobalRtDivEnabled(true)
              .setPartitionCount(partitionCount);

      try (ControllerClient parentControllerClient =
          createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, vpjProps, updateStoreParams)) {

        // Verify store config propagated to child DCs
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (VeniceMultiClusterWrapper dc: childDatacenters) {
            dc.getClusters().get(clusterName).useControllerClient(cc -> {
              ControllerResponse resp = cc.getStore(storeName);
              assertFalse(resp.isError(), "Failed to get store: " + resp.getError());
            });
          }
        });

        // Run VPJ batch push
        try (VenicePushJob job = new VenicePushJob("Test push job", vpjProps)) {
          job.run();
          LOGGER.info("Push destination: {}", job.getPushDestinationPubsubBroker());
        }

        // Wait for version to become current in all regions
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          for (int version: parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
            assertTrue(version == 1, "Version should be 1, got: " + version);
          }
        });

        // Check DIV state on dc-1 (the remote consumer). dc-1's leader consumes from dc-0's VT (remote).
        // This is where consumeRemotely() = true and the bug manifests.
        VeniceClusterWrapper dc1Cluster = childDatacenters.get(1).getClusters().get(clusterName);

        HelixExternalViewRepository routingDataRepo = dc1Cluster.getLeaderVeniceController()
            .getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getRoutingDataRepository();

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Instance leaderNode = routingDataRepo.getLeaderInstance(topicName, PARTITION);
          assertNotNull(leaderNode, "Leader should be assigned in dc-1 for partition " + PARTITION);

          dc1Cluster.getVeniceServers().forEach(server -> {
            if (!server.isRunning()) {
              return;
            }
            StoreIngestionTask sit =
                server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
            assertNotNull(sit, "StoreIngestionTask should exist on server: " + server.getAddress());

            DataIntegrityValidator div = sit.getDataIntegrityValidator();
            assertNotNull(div, "DIV should be initialized on server: " + server.getAddress());

            boolean isLeader = server.getPort() == leaderNode.getPort();
            boolean hasVtState = div.hasVtDivState(PARTITION);
            boolean hasRtState = div.hasGlobalRtDivState(PARTITION);

            LOGGER.info(
                "dc-1 server {} ({}): hasVtDivState={}, hasGlobalRtDivState={}",
                server.getAddress(),
                isLeader ? "leader" : "follower",
                hasVtState,
                hasRtState);

            // BUG REPRODUCTION: On the leader in dc-1, consumeRemotely() = true causes
            // shouldProduceToVersionTopic() = true, which routes VT message validation to
            // REALTIME_TOPIC_TYPE instead of VERSION_TOPIC. VT producer states end up in
            // rtSegments instead of vtSegments. cloneVtProducerStates returns size: 0.
            if (isLeader) {
              assertFalse(
                  hasVtState,
                  "BUG REPRODUCTION: VT DIV state should be empty on dc-1 leader (consuming remotely) "
                      + "due to topicType misrouting caused by consumeRemotely()=true. " + "Server: "
                      + server.getAddress());
            }
          });
        });
      }
    }
  }
}
