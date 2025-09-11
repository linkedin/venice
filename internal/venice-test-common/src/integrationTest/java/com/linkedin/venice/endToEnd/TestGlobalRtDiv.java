package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
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
import org.testng.annotations.Test;


public class TestGlobalRtDiv {
  private static final Logger LOGGER = LogManager.getLogger(TestGlobalRtDiv.class);

  private VeniceClusterWrapper venice;

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
    String globalRtDivKey = "GLOBAL_RT_DIV_KEY." + brokerWrapper.getAddress();
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
      GlobalRtDivState globalRtDiv =
          getGlobalRtDivState(testVeniceServer, storageEngine, PARTITION, globalRtDivKey, isChunkingEnabled);
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

    // Verify that the other server is promoted to leader and load the Global RT DIV State correctly.
    Instance newLeader = verifyGlobalDivStateOnAllServers(topicName, PARTITION);
    LOGGER.info("New leader server: {}", newLeader.getNodeId());
    // Confirm that leader has changed.
    assertNotEquals(newLeader.getNodeId(), oldLeaderNode.getNodeId());

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
      assertTrue(producerState.getChecksumType() >= 0 && producerState.getChecksumType() <= 3, "Checksum validity");
      assertNotNull(producerState.getChecksumState(), "Checksum state should not be null");
      assertNotNull(producerState.getAggregates(), "Aggregates should not be null");
      assertNotNull(producerState.getDebugInfo(), "Debug info should not be null");
    });
  }

  private static GlobalRtDivState getGlobalRtDivState(
      TestVeniceServer testVeniceServer,
      StorageEngine storageEngine,
      int PARTITION,
      String globalRtDivKey,
      boolean isChunkingEnabled) {
    byte[] keyBytes = globalRtDivKey.getBytes();
    if (isChunkingEnabled) {
      keyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(keyBytes);
    }

    StorageEngineBackedCompressorFactory compressorFactory =
        new StorageEngineBackedCompressorFactory(testVeniceServer.getStorageMetadataService());
    VeniceCompressor compressor = compressorFactory
        .getCompressor(CompressionStrategy.NO_OP, storageEngine.getStoreVersionName(), Zstd.defaultCompressionLevel());

    // TODO: unify the production code with this
    ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
    ByteBuffer value = (ByteBuffer) GenericChunkingAdapter.INSTANCE.get(
        storageEngine,
        PARTITION,
        ByteBuffer.wrap(keyBytes),
        isChunkingEnabled,
        null,
        null,
        NoOpReadResponseStats.SINGLETON,
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion(),
        RawBytesStoreDeserializerCache.getInstance(),
        compressor,
        manifestContainer);

    InternalAvroSpecificSerializer<GlobalRtDivState> globalRtDivStateSerializer =
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getSerializer();

    GlobalRtDivState globalRtDiv = globalRtDivStateSerializer.deserialize(
        ByteUtils.extractByteArray(value),
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion());
    return globalRtDiv;
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
}
