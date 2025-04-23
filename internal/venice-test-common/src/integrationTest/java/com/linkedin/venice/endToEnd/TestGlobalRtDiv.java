package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.*;
import static com.linkedin.venice.utils.TestWriteUtils.*;
import static org.testng.Assert.*;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
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
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
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
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestGlobalRtDiv {
  private static final Logger LOGGER = LogManager.getLogger(TestGlobalRtDiv.class);

  private VeniceClusterWrapper sharedVenice;

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    extraProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, "500");
    // extraProperties.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "400");

    // N.B.: RF 2 with 3 servers is important, in order to test both the leader and follower code paths
    sharedVenice = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(2)
            .partitionSize(1000000)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(extraProperties)
            .build());

    Properties routerProperties = new Properties();

    sharedVenice.addVeniceRouter(routerProperties);
    // Added a server with shared consumer enabled.
    Properties serverPropertiesWithSharedConsumer = new Properties();
    serverPropertiesWithSharedConsumer.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    // extraProperties.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "400");
    extraProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4");
    extraProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    // Enable global div feature in the integration test.
    // extraProperties.setProperty(SERVER_GLOBAL_RT_DIV_ENABLED, "true");

    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    // sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    LOGGER.info("Finished creating VeniceClusterWrapper");
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
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

    sharedVenice.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });

    // Create store version 1 by writing keyCount records.
    sharedVenice.createVersion(
        storeName,
        DEFAULT_KEY_SCHEMA,
        DEFAULT_VALUE_SCHEMA,
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));

    Properties writerProperties = new Properties();
    writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, sharedVenice.getPubSubBrokerWrapper().getAddress());

    // Set max segment elapsed time to 0 to enforce creating small segments aggressively
    writerProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, "0");
    writerProperties.putAll(
        PubSubBrokerWrapper
            .getBrokerDetailsForClients(Collections.singletonList(sharedVenice.getPubSubBrokerWrapper())));

    // TODO: integration test
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testGlobalRtDiv() throws Exception {
    VeniceClusterWrapper venice = sharedVenice;
    LOGGER.info("Finished creating VeniceClusterWrapper");
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;
    int numWriters = 2;
    int messageCount = 100;
    int partitionCount = 1;
    int partition = partitionCount - 1;
    int perWriterMessageCount = messageCount / numWriters;
    boolean isChunkingEnabled = false;
    String storeName = Utils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      // Have 1 partition only, so that all keys are produced to the same partition
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setGlobalRtDivEnabled(true)
              .setChunkingEnabled(isChunkingEnabled)
              // .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setPartitionCount(partitionCount));
      Assert.assertFalse(response.isError());
      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);
      Properties writerProperties = new Properties();
      writerProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());
      writerProperties.put(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "100"); // force chunking
      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      String prefix = "testGlobalRtDiv_";
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          venice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();

      String resourceName = Version.composeKafkaTopic(storeName, 1);
      PubSubBrokerWrapper pubSubBrokerWrapper = venice.getPubSubBrokerWrapper();
      // chunk the data into 2 parts and send each part by different producers. Also, close the producers
      // as soon as it finishes writing. This makes sure that closing or switching producers won't impact the ingestion
      for (int i = 0; i < numWriters; i++) {
        try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter = TestUtils
            .getVeniceWriterFactory(
                writerProperties,
                pubSubProducerAdapterFactory,
                pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
            .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
          for (int j = i * perWriterMessageCount + 1; j <= i * perWriterMessageCount + perWriterMessageCount; j++) {
            realTimeTopicWriter
                .put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize(prefix + j), 1);
          }
        }
      }

      // Check both leader and follower hosts
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        for (VeniceServerWrapper server: venice.getVeniceServers()) {
          try {
            for (int i = 1; i <= messageCount; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              assertNotNull(value, "Key " + i + " should not be missing!");
              assertEquals(value.toString(), prefix + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        }
      });

      // /**
      // * Restart leader SN (server1) to trigger leadership handover during batch consumption.
      // * When server1 stops, server2 will be promoted to leader. When server1 starts, due to full-auto rebalance,
      // server2:
      // * 1) Will be demoted to follower. Leader->standby transition during remote consumption will be tested.
      // * 2) Or remain as leader. In this case, Leader->standby transition during remote consumption won't be tested.
      // * TODO: Use semi-auto rebalance and assign a server as the leader to make sure leader->standby always happen.
      // */
      HelixExternalViewRepository routingDataRepo = sharedVenice.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixVeniceClusterResources(sharedVenice.getClusterName())
          .getRoutingDataRepository();
      Assert.assertTrue(routingDataRepo.containsKafkaTopic(resourceName), "Topic " + resourceName + " should exist");
      List<VeniceServerWrapper> servers = sharedVenice.getVeniceServers();
      servers.forEach(server -> {
        // server.getVeniceServer().getStorageService().get;
        AbstractStorageEngine storageEngine =
            server.getVeniceServer().getStorageService().getStorageEngine(resourceName);
        String key = "GLOBAL_RT_DIV_KEY." + venice.getPubSubBrokerWrapper().getAddress();
        byte[] keyBytes = key.getBytes();
        if (isChunkingEnabled) {
          keyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key.getBytes());
        }
        if (storageEngine == null) {
          return;
        }
        byte[] result = storageEngine.get(partition, keyBytes);

        ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
        StorageEngineBackedCompressorFactory compressorFactory =
            new StorageEngineBackedCompressorFactory(server.getVeniceServer().getStorageMetadataService());
        VeniceCompressor compressor = compressorFactory.getCompressor(
            CompressionStrategy.NO_OP,
            storageEngine.getStoreVersionName(),
            Zstd.defaultCompressionLevel());

        ByteBuffer value = (ByteBuffer) GenericChunkingAdapter.INSTANCE.get(
            storageEngine,
            partition,
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
        Assert.assertNotNull(globalRtDiv);
      });

      // Instance leaderNode = routingDataRepo.getLeaderInstance(resourceName, 0);
      // Assert.assertNotNull(leaderNode);
      // LOGGER.info("Restart server port {}", leaderNode.getPort());
      // leaderNode.
      // sharedVenice.stopAndRestartVeniceServer(leaderNode.getPort());

      // Utils.sleep(1000000);
      // Object value = client.get(key).get();
      // assertNull(value, "Key " + key + " should be missing!");
    }
  }
}
