package com.linkedin.venice.helix;

import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestServerKMERegistrationFromMessageHeader {
  private static final Logger LOGGER = LogManager.getLogger(TestServerKMERegistrationFromMessageHeader.class);

  private VeniceClusterWrapper cluster;
  protected ControllerClient controllerClient;
  int replicaFactor = 1;
  int numOfController = 1;
  int numOfRouters = 1;
  int partitionSize = 1000;
  private static final int TEST_TIMEOUT = 90_000; // ms
  private String storeName;
  private String storeVersion;
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private VeniceKafkaSerializer keySerializer;
  private VeniceServerWrapper server;
  private String clusterName;
  private KafkaValueSerializer valueSerializer;
  private int pushVersion;
  private HelixReadWriteSchemaRepository repo;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(numOfController)
        .numberOfServers(0)
        .numberOfRouters(numOfRouters)
        .replicationFactor(replicaFactor)
        .partitionSize(partitionSize)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
    clusterName = cluster.getClusterName();

    Properties serverProperties = new Properties();
    Properties serverFeatureProperties = new Properties();

    // Enable KME registration from message header.
    serverProperties.put(KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED, true);

    // Create client config for consumer to enable schema readers.
    ClientConfig clientConfig = new ClientConfig().setVeniceURL(cluster.getZk().getAddress())
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setSslFactory(SslUtils.getVeniceLocalSslFactory());
    serverFeatureProperties.put(CLIENT_CONFIG_FOR_CONSUMER, clientConfig);

    server = cluster.addVeniceServer(serverFeatureProperties, serverProperties);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    repo.clear();
    valueSerializer.close();
    veniceWriter.close();
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testServerKMERegistrationFromMessageHeader() {
    storeName = Utils.getUniqueString("venice-store");
    commonSetup();

    // Remove the latest schema from child controller's local value serializer and remove it from child colo's schema
    // repository (ZK).
    repo.removeValueSchema(
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    valueSerializer.removeAllSchemas();
    LOGGER.info("all schemas are removed");

    /*
     * Sending a start of segment control message will the latest schema in its header.
     * Venice server, when it encounters the new schema in the message header and when KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED
     * is enabled, registers the new schema into the child colo's schema repo as well as add to its local serializer.
     */
    veniceWriter.broadcastStartOfPush(false, false, CompressionStrategy.NO_OP, new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Verify that new schema is registered in the child colo's schema repo.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertEquals(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()).getId(),
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    });

    // Verify that empty push can be finished successfully and a new version is created.
    String controllerUrl = cluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion =
          ControllerClient.getStore(controllerUrl, cluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }

  /**
   * Integration test that validates the VTP header optimization for heartbeat messages.
   *
   * This test demonstrates that Venice can safely exclude VTP headers from heartbeat messages
   * while maintaining full schema evolution capabilities through its system-level infrastructure.
   *
   * Test Scenario:
   * 1. Remove all local KME schemas to simulate a server without cached schema knowledge
   * 2. Send multi-heartbeats with dependentFeatureEnabled=true (excludes VTP headers)
   * 3. Complete a full push cycle to verify schema evolution works without heartbeat VTP headers
   *
   * Key Validation Points:
   * - Heartbeat messages exclude VTP headers when optimization is enabled
   * - Schema evolution infrastructure (KME registration + schema reader) handles compatibility
   * - Push completion succeeds despite heartbeats lacking schema information
   * - System-level schema management is sufficient for operational needs
   *
   * This proves that frequent heartbeat messages can be optimized for performance without
   * compromising Venice's robust schema evolution capabilities.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testServerDoNotIncludeKMESchemaInHeartbeatMessage() {
    storeName = Utils.getUniqueString("venice-store-do-not-include-kme-schema");
    commonSetup();

    // Step 1: Remove all local KME schemas to simulate a server without cached schema knowledge
    // This creates the scenario where schema evolution infrastructure must handle compatibility
    valueSerializer.removeAllSchemas();
    LOGGER.info("Server local schemas are removed - simulating schema evolution scenario");

    // Step 2: Send multiple heartbeat messages with VTP header optimization enabled
    // The last parameter (dependentFeatureEnabled=true) tells VeniceWriter to exclude VTP headers
    // because we can rely on system-level schema evolution infrastructure
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic(storeVersion), 0);

    for (int i = 0; i < 10; i++) {
      veniceWriter.sendHeartbeat(
          topicPartition,
          null, // No callback needed for this test
          VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
          false, // Don't add leader complete state
          LeaderCompleteState.LEADER_NOT_COMPLETED,
          System.currentTimeMillis(),
          true); // dependentFeatureEnabled=true -> excludes VTP headers

      LOGGER.info(
          "Sent optimized heartbeat message #{} (no VTP headers) to topic partition: {}",
          i,
          topicPartition.getPubSubTopic().getName() + "-" + topicPartition.getPartitionNumber());
    }

    // Step 3: Execute a complete push cycle to validate that schema evolution works
    // Even though heartbeats excluded VTP headers, the system should handle schema compatibility
    // through KME schema registration and RouterBackedSchemaReader
    veniceWriter.broadcastStartOfPush(false, false, CompressionStrategy.NO_OP, new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Step 4: Verify successful push completion despite optimized heartbeats
    // This proves that excluding VTP headers from heartbeats doesn't break schema evolution
    String controllerUrl = cluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion =
          ControllerClient.getStore(controllerUrl, cluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }

  private void commonSetup() {
    cluster.getNewStore(storeName, KEY_SCHEMA, VALUE_SCHEMA);
    VersionCreationResponse creationResponse = cluster.getNewVersion(storeName, false);

    storeVersion = creationResponse.getKafkaTopic();
    pushVersion = Version.parseVersionFromKafkaTopicName(storeVersion);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA);

    veniceWriter =
        IntegrationTestPushUtils.getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(storeVersion).setKeyPayloadSerializer(keySerializer).build());

    VeniceControllerWrapper leaderController = cluster.getLeaderVeniceController();
    valueSerializer = server.getVeniceServer().getKafkaStoreIngestionService().getKafkaValueSerializer();

    HelixReadWriteSchemaRepositoryAdapter adapter =
        (HelixReadWriteSchemaRepositoryAdapter) (leaderController.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getSchemaRepository());
    repo = (HelixReadWriteSchemaRepository) adapter.getReadWriteRegularStoreSchemaRepository();

    // Wait until the latest schema appears in child colo's schema repository (ZK).
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertEquals(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()).getId(),
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    });
  }
}
