package com.linkedin.venice.helix;

import static com.linkedin.venice.ConfigKeys.KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

  @BeforeClass
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
    serverProperties.put(KME_REGISTRATION_FROM_MESSAGE_HEADER_ENABLED, true);
    server = cluster.addVeniceServer(serverFeatureProperties, serverProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testServerKMERegistrationFromMessageHeader() {
    storeName = Utils.getUniqueString("venice-store");
    cluster.getNewStore(storeName, KEY_SCHEMA, VALUE_SCHEMA);
    VersionCreationResponse creationResponse = cluster.getNewVersion(storeName, false);

    storeVersion = creationResponse.getKafkaTopic();
    int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersion);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA);

    veniceWriter =
        IntegrationTestPushUtils.getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(storeVersion).setKeyPayloadSerializer(keySerializer).build());

    VeniceControllerWrapper leaderController = cluster.getLeaderVeniceController();
    KafkaValueSerializer valueSerializer =
        server.getVeniceServer().getKafkaStoreIngestionService().getKafkaValueSerializer();

    HelixReadWriteSchemaRepositoryAdapter adapter =
        (HelixReadWriteSchemaRepositoryAdapter) (leaderController.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getSchemaRepository());
    HelixReadWriteSchemaRepository repo =
        (HelixReadWriteSchemaRepository) adapter.getReadWriteRegularStoreSchemaRepository();

    // Wait until the latest schema appears in child colo's schema repository (ZK).
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertEquals(
          repo.getValueSchema(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()).getId(),
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    });

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
}
