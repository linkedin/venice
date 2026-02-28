package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.Utils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.ImmutableChangeCapturePubSubMessage;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.davinci.utils.ClientRmdSerDe;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestVersionSpecificChangelogConsumerReplicationMetadata {
  private static final Logger LOGGER =
      LogManager.getLogger(TestVersionSpecificChangelogConsumerReplicationMetadata.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private final List<AutoCloseable> testCloseables = new ArrayList<>();
  private final List<String> testStoresToDelete = new ArrayList<>();

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;
  private ControllerClient childControllerClientRegion0;
  private D2Client d2Client;
  private ZkServerWrapper localZkServer;
  private PubSubBrokerWrapper localKafka;

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAAWCParallelProcessingEnabled());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
    localZkServer = childDatacenters.get(0).getZkServerWrapper();
    localKafka = childDatacenters.get(0).getPubSubBrokerWrapper();

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    childControllerClientRegion0 =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    d2Client = new D2ClientBuilder().setZkHosts(localZkServer.getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(childControllerClientRegion0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
    TestView.resetCounters();
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    ChangelogConsumerTestUtils.cleanupAfterTest(testCloseables, testStoresToDelete, parentControllerClient, LOGGER);
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testVersionSpecificWithDeserializedReplicationMetadata()
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 10;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    int partitionCount = 3;
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testStore");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 3)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1, true, true);
    testCloseables.add(changeLogConsumer);
    // Rewrite all the keys in near-line
    try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducer(
        childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]),
        storeName,
        Version.PushType.STREAM)) {
      veniceProducer.start();
      for (int i = 1; i <= numKeys; ++i) {
        String value = "near-line" + i;
        sendStreamingRecord(veniceProducer, storeName, i, value, null);
      }
    }
    changeLogConsumer.subscribeAll().get();
    List<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
          changeLogConsumer.poll(1000);
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
        if (message.getKey() != null) {
          pubSubMessages.add(message);
        }
      }
      assertEquals(pubSubMessages.size(), numKeys * 2);
    });
    // The change events written from near-line should have valid and deserialized replication metadata
    try (StoreSchemaFetcher schemaFetcher = ClientFactory.createStoreSchemaFetcher(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
      ClientRmdSerDe clientRmdSerDe = new ClientRmdSerDe(schemaFetcher);
      for (int i = numKeys; i < pubSubMessages.size(); i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessages.get(i);
        assertNotNull(message.getDeserializedReplicationMetadata());
        long timestamp = (long) message.getDeserializedReplicationMetadata().get("timestamp");
        assertTrue(timestamp > 0);
        // Use ClientRmdSerDe to verify the deserialized replication metadata and vice versa
        assertEquals(
            message.getDeserializedReplicationMetadata(),
            clientRmdSerDe.deserializeRmdBytes(
                message.getWriterSchemaId(),
                message.getWriterSchemaId(),
                message.getReplicationMetadataPayload()));
        assertEquals(
            message.getReplicationMetadataPayload(),
            clientRmdSerDe
                .serializeRmdRecord(message.getWriterSchemaId(), message.getDeserializedReplicationMetadata()));
      }
    }
  }
}
