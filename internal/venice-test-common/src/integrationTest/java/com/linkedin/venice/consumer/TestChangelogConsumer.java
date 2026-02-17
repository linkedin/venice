package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LINGER_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.IntegrationTestUtils.pollChangeEventsFromSpecificChangeCaptureConsumer;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithLogicalTimestamp;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V10_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V11_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V5_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V6_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V7_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V8_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V9_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.ImmutableChangeCapturePubSubMessage;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
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
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestChangelogConsumer {
  private static final Logger LOGGER = LogManager.getLogger(TestChangelogConsumer.class);
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
  private static final List<Schema> SCHEMA_HISTORY = Arrays.asList(
      NAME_RECORD_V1_SCHEMA,
      NAME_RECORD_V2_SCHEMA,
      NAME_RECORD_V3_SCHEMA,
      NAME_RECORD_V5_SCHEMA,
      NAME_RECORD_V6_SCHEMA,
      NAME_RECORD_V7_SCHEMA,
      NAME_RECORD_V8_SCHEMA,
      NAME_RECORD_V9_SCHEMA,
      NAME_RECORD_V10_SCHEMA,
      NAME_RECORD_V11_SCHEMA);

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  /**
   * Wait for the meta system store to be ready for the given store.
   * This ensures that the thin client metadata repository can successfully fetch store metadata.
   * The method waits for:
   * 1. The meta system store push to complete
   * 2. The meta store to be queryable via the router (verifies router has healthy routes)
   */
  private void waitForMetaSystemStoreToBeReady(String storeName) {
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    // Wait for meta system store push to complete
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(metaSystemStoreName, 1),
        childControllerClientRegion0,
        90,
        TimeUnit.SECONDS);
    clusterWrapper.refreshAllRouterMetaData();
    // Query the meta store to verify it's accessible via the router
    String routerUrl = clusterWrapper.getRandomRouterURL();
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreClient =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
                .setVeniceURL(routerUrl))) {
      StoreMetaKey storeClusterConfigKey =
          MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(Collections.singletonMap("KEY_STORE_NAME", storeName));
      // Wait for the meta store to be queryable
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        StoreMetaValue value = metaStoreClient.get(storeClusterConfigKey).get();
        Assert.assertNotNull(value, "Meta store should return non-null value for STORE_CLUSTER_CONFIG");
        Assert.assertNotNull(value.storeClusterConfig, "storeClusterConfig should not be null");
      });
    }
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
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

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    childControllerClientRegion0 =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    d2Client = new D2ClientBuilder()
        .setZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    for (int i = testCloseables.size() - 1; i >= 0; i--) {
      try {
        testCloseables.get(i).close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close resource during test cleanup", e);
      }
    }
    testCloseables.clear();

    for (String storeName: testStoresToDelete) {
      try {
        parentControllerClient.disableAndDeleteStore(storeName);
      } catch (Exception e) {
        LOGGER.warn("Failed to delete store {} during test cleanup", storeName, e);
      }
    }
    testStoresToDelete.clear();
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testDisabledStoreVeniceChangelogConsumer() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    TestUtils.assertCommand(
        setupControllerClient
            .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms)));
    // Registering real data schema as schema v2.
    for (Schema schema: SCHEMA_HISTORY) {
      TestUtils.assertCommand(
          setupControllerClient.retryableRequest(
              5,
              controllerClient1 -> setupControllerClient.addValueSchema(storeName, schema.toString())),
          "Failed to add schema: " + schema.toString() + " to store " + storeName);
    }

    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1L)
            .setSpecificValue(TestChangelogValue.class)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0", TestChangelogValue.class);
    testCloseables.add(specificChangelogConsumer);

    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient1 -> setupControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false))));

    // Wait for store update to propagate
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertThrows(StoreDisabledException.class, () -> specificChangelogConsumer.subscribeAll().get()));

    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient1 -> setupControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(true))));

    // wait until reads are enabled and subscribe succeeds
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(setupControllerClient.getStore(storeName).getStore().isEnableStoreReads());
      try {
        specificChangelogConsumer.subscribeAll().get();
      } catch (StoreDisabledException e) {
        Assert.fail("Subscribe should not fail");
      }
    });

    Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();

    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient1 -> setupControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false))));

    // Wait for store update to propagate
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertThrows(
            StoreDisabledException.class,
            () -> pollChangeEventsFromSpecificChangeCaptureConsumer(
                polledChangeEventsMap,
                polledChangeEventsList,
                specificChangelogConsumer)));

    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient1 -> setupControllerClient
                .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(true))));

    // Wait for reads to be enabled in child controller
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(childControllerClientRegion0.getStore(storeName).getStore().isEnableStoreReads());
    });

    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          specificChangelogConsumer);
      Assert.assertEquals(polledChangeEventsList.size(), 100);
      Assert.assertTrue(specificChangelogConsumer.isCaughtUp());
    });

    Assert.assertTrue(
        polledChangeEventsMap.get(Integer.toString(1)).getValue().getCurrentValue() instanceof SpecificRecord);
    TestChangelogValue value = new TestChangelogValue();
    value.firstName = "first_name_1";
    value.lastName = "last_name_1";
    Assert.assertEquals(polledChangeEventsMap.get(Integer.toString(1)).getValue().getCurrentValue(), value);
  }

  // This is a beefier test, so giving it a bit more time
  @Test(timeOut = TEST_TIMEOUT * 3, priority = 3)
  public void testVersionSwapInALoop() throws Exception {
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    props.put(KAFKA_LINGER_MS, 0);
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);

    // This is a dumb check that we're doing just to make static analysis happy
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(setupControllerClient.getStore(storeName).getStore().getCurrentVersion(), 0));

    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());

    // Write Records to the store for version v1, the push job will contain 100 records.
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));

    // Write Records from nearline
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    ChangelogClientConfig globalAfterImageClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1L)
            .setIsBeforeImageView(true);

    VeniceChangelogConsumerClientFactory veniceAfterImageConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalAfterImageClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> versionTopicConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    testCloseables.add(versionTopicConsumer);
    Assert.assertTrue(versionTopicConsumer instanceof VeniceAfterImageConsumerImpl);
    versionTopicConsumer.subscribeAll().get();

    Map<String, Utf8> versionTopicEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 100);
    });

    versionTopicEvents.clear();

    // in a loop, write a push job, then write some stream data, then poll data with versionTopicConsumer
    for (int i = 0; i < 10; i++) {
      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
        // Run Samza job to send PUT and DELETE requests.
        runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
        // Produce a DELETE record with large timestamp
        sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      }

      // run push job
      IntegrationTestPushUtils.runVPJ(props);

      // Assert the push has gone through
      int expectedVersion = i + 2;
      TestUtils.waitForNonDeterministicAssertion(
          90,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), expectedVersion));

      // poll data from version topic
      TestUtils.waitForNonDeterministicAssertion(100, TimeUnit.SECONDS, true, true, () -> {
        IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        Assert.assertEquals(versionTopicEvents.size(), 21);
      });
      versionTopicEvents.clear();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSpecificRecordVeniceChangelogConsumer() throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    TestUtils.assertCommand(
        setupControllerClient
            .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms)));
    // Registering real data schema as schema v2.
    for (Schema schema: SCHEMA_HISTORY) {
      TestUtils.assertCommand(
          setupControllerClient.retryableRequest(
              5,
              controllerClient1 -> setupControllerClient.addValueSchema(storeName, schema.toString())),
          "Failed to add schema: " + schema.toString() + " to store " + storeName);
    }

    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    consumerProperties.put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true);
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1L)
            .setSpecificValue(TestChangelogValue.class)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0", TestChangelogValue.class);
    testCloseables.add(specificChangelogConsumer);

    specificChangelogConsumer.subscribeAll().get();

    Assert.assertFalse(specificChangelogConsumer.isCaughtUp());

    Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          specificChangelogConsumer);
      Assert.assertEquals(polledChangeEventsList.size(), 100);
      Assert.assertTrue(specificChangelogConsumer.isCaughtUp());
    });

    Assert.assertTrue(
        polledChangeEventsMap.get(Integer.toString(1)).getValue().getCurrentValue() instanceof SpecificRecord);
    TestChangelogValue value = new TestChangelogValue();
    value.firstName = "first_name_1";
    value.lastName = "last_name_1";
    Assert.assertEquals(polledChangeEventsMap.get(Integer.toString(1)).getValue().getCurrentValue(), value);
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    GenericRecord genericRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    genericRecord.put("firstName", "Venice");
    genericRecord.put("lastName", "Italy");

    GenericRecord genericRecordV2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    genericRecordV2.put("firstName", "Barcelona");
    genericRecordV2.put("lastName", "Spain");

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Run Samza job to send PUT and DELETE requests.
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
      Utils.sleep(1);
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecordV2, null);
    }

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          specificChangelogConsumer);
      Assert.assertEquals(polledChangeEventsList.size(), 2);
    });
    Assert.assertTrue(
        polledChangeEventsMap.get(Integer.toString(10000)).getValue().getCurrentValue() instanceof SpecificRecord);
    parentControllerClient.disableAndDeleteStore(storeName);
    // Verify that topics and store is cleaned up
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      MultiStoreTopicsResponse storeTopicsResponse = childControllerClient.getDeletableStoreTopics();
      Assert.assertFalse(storeTopicsResponse.isError());
      Assert.assertEquals(storeTopicsResponse.getTopics().size(), 0);
    });
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testChangelogConsumerWithNewValueSchema() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    // setVersionSwapDetectionIntervalTimeInSeconds also controls native metadata repository refresh interval, setting
    // it to a large value to reproduce the scenario where the newly added schema is yet to be fetched.
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    testCloseables.add(changeLogConsumer);

    changeLogConsumer.subscribeAll().get();
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 100);
      Assert.assertTrue(changeLogConsumer.isCaughtUp());
    });
    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient -> setupControllerClient.addValueSchema(storeName, NAME_RECORD_V2_SCHEMA.toString())),
        "Failed to add schema: " + NAME_RECORD_V2_SCHEMA.toString() + " to store " + storeName);
    GenericRecord genericRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    genericRecord.put("firstName", "Venice");
    genericRecord.put("lastName", "Italy");
    genericRecord.put("age", 18);
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Run Samza job to send the new write with schema v2.
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 101);
    });
    Assert.assertNotNull(polledChangeEventsMap.get(Integer.toString(10000)));
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testNewChangelogConsumerWithNewValueSchema()
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    // setVersionSwapDetectionIntervalTimeInSeconds also controls native metadata repository refresh interval, setting
    // it to a large value to reproduce the scenario where the newly added schema is yet to be fetched.
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
            .setIsNewStatelessClientEnabled(true);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    testCloseables.add(changeLogConsumer);

    changeLogConsumer.subscribeAll().get();
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 100);
      Assert.assertTrue(changeLogConsumer.isCaughtUp());
    });
    TestUtils.assertCommand(
        setupControllerClient.retryableRequest(
            5,
            controllerClient -> setupControllerClient.addValueSchema(storeName, NAME_RECORD_V2_SCHEMA.toString())),
        "Failed to add schema: " + NAME_RECORD_V2_SCHEMA.toString() + " to store " + storeName);
    GenericRecord genericRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    genericRecord.put("firstName", "Venice");
    genericRecord.put("lastName", "Italy");
    genericRecord.put("age", 18);
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Run Samza job to send the new write with schema v2.
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 101);
    });
    Assert.assertNotNull(polledChangeEventsMap.get(Integer.toString(10000)));
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testChangeLogConsumerSequenceId() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    testCloseables.add(changeLogConsumer);

    changeLogConsumer.subscribeAll().get();
    final List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      pubSubMessages.addAll(changeLogConsumer.poll(5));
      Assert.assertEquals(pubSubMessages.size(), 100);
    });
    // The consumer sequence id should be consecutive and monotonically increasing within the same partition. All
    // partitions should start with the same sequence id (seeded by consumer initialization timestamp).
    long startingSequenceId = pubSubMessages.iterator().next().getPosition().getConsumerSequenceId();
    HashMap<Integer, Long> partitionSequenceIdMap = new HashMap<>();
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessages) {
      int partition = message.getPartition();
      long expectedSequenceId = partitionSequenceIdMap.computeIfAbsent(partition, k -> startingSequenceId);
      Assert.assertEquals(message.getPosition().getConsumerSequenceId(), expectedSequenceId);
      partitionSequenceIdMap.put(partition, expectedSequenceId + 1);
    }
    Assert.assertEquals(partitionSequenceIdMap.size(), 3);
  }

  @Test(timeOut = TEST_TIMEOUT * 2, priority = 3)
  public void testVersionSpecificSeekingChangeLogConsumer()
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 100;
    int partitionCount = 3;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(partitionCount);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap();
    Map<Integer, VeniceChangeCoordinate> partitionToChangeCoordinateMap = new HashMap();

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {

      for (int partition = 0; partition < partitionCount; partition++) {
        // Ensure we can "seek" multiple times on the same version
        changeLogConsumer.subscribe(Collections.singleton(partition)).get();
      }
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from version 1
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        partitionToChangeCoordinateMap.put(message.getPartition(), message.getPosition());

        assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
        assertTrue(message.getPayloadSize() > 0);
        assertNotNull(message.getPosition());
        assertTrue(message.getWriterSchemaId() > 0);
        assertNotNull(message.getReplicationMetadataPayload());
      }
    }

    // Restart client and resume from the last consumed checkpoints
    pubSubMessagesMap.clear();

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      pubSubMessagesMap.clear();
      changeLogConsumer.seekToCheckpoint(new HashSet<>(partitionToChangeCoordinateMap.values())).get();

      Set<Integer> partitions = new HashSet<>(partitionCount);
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        // get unique partitions
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          partitions.add(message.getPartition());
        }
        assertEquals(partitions.size(), partitionCount);
      });

      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
        // Run Samza job to send PUT and DELETE requests.
        sendStreamingRecord(veniceProducer, storeName, 10000, "10000", null);
      }

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> tempPubSubMessagesList =
            changeLogConsumer.poll(5);
        assertEquals(tempPubSubMessagesList.size(), 1);

        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: tempPubSubMessagesList) {
          assertEquals((int) pubSubMessage.getKey(), 10000);
          assertEquals(pubSubMessage.getValue().getCurrentValue().toString(), "10000");
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testVersionSpecificChangeLogConsumer() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 100;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    testStoresToDelete.add(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready before creating changelog consumer
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    // wait until the version is active in child region
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 1));

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap =
        new VeniceConcurrentHashMap<>(100);

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from version 1
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
        assertTrue(message.getPayloadSize() > 0);
        assertNotNull(message.getPosition());
        assertTrue(message.getWriterSchemaId() > 0);
        assertNotNull(message.getReplicationMetadataPayload());
      }
    }

    // Restart client to ensure it seeks to the beginning of the topic and all record metadata is available
    pubSubMessagesMap.clear();
    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from version 1
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
        assertTrue(message.getPayloadSize() > 0);
        assertNotNull(message.getPosition());
        assertTrue(message.getWriterSchemaId() > 0);
        assertNotNull(message.getReplicationMetadataPayload());
      }

      // Push version 2
      version++;
      TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
      IntegrationTestPushUtils.runVPJ(props);
      // wait until the version is active in child region
      TestUtils.waitForNonDeterministicAssertion(
          90,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(), 2));

      // Client shouldn't be able to poll anything, since it's still on version 1
      assertEquals(changeLogConsumer.poll(5).size(), 0);
    }

    // Restart client
    pubSubMessagesMap.clear();
    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from version 1
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version - 1) + i);
        assertTrue(message.getPayloadSize() > 0);
        assertNotNull(message.getPosition());
        assertTrue(message.getWriterSchemaId() > 0);
        assertNotNull(message.getReplicationMetadataPayload());
      }

      // Push version 3 with deferred version swap and subscribe to the future version
      changeLogConsumer.close();
      pubSubMessagesMap.clear();
      version++;
      TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
      props.put(DEFER_VERSION_SWAP, true);
      IntegrationTestPushUtils.runVPJ(props);
      // Wait for version 3 push to complete (but current version stays at 2 due to deferred swap)
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          childControllerClientRegion0,
          90,
          TimeUnit.SECONDS);

      VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer3 =
          veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, version);
      testCloseables.add(changeLogConsumer3);
      changeLogConsumer3.subscribeAll().get();

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer3.poll(1000);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from future version 3
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
        assertTrue(message.getPayloadSize() > 0);
        assertNotNull(message.getPosition());
        assertTrue(message.getWriterSchemaId() > 0);
        assertNotNull(message.getReplicationMetadataPayload());
      }
    }
  }

  private void runSamzaStreamJob(
      VeniceSystemProducer veniceProducer,
      String storeName,
      Time mockedTime,
      int numPuts,
      int numDels,
      int startIdx) {
    // Send PUT requests.
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      sendStreamingRecord(
          veniceProducer,
          storeName,
          Integer.toString(i),
          "stream_" + i,
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }
    // Send DELETE requests.
    for (int i = startIdx + numPuts; i < startIdx + numPuts + numDels; i++) {
      sendStreamingDeleteRecord(
          veniceProducer,
          storeName,
          Integer.toString(i),
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }
  }
}
