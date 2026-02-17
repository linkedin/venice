package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.Utils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
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
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
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
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestVersionSpecificChangelogConsumer {
  private static final Logger LOGGER = LogManager.getLogger(TestVersionSpecificChangelogConsumer.class);
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

  private void waitForMetaSystemStoreToBeReady(String storeName) {
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(metaSystemStoreName, 1),
        childControllerClientRegion0,
        90,
        TimeUnit.SECONDS);
    clusterWrapper.refreshAllRouterMetaData();
    String routerUrl = clusterWrapper.getRandomRouterURL();
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreClient =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
                .setVeniceURL(routerUrl))) {
      StoreMetaKey storeClusterConfigKey =
          MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(Collections.singletonMap("KEY_STORE_NAME", storeName));
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
        StoreMetaValue value = metaStoreClient.get(storeClusterConfigKey).get(30, TimeUnit.SECONDS);
        Assert.assertNotNull(value, "Meta store should return non-null value for STORE_CLUSTER_CONFIG");
        Assert.assertNotNull(value.storeClusterConfig, "storeClusterConfig should not be null");
      });
    }
  }

  private Properties buildConsumerProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafka.getAddress());
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    return consumerProperties;
  }

  private ChangelogClientConfig buildBaseChangelogClientConfig(Properties consumerProperties) {
    return new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(3);
  }

  private UpdateStoreQueryParams buildDefaultStoreParams() {
    return new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
  }

  private void waitForVersionToBeActive(String storeName, int expectedVersion) {
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(),
            expectedVersion));
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testVersionSpecificChangeLogConsumerWithControlMessages()
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 10;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    int partitionCount = 3;
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
    UpdateStoreQueryParams storeParms = buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    waitForVersionToBeActive(storeName, 1);
    Properties consumerProperties = buildConsumerProperties();
    ChangelogClientConfig globalChangelogClientConfig =
        buildBaseChangelogClientConfig(consumerProperties).setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1, true);
    testCloseables.add(changeLogConsumer);

    changeLogConsumer.subscribeAll().get();

    // All data should be from version 1
    // there are 3 partitions, so we expect 3 control messages EOP
    pollAndVerify(
        changeLogConsumer,
        1,
        numKeys,
        createControlMessageCountMap(partitionCount, 0),
        partitionCount,
        false);

    // Restart client to ensure it seeks to the beginning of the topic and all record metadata is available
    changeLogConsumer.close();
    changeLogConsumer.subscribeAll().get();

    // All data should be from version 1
    // there are 3 partitions, so we expect 3 control messages EOP
    pollAndVerify(
        changeLogConsumer,
        1,
        numKeys,
        createControlMessageCountMap(partitionCount, 0),
        partitionCount,
        false);

    // Push version 2
    version++;
    TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    IntegrationTestPushUtils.runVPJ(props);
    waitForVersionToBeActive(storeName, 2);

    // Client should see VersionSwap control messages since new version is pushed
    pollAndVerify(changeLogConsumer, 1, 0, createControlMessageCountMap(0, partitionCount), partitionCount, true);

    // Client shouldn't be able to poll anything besides heartbeat messages, since it's still on version 1
    int nonHeartbeatMessagesCount = 0;
    Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> oldVersionMessages =
        changeLogConsumer.poll(1000);
    for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: oldVersionMessages) {
      if (message.getKey() != null) {
        nonHeartbeatMessagesCount++;
      } else {
        ControlMessage controlMessage =
            ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) message).getControlMessage();
        if (controlMessage.getControlMessageType() != ControlMessageType.START_OF_SEGMENT.getValue()) {
          nonHeartbeatMessagesCount++;
        }
      }
    }
    assertEquals(nonHeartbeatMessagesCount, 0);

    // Restart a new client
    changeLogConsumer.close();
    VeniceChangelogConsumer<Integer, Utf8> newChangeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1, true);
    testCloseables.add(newChangeLogConsumer);
    newChangeLogConsumer.subscribeAll().get();

    // all data should be from version 1
    pollAndVerify(
        newChangeLogConsumer,
        1,
        numKeys,
        createControlMessageCountMap(partitionCount, partitionCount),
        partitionCount,
        true);

    // Push version 3 with deferred version swap and subscribe to the future version
    newChangeLogConsumer.close();
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
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, version, true);
    testCloseables.add(changeLogConsumer3);
    changeLogConsumer3.subscribeAll().get();

    // Client should see version 3 data
    // 3 EOP for v3; and 3 VS for v1->v2
    pollAndVerify(
        changeLogConsumer3,
        3,
        numKeys,
        createControlMessageCountMap(partitionCount, partitionCount),
        partitionCount,
        true);
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
    UpdateStoreQueryParams storeParms = buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    waitForMetaSystemStoreToBeReady(storeName);
    IntegrationTestPushUtils.runVPJ(props);
    waitForVersionToBeActive(storeName, 1);
    Properties consumerProperties = buildConsumerProperties();
    ChangelogClientConfig globalChangelogClientConfig =
        buildBaseChangelogClientConfig(consumerProperties).setD2Client(d2Client)
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

  private void pollAndVerify(
      VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer,
      int version,
      int expectedNumMessages,
      HashMap<Integer, Integer> expectedControlMessagesCountPerType,
      int partitionCount,
      boolean verifyHeartbeatMessages) throws InterruptedException {
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap();
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> controlMessageMap = new HashMap();
    Map<Integer, Long> heartbeatTimestampMap = new HashMap<>();

    int expectedTotalControlMessageCount =
        expectedControlMessagesCountPerType.values().stream().mapToInt(Integer::intValue).sum();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
          changeLogConsumer.poll(1000);
      int controlMessagesCount = 0;
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
        if (message.getKey() != null) {
          pubSubMessagesMap.put(message.getKey(), message);
        } else {
          // Validate heartbeat messages separately due to its non-deterministic nature
          ControlMessage controlMessage =
              ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) message).getControlMessage();
          if (controlMessage.getControlMessageType() == ControlMessageType.START_OF_SEGMENT.getValue()) {
            heartbeatTimestampMap.put(message.getPartition(), message.getPubSubMessageTime());
          } else {
            controlMessageMap.put(controlMessagesCount, message);
            controlMessagesCount++;
          }
        }
      }
      assertEquals(pubSubMessagesMap.size(), expectedNumMessages);
      assertEquals(controlMessageMap.size(), expectedTotalControlMessageCount);
    });

    for (int i = 1; i <= expectedNumMessages; i++) {
      ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
          (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
      assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
      assertTrue(message.getPayloadSize() > 0);
      assertNotNull(message.getPosition());
      assertTrue(message.getWriterSchemaId() > 0);
      assertNotNull(message.getReplicationMetadataPayload());
    }

    int endOfPushCount = 0;
    int versionSwapCount = 0;
    for (int i = 0; i < expectedTotalControlMessageCount; i++) {
      ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> controlMessage =
          (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) controlMessageMap.get(i);
      int controlMessageType = controlMessage.getControlMessage().getControlMessageType();
      if (controlMessageType == ControlMessageType.END_OF_PUSH.getValue()) {
        endOfPushCount++;
      } else if (controlMessageType == ControlMessageType.VERSION_SWAP.getValue()) {
        versionSwapCount++;
      } else {
        fail("Unexpected control message type: " + controlMessageType);
      }
    }
    assertEquals(createControlMessageCountMap(endOfPushCount, versionSwapCount), expectedControlMessagesCountPerType);
    if (verifyHeartbeatMessages) {
      assertEquals(heartbeatTimestampMap.size(), partitionCount);
      for (long heartbeatTimestamp: heartbeatTimestampMap.values()) {
        assertTrue(heartbeatTimestamp > 0);
      }
    }
  }

  private HashMap<Integer, Integer> createControlMessageCountMap(int endOfPushCount, int versionSwapCount) {
    HashMap<Integer, Integer> controlMessageTypeCountMap = new HashMap<>();
    controlMessageTypeCountMap.put(ControlMessageType.END_OF_PUSH.getValue(), endOfPushCount);
    controlMessageTypeCountMap.put(ControlMessageType.VERSION_SWAP.getValue(), versionSwapCount);
    return controlMessageTypeCountMap;
  }
}
