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
import com.linkedin.venice.D2.D2ClientUtils;
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
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVersionSpecificChangelogConsumer {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private D2Client d2Client;

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
    IntegrationTestPushUtils.runVPJ(props);
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
            .setVersionSwapDetectionIntervalTimeInSeconds(3)
            .setUseRequestBasedMetadataRepository(true)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
            .setIncludeControlMessages(true); // Enable control messages passing
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1);

    changeLogConsumer.subscribeAll().get();

    // All data should be from version 1
    // there are 3 partitions, so we expect 3 control messages EOP
    pollAndVerify(changeLogConsumer, 1, numKeys, createControlMessageCountMap(partitionCount, 0));

    // Restart client to ensure it seeks to the beginning of the topic and all record metadata is available
    changeLogConsumer.close();
    changeLogConsumer.subscribeAll().get();

    // All data should be from version 1
    // there are 3 partitions, so we expect 3 control messages EOP
    pollAndVerify(changeLogConsumer, 1, numKeys, createControlMessageCountMap(partitionCount, 0));

    // Push version 2
    version++;
    TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    IntegrationTestPushUtils.runVPJ(props);

    // Client should see VersionSwap control messages since new version is pushed
    pollAndVerify(changeLogConsumer, 1, 0, createControlMessageCountMap(0, partitionCount));

    // Client shouldn't be able to poll anything, since it's still on version 1
    assertEquals(changeLogConsumer.poll(1000).size(), 0);

    // Restart client
    changeLogConsumer.close();
    changeLogConsumer.subscribeAll().get();

    // all data should be from version 1
    pollAndVerify(changeLogConsumer, 1, numKeys, createControlMessageCountMap(partitionCount, partitionCount));

    // Push version 3 with deferred version swap and subscribe to the future version
    changeLogConsumer.close();
    version++;
    TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    props.put(DEFER_VERSION_SWAP, true);
    IntegrationTestPushUtils.runVPJ(props);

    VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer3 =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, version);
    changeLogConsumer3.subscribeAll().get();

    // Client should see version 3 data
    // 3 EOP for v3; and 3 VS for v1->v2
    pollAndVerify(changeLogConsumer3, 3, numKeys, createControlMessageCountMap(partitionCount, partitionCount));
  }

  @Test
  public void testSeekToCheckpointWithVersionSpecific() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 10;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    int partitionCount = 3;
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
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
    IntegrationTestPushUtils.runVPJ(props);
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
            .setVersionSwapDetectionIntervalTimeInSeconds(3)
            .setUseRequestBasedMetadataRepository(true)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1);

    changeLogConsumer.subscribeAll().get();

    // All data should be from version 1
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
          changeLogConsumer.poll(1000);
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
        if (message.getKey() != null) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
      }
      // There are 10 messages with keys from 1 to 10
      assertEquals(pubSubMessagesMap.size(), numKeys);
    });

    // Get checkpoint from key 5
    HashSet<VeniceChangeCoordinate> checkpoints = new HashSet<>();
    ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
        (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(5);
    VeniceChangeCoordinate vcc = message.getPosition();
    checkpoints.add(vcc);

    // Restart client
    changeLogConsumer.close();

    // Seek to checkpoint
    changeLogConsumer.seekToCheckpoint(checkpoints).get();

    // all data should be from version 1 starting from key 6
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMapAfterSeek =
        new HashMap();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
          changeLogConsumer.poll(1000);
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> polledMessage: pubSubMessagesList) {
        if (polledMessage.getKey() != null) {
          pubSubMessagesMapAfterSeek.put(polledMessage.getKey(), polledMessage);
        }
      }
      assertEquals(pubSubMessagesMapAfterSeek.size(), numKeys - 5);
    });
  }

  private void pollAndVerify(
      VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer,
      int version,
      int expectedNumMessages,
      HashMap<Integer, Integer> expectedControlMessagesCountPerType) throws InterruptedException {
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap();
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> controlMessageMap = new HashMap();

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
          controlMessageMap.put(controlMessagesCount, message);
          controlMessagesCount++;
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
  }

  private HashMap<Integer, Integer> createControlMessageCountMap(int endOfPushCount, int versionSwapCount) {
    HashMap<Integer, Integer> controlMessageTypeCountMap = new HashMap<>();
    controlMessageTypeCountMap.put(ControlMessageType.END_OF_PUSH.getValue(), endOfPushCount);
    controlMessageTypeCountMap.put(ControlMessageType.VERSION_SWAP.getValue(), versionSwapCount);
    return controlMessageTypeCountMap;
  }
}
