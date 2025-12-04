package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.*;
import static com.linkedin.venice.stats.VeniceMetricsRepository.*;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.*;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.createAndVerifyStoreInAllRegions;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestWriteUtils.*;
import static com.linkedin.venice.vpj.VenicePushJobConstants.*;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This A/A integration test environment is intended to verify the version swap message and relevant change capture
 * consumer behaviors in A/A environment.
 * TODO add client/consumer side integration tests here too once implementation is available in future PR.
 */
public class TestActiveActiveVersionSwapMessage {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = TEST_TIMEOUT / 2;

  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  protected static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  protected static final int NUMBER_OF_CLUSTERS = 1;

  static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private D2Client d2ClientForDC0Region;
  private Properties serverProperties;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0Client;
  private ControllerClient dc1Client;
  private List<ControllerClient> dcControllerClientList;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 second;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    // Enable multi data center version swap message write
    controllerProps.put(CONTROLLER_USE_MULTI_REGION_REAL_TIME_TOPIC_SWITCHER_ENABLED, true);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    // Set up a d2 client for DC0 region
    d2ClientForDC0Region = new D2ClientBuilder().setZkHosts(childDatacenters.get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2ClientForDC0Region);

    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (d2ClientForDC0Region != null) {
      D2ClientUtils.shutdownClient(d2ClientForDC0Region);
    }
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(dc0Client);
    Utils.closeQuietlyWithErrorLogged(dc1Client);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterVersioSwapMessageWrite() {
    String storeName = Utils.getUniqueString("test-store");
    try {
      createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true));
      // Empty push to create a version
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      assertCommand(versionCreationResponse);
      TestUtils.waitForNonDeterministicAssertion(
          PUSH_TIMEOUT,
          TimeUnit.MILLISECONDS,
          true,
          () -> Assert.assertEquals(
              parentControllerClient.queryJobStatus(versionCreationResponse.getKafkaTopic()).getStatus(),
              ExecutionStatus.COMPLETED.toString()));
      // Another empty push should trigger version swap message write
      VersionCreationResponse newVersionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      assertCommand(newVersionCreationResponse);
      TestUtils.waitForNonDeterministicAssertion(
          PUSH_TIMEOUT,
          TimeUnit.MILLISECONDS,
          true,
          () -> Assert.assertEquals(
              parentControllerClient.queryJobStatus(newVersionCreationResponse.getKafkaTopic()).getStatus(),
              ExecutionStatus.COMPLETED.toString()));
      // Verify version swap messages are written to RT in all data centers and replicated to VTs
      // dc-0 RT: v1->v2 source dc-0 dest dc-0, v1->v2 source dc-1 dest dc-0
      // dc-1 RT: v1->v2 source dc-0 dest dc-1, v1->v2 source dc-1 dest dc-1
      // VTs in each data center should receive 4 version swap messages each in total
      verifyVersionSwapMessagesInAllDataCenters(
          versionCreationResponse.getKafkaTopic(),
          versionCreationResponse.getKafkaTopic(),
          newVersionCreationResponse.getKafkaTopic(),
          0,
          4);
      verifyVersionSwapMessagesInAllDataCenters(
          newVersionCreationResponse.getKafkaTopic(),
          versionCreationResponse.getKafkaTopic(),
          newVersionCreationResponse.getKafkaTopic(),
          0,
          4);
    } finally {
      deleteStores(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCDCMultiDataCenterVersionSwapMessageHandling() throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    try {
      String keySchemaStr = TestChangelogKey.SCHEMA$.toString();
      String valueSchemaStr = TestChangelogValue.SCHEMA$.toString();
      createAndVerifyStoreInAllRegions(
          storeName,
          parentControllerClient,
          dcControllerClientList,
          keySchemaStr,
          valueSchemaStr);
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true));
      // Force to rewind the entire RT
      parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(600L));
      // Empty push to create a version
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      assertCommand(versionCreationResponse);
      TestUtils.waitForNonDeterministicAssertion(
          PUSH_TIMEOUT,
          TimeUnit.MILLISECONDS,
          true,
          () -> Assert.assertEquals(
              parentControllerClient.queryJobStatus(versionCreationResponse.getKafkaTopic()).getStatus(),
              ExecutionStatus.COMPLETED.toString()));
      // Write 100 records in one child region
      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducer(
          childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]),
          storeName,
          Version.PushType.STREAM)) {
        veniceProducer.start();
        performStreamWrites(veniceProducer, storeName, 100, 0);
      }
      // Write another 50 records in remote region
      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducer(
          childDatacenters.get(1).getClusters().get(CLUSTER_NAMES[0]),
          storeName,
          Version.PushType.STREAM)) {
        veniceProducer.start();
        performStreamWrites(veniceProducer, storeName, 50, 100);
      }
      // Start a local consumer with version swap by control messages that should subscribe to v1
      MetricsRepository metricsRepository = new MetricsRepository();
      VeniceChangelogConsumerClientFactory changelogConsumerClientFactory = new VeniceChangelogConsumerClientFactory(
          getDefaultVersionSwapEnabledChangelogClientConfig(childDatacenters.get(0)),
          metricsRepository);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer =
          changelogConsumerClientFactory.getChangelogConsumer(storeName);
      consumer.subscribeAll().get();
      Map<String, VeniceChangeCoordinate> polledChangeEventsMap = new HashMap<>();
      List<String> polledChangeEventsKeyList = new ArrayList<>();
      pollUntilSpecificIndexes(
          consumer,
          polledChangeEventsMap,
          polledChangeEventsKeyList,
          Collections.singleton("149"));
      // Start a second consumer configured with a short version swap timeout and a non-existing region to simulate when
      // a region is down.
      MetricsRepository metricsRepository2 = new MetricsRepository();
      ChangelogClientConfig timeoutConsumerConfig =
          getDefaultVersionSwapEnabledChangelogClientConfig(childDatacenters.get(0));
      timeoutConsumerConfig.setTotalRegionCount(childDatacenters.size() + 1);
      timeoutConsumerConfig.setVersionSwapTimeoutInMs(1000L);
      timeoutConsumerConfig.setConsumerName("timeout-consumer");
      VeniceChangelogConsumerClientFactory timeoutConsumerFactory =
          new VeniceChangelogConsumerClientFactory(timeoutConsumerConfig, metricsRepository2);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> timeoutConsumer =
          timeoutConsumerFactory.getChangelogConsumer(storeName);
      timeoutConsumer.subscribeAll().get();
      Map<String, VeniceChangeCoordinate> timeoutConsumerpolledChangeEventsMap = new HashMap<>();
      List<String> timeoutConsumerpolledChangeEventsKeyList = new ArrayList<>();
      // For the other consumer we need to make sure it consumes to tail for all partitions for ease of verification.
      // Since it has a short version swap timeout, without this condition a partition could forcefully swap to the new
      // version before reaching the first version swap message.
      pollUntilSpecificEventKeyListSize(
          timeoutConsumer,
          timeoutConsumerpolledChangeEventsMap,
          timeoutConsumerpolledChangeEventsKeyList,
          150);

      // Another empty push should trigger version swap message write and switch current version to v2
      VersionCreationResponse newVersionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      assertCommand(newVersionCreationResponse);
      TestUtils.waitForNonDeterministicAssertion(
          PUSH_TIMEOUT,
          TimeUnit.MILLISECONDS,
          true,
          () -> Assert.assertEquals(
              parentControllerClient.queryJobStatus(newVersionCreationResponse.getKafkaTopic()).getStatus(),
              ExecutionStatus.COMPLETED.toString()));
      // Write 5 more records in local region, we need enough to cover all the partitions.
      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducer(
          childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]),
          storeName,
          Version.PushType.STREAM)) {
        veniceProducer.start();
        performStreamWrites(veniceProducer, storeName, 5, 150);
      }
      Set<String> afterNewVersionIndexes = new HashSet<>();
      for (int i = 150; i < 155; i++) {
        afterNewVersionIndexes.add(String.valueOf(i));
      }
      pollUntilSpecificIndexes(consumer, polledChangeEventsMap, polledChangeEventsKeyList, afterNewVersionIndexes);
      // In this controlled test we shouldn't see any duplicates since version swap messages are not interleaved with
      // any data writes. 100 local writes + 50 remote writs + 5 after version swap writes = 155
      Assert.assertEquals(polledChangeEventsKeyList.size(), 155);
      for (int i = 0; i < 155; i++) {
        Assert.assertTrue(polledChangeEventsMap.containsKey(String.valueOf(i)));
      }
      // Hacky verification using toString since internal topic is not exposed by the VeniceChangeCoordinate
      Assert
          .assertTrue(polledChangeEventsMap.get("154").toString().contains(newVersionCreationResponse.getKafkaTopic()));

      // The other consumer should be able to consume at least 305 events if version swap via timeout is successful.
      // 150 (base) + 150 (base replayed in new version) + 5 (new writes after version swap) = 305.
      pollUntilSpecificEventKeyListSize(
          timeoutConsumer,
          timeoutConsumerpolledChangeEventsMap,
          timeoutConsumerpolledChangeEventsKeyList,
          305);
      Assert.assertTrue(
          timeoutConsumerpolledChangeEventsMap.get("149")
              .toString()
              .contains(newVersionCreationResponse.getKafkaTopic()));
    } finally {
      deleteStores(storeName);
    }
  }

  @Test
  public void testSeekToCheckpointWithVersionSpecific() throws IOException, ExecutionException, InterruptedException {
    String storeName = Utils.getUniqueString("test-store");
    try {
      String keySchemaStr = TestChangelogKey.SCHEMA$.toString();
      String valueSchemaStr = TestChangelogValue.SCHEMA$.toString();
      createAndVerifyStoreInAllRegions(
          storeName,
          parentControllerClient,
          dcControllerClientList,
          keySchemaStr,
          valueSchemaStr);
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true));
      // Force to rewind the entire RT
      parentControllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(1).setHybridRewindSeconds(600L));
      // Empty push to create a version
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      assertCommand(versionCreationResponse);
      TestUtils.waitForNonDeterministicAssertion(
          PUSH_TIMEOUT,
          TimeUnit.MILLISECONDS,
          true,
          () -> Assert.assertEquals(
              parentControllerClient.queryJobStatus(versionCreationResponse.getKafkaTopic()).getStatus(),
              ExecutionStatus.COMPLETED.toString()));

      // Write 100 records in one child region
      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducer(
          childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]),
          storeName,
          Version.PushType.STREAM)) {
        veniceProducer.start();
        performStreamWrites(veniceProducer, storeName, 100, 0);
      }

      ChangelogClientConfig globalChangelogClientConfig =
          getDefaultVersionSwapEnabledChangelogClientConfig(childDatacenters.get(0));
      MetricsRepository metricsRepository = new MetricsRepository();
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
          new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> changeLogConsumer =
          veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1);

      changeLogConsumer.subscribeAll().get();

      // All data should be from version 1
      Map<Integer, VeniceChangeCoordinate> pubSubMessagesMap = new HashMap();

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(1000);
        for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          if (message.getKey() != null) {
            pubSubMessagesMap.put((Integer) (message.getKey().get("id")), message.getPosition());
          }
        }
        assertEquals(pubSubMessagesMap.size(), 100);
      });

      HashSet<VeniceChangeCoordinate> checkpoints = new HashSet<>();
      checkpoints.add(pubSubMessagesMap.get(50));

      // Restart client
      changeLogConsumer.unsubscribeAll();

      // Seek to checkpoint
      changeLogConsumer.seekToCheckpoint(checkpoints).get();

      // all data should be from version 1 starting from key 6
      Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMapAfterSeek =
          new HashMap();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(1000);
        for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> polledMessage: pubSubMessagesList) {
          if (polledMessage.getKey() != null) {
            pubSubMessagesMapAfterSeek.put((Integer) polledMessage.getKey().get("id"), null);
          }
        }
        assertEquals(pubSubMessagesMapAfterSeek.size(), 49);
      });

    } finally {
      deleteStores(storeName);
    }
  }

  private void pollUntilSpecificIndexes(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, VeniceChangeCoordinate> polledChangeEventsMap,
      List<String> polledChangeEventsKeyList,
      Set<String> specificIndexes) {
    final Set<String> consumedSpecificIndexes = new HashSet<>();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessages =
          consumer.poll(100);
      for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
        String key = pubSubMessage.getKey() == null ? null : String.valueOf(pubSubMessage.getKey().get("id"));
        polledChangeEventsMap.put(key, pubSubMessage.getPosition());
        polledChangeEventsKeyList.add(key);
        if (key != null && specificIndexes.contains(key)) {
          consumedSpecificIndexes.add(key);
        }
      }
      return consumedSpecificIndexes.size() == specificIndexes.size();
    });
  }

  private void pollUntilSpecificEventKeyListSize(
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer,
      Map<String, VeniceChangeCoordinate> polledChangeEventsMap,
      List<String> polledChangeEventsKeyList,
      int specifiedSize) {
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessages =
          consumer.poll(100);
      for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
        String key = pubSubMessage.getKey() == null ? null : String.valueOf(pubSubMessage.getKey().get("id"));
        polledChangeEventsMap.put(key, pubSubMessage.getPosition());
        polledChangeEventsKeyList.add(key);
      }
      return polledChangeEventsKeyList.size() >= specifiedSize;
    });
  }

  private ChangelogClientConfig getDefaultVersionSwapEnabledChangelogClientConfig(
      VeniceMultiClusterWrapper localRegion) {
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localRegion.getPubSubBrokerWrapper().getAddress());
    ChangelogClientConfig changelogClientConfig = new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localRegion.getZkServerWrapper().getAddress())
        .setD2Client(IntegrationTestPushUtils.getD2Client(localRegion.getZkServerWrapper().getAddress()))
        .setVersionSwapDetectionIntervalTimeInSeconds(3L)
        .setControllerRequestRetryCount(3)
        .setBootstrapFileSystemPath(getTempDataDirectory().getAbsolutePath())
        .setVersionSwapByControlMessageEnabled(true)
        .setClientRegionName(localRegion.getRegionName())
        .setTotalRegionCount(childDatacenters.size());
    return changelogClientConfig;
  }

  private void performStreamWrites(SystemProducer veniceProducer, String storeName, int numPuts, int startIndex) {
    for (int i = startIndex; i < startIndex + numPuts; i++) {
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;

      Object valueObject;
      TestChangelogValue value = new TestChangelogValue();
      value.firstName = "first_name_stream_" + i;
      value.lastName = "last_name_stream_" + i;
      valueObject = value;

      sendStreamingRecord(veniceProducer, storeName, key, valueObject, null);
    }
  }

  private void verifyVersionSwapMessagesInAllDataCenters(
      String topicName,
      String oldVersionTopic,
      String newVersionTopic,
      int partitionId,
      int expectedVersionSwapMessageCount) {
    for (VeniceMultiClusterWrapper dc: childDatacenters) {
      String dcName = dc.getRegionName();
      List<VersionSwap> versionSwapMessages =
          dumpVersionSwapMessages(dc.getPubSubBrokerWrapper(), topicName, partitionId, expectedVersionSwapMessageCount);
      List<VersionSwap> versionSwapMessagesFromThisDC = new ArrayList<>();
      for (VersionSwap versionSwap: versionSwapMessages) {
        if (versionSwap.sourceRegion.toString().equals(dcName)) {
          versionSwapMessagesFromThisDC.add(versionSwap);
        }
      }
      Assert.assertEquals(versionSwapMessagesFromThisDC.size(), 2);
      for (VersionSwap versionSwap: versionSwapMessagesFromThisDC) {
        Assert.assertEquals(versionSwap.oldServingVersionTopic.toString(), oldVersionTopic);
        Assert.assertEquals(versionSwap.newServingVersionTopic.toString(), newVersionTopic);
      }
      Assert.assertEquals(
          versionSwapMessagesFromThisDC.get(0).generationId,
          versionSwapMessagesFromThisDC.get(1).generationId);
      Assert.assertNotEquals(
          versionSwapMessagesFromThisDC.get(0).destinationRegion,
          versionSwapMessagesFromThisDC.get(1).destinationRegion);
    }
  }

  private List<VersionSwap> dumpVersionSwapMessages(
      PubSubBrokerWrapper pubSubBrokerWrapper,
      String topicName,
      int partitionId,
      int expectedVersionSwapMessageCount) {
    List<VersionSwap> versionSwapMessages = new ArrayList<>();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {
      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(topicName), partitionId),
          PubSubSymbolicPosition.EARLIEST,
          false);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(1000 * Time.MS_PER_SECOND);
        messages.forEach((topicPartition, messageList) -> {
          messageList.forEach(message -> {
            if (message.getKey().isControlMessage()) {
              KafkaMessageEnvelope kme = message.getValue();
              ControlMessage cm = (ControlMessage) kme.payloadUnion;
              if (cm.getControlMessageType() == ControlMessageType.VERSION_SWAP.getValue()) {
                versionSwapMessages.add((VersionSwap) cm.controlMessageUnion);
              }
            }
          });
        });
        Assert.assertEquals(versionSwapMessages.size(), expectedVersionSwapMessageCount);
      });
    }
    return versionSwapMessages;
  }

  private void deleteStores(String... storeNames) {
    CompletableFuture.runAsync(() -> {
      try {
        for (String storeName: storeNames) {
          parentControllerClient.disableAndDeleteStore(storeName);
        }
      } catch (Exception e) {
        // ignore... this is just best-effort.
      }
    });
  }
}
