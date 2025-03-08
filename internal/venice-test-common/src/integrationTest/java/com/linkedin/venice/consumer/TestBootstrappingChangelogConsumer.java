package com.linkedin.venice.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithLogicalTimestamp;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBootstrappingChangelogConsumer {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final String REGION_NAME = "local-pubsub";

  // Use a unique key for DELETE with RMD validation
  private static final int deleteWithRmdKeyIndex = 1000;
  private static final TestMockTime testMockTime = new TestMockTime();

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;
  private D2Client d2Client;
  private MetricsRepository metricsRepository;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
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
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()),
        "Failed to configure active-active replication for cluster " + clusterName);

    d2Client = new D2ClientBuilder().setZkHosts(clusterWrapper.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    metricsRepository = new MetricsRepository();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "changelogConsumer", dataProviderClass = DataProviderUtils.class, priority = 3)
  public void testVeniceChangelogConsumer(int consumerCount) throws Exception {
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = setUpStore(storeName);
    Map<String, String> samzaConfig = getSamzaProducerConfig(childDatacenters, 0, storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    try (PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer)
            .setMockTime(testMockTime)
            .setRegionName(REGION_NAME)
            .build())) {
      Properties consumerProperties = new Properties();
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
              .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
          new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList =
          new ArrayList<>();
      for (int i = 0; i < consumerCount; i++) {
        bootstrappingVeniceChangelogConsumerList.add(
            veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(storeName, Integer.toString(i)));
      }

      try (VeniceSystemProducer veniceProducer =
          factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
        veniceProducer.start();
        // Run Samza job to send PUT and DELETE requests.
        runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
        // Produce a DELETE record with large timestamp
        sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      }

      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(clusterWrapper.getRandomRouterURL())
              .setMetricsRepository(metricsRepository))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
        });
      }

      // Wait for 10 seconds so bootstrap can load results from kafka
      Utils.sleep(10000);
      if (consumerCount == 1) {
        bootstrappingVeniceChangelogConsumerList.get(0).start().get();
      } else {
        for (int i = 0; i < consumerCount; i++) {
          bootstrappingVeniceChangelogConsumerList.get(i).start(Collections.singleton(i)).get();
        }
      }

      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap =
          new HashMap<>();
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList = new ArrayList<>();
      // 21 changes in near-line. 10 puts, 10 deletes, and 1 record with a producer timestamp
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(
            polledChangeEventsMap,
            polledChangeEventsList,
            bootstrappingVeniceChangelogConsumerList);
        // 21 events for near-line events, but the 10 deletes are not returned due to compaction.
        int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9 + consumerCount;
        Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);

        verifyPut(polledChangeEventsMap, 100, 110, 1);

        // Verify the 10 deletes were compacted away
        for (int i = 110; i < 120; i++) {
          String key = Integer.toString(i);
          PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
          Assert.assertNull(message);
        }
      });
      polledChangeEventsList.clear();
      polledChangeEventsMap.clear();

      runNearlineJobAndVerifyConsumption(
          120,
          storeName,
          1,
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);

      // Since nothing is produced, so no changed events generated.
      verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

      VeniceChangelogConsumer<Utf8, Utf8> afterImageChangelogConsumer =
          veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
      afterImageChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0, 1, 2))).get();

      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> changedEventList = new ArrayList<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumerToList(changedEventList, afterImageChangelogConsumer);
        Assert.assertEquals(changedEventList.size(), 141);
      });

      cleanUpStoreAndVerify(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2, priority = 3)
  public void testVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = setUpStore(storeName);
    Map<String, String> samzaConfig = getSamzaProducerConfig(childDatacenters, 0, storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    try (PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer)
            .setMockTime(testMockTime)
            .setRegionName(REGION_NAME)
            .build())) {
      Properties consumerProperties = new Properties();
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
              .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
              .setIsExperimentalClientEnabled(true)
              .setD2Client(d2Client);
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
          new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList =
          Collections.singletonList(
              veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(storeName, Integer.toString(0)));

      try (VeniceSystemProducer veniceProducer =
          factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
        veniceProducer.start();
        // Run Samza job to send PUT and DELETE requests.
        runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
        // Produce a DELETE record with large timestamp
        sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      }

      try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(clusterWrapper.getRandomRouterURL())
              .setMetricsRepository(metricsRepository))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
        });
      }

      bootstrappingVeniceChangelogConsumerList.get(0).start().get();

      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap =
          new HashMap<>();
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList = new ArrayList<>();
      // 21 changes in near-line. 10 puts, 10 deletes, and 1 record with a producer timestamp
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(
            polledChangeEventsMap,
            polledChangeEventsList,
            bootstrappingVeniceChangelogConsumerList);
        // 21 events for near-line events
        int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 21;
        Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
        verifyPut(polledChangeEventsMap, 100, 110, 1);
        verifyDelete(polledChangeEventsMap, 110, 120, 1);
      });
      polledChangeEventsList.clear();
      polledChangeEventsMap.clear();

      int startIndex = runNearlineJobAndVerifyConsumption(
          120,
          storeName,
          1,
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);

      // Since nothing is produced, so no changed events generated.
      verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

      // Create new version
      Properties props =
          TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
      TestWriteUtils.runPushJob("Run push job v2", props);

      clusterWrapper.useControllerClient(controllerClient -> {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2);
        });
      });

      // Change events should be from version 2 and 20 nearline events produced before
      runNearlineJobAndVerifyConsumption(
          startIndex,
          storeName,
          2,
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);

      cleanUpStoreAndVerify(storeName);
    }
  }

  private void pollChangeEventsFromChangeCaptureConsumerToList(
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      polledChangeEvents.add(pubSubMessage);
    }
  }

  private void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> keyToMessageMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledMessageList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    for (BootstrappingVeniceChangelogConsumer bootstrappingVeniceChangelogConsumer: bootstrappingVeniceChangelogConsumerList) {
      Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
          bootstrappingVeniceChangelogConsumer.poll(1000);
      for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
        String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
        keyToMessageMap.put(key, pubSubMessage);
      }
      polledMessageList.addAll(pubSubMessages);
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

  /**
   * @param storeName the name of the store
   * @return the path that's being used for the test
   */
  private String setUpStore(String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);

    try (ControllerClient controllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      TestUtils.assertCommand(controllerClient.updateStore(storeName, storeParms));
      TestWriteUtils.runPushJob("Run push job", props);
    }

    return inputDirPath;
  }

  /**
   * @return the end index
   */
  private int runNearlineJobAndVerifyConsumption(
      int startIndex,
      String storeName,
      int version,
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    Map<String, String> samzaConfig = getSamzaProducerConfig(childDatacenters, 0, storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // Half puts and half deletes
    int recordsToProduce = 20;
    int numPuts = recordsToProduce / 2;
    int numDeletes = recordsToProduce / 2;

    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, numPuts, numDeletes, startIndex);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(startIndex + numPuts - 1)).get());
      });
    }

    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      Assert.assertEquals(polledChangeEventsMap.size(), recordsToProduce);

      verifyPut(polledChangeEventsMap, startIndex, startIndex + numPuts, version);
      verifyDelete(polledChangeEventsMap, startIndex + numPuts, startIndex + numDeletes, version);
    });
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    return startIndex + recordsToProduce;
  }

  private void verifyPut(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
      ChangeEvent<Utf8> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());
      Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
    }
  }

  private void verifyDelete(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
      ChangeEvent<Utf8> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());
      Assert.assertNull(changeEvent.getCurrentValue());
    }
  }

  private void verifyNoRecordsProduced(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    pollChangeEventsFromChangeCaptureConsumer(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);
    Assert.assertEquals(polledChangeEventsList.size(), 0);
  }

  private void cleanUpStoreAndVerify(String storeName) {
    parentControllerClient.disableAndDeleteStore(storeName);
    // Verify that topics and store is cleaned up
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      clusterWrapper.useControllerClient(controllerClient -> {
        MultiStoreTopicsResponse storeTopicsResponse = controllerClient.getDeletableStoreTopics();
        Assert.assertFalse(storeTopicsResponse.isError());
        Assert.assertEquals(storeTopicsResponse.getTopics().size(), 0);
      });
    });
  }
}
