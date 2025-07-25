package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LINGER_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
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
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogValue;
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
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.views.MaterializedView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestChangelogConsumer {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;
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

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()),
        "Failed to configure active-active replication for cluster " + clusterName);
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

  // This is a beefier test, so giving it a bit more time
  @Test(timeOut = TEST_TIMEOUT * 3, priority = 3)
  public void testVersionSwapInALoop() throws Exception {
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
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

    // This is a dumb check that we're doing just to make static analysis happy
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(setupControllerClient.getStore(storeName).getStore().getCurrentVersion(), 0));

    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());

    // Write Records to the store for version v1, the push job will contain 100 records.
    IntegrationTestPushUtils.runVPJ(props);

    // Write Records from nearline
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    ChangelogClientConfig globalAfterImageClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(3L)
            .setIsBeforeImageView(true);

    VeniceChangelogConsumerClientFactory veniceAfterImageConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalAfterImageClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> versionTopicConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    Assert.assertTrue(versionTopicConsumer instanceof VeniceAfterImageConsumerImpl);
    versionTopicConsumer.subscribeAll().get();

    Map<String, Utf8> versionTopicEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
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
          5,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), expectedVersion));

      // poll data from version topic
      TestUtils.waitForNonDeterministicAssertion(100, TimeUnit.SECONDS, true, () -> {
        IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        Assert.assertEquals(versionTopicEvents.size(), 21);
      });
      versionTopicEvents.clear();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testAAIngestionWithStoreView() throws Exception {
    // Set up the store
    Long timestamp = System.currentTimeMillis();
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    Map<String, String> viewConfig = new HashMap<>();
    props.put(KAFKA_LINGER_MS, 0);
    viewConfig.put(
        "testViewWrong",
        "{\"viewClassName\" : \"" + TestView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");
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
    UpdateStoreQueryParams storeParams1 = new UpdateStoreQueryParams().setStoreViews(viewConfig);
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParams1));
    UpdateStoreQueryParams storeParams2 =
        new UpdateStoreQueryParams().setViewName("testViewWrong").setDisableStoreView();
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParams2));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Map<String, ViewConfig> viewConfigMap = setupControllerClient.getStore(storeName).getStore().getViewConfigs();
      Assert.assertTrue(viewConfigMap.isEmpty());
    });

    UpdateStoreQueryParams storeParams3 = new UpdateStoreQueryParams().setViewName("changeCaptureView")
        .setViewClassName(ChangeCaptureView.class.getCanonicalName())
        .setViewClassParams(Collections.singletonMap("kafka.linger.ms", "0"));
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParams3));

    UpdateStoreQueryParams storeParams4 =
        new UpdateStoreQueryParams().setViewName("testView").setViewClassName(TestView.class.getCanonicalName());
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParams4));

    UpdateStoreQueryParams storeParams5 = new UpdateStoreQueryParams().setViewName("materializedView")
        .setViewClassName(MaterializedView.class.getCanonicalName())
        .setViewClassParams(
            Collections.singletonMap(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), "1"));
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParams5));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Map<String, ViewConfig> viewConfigMap = setupControllerClient.getStore(storeName).getStore().getViewConfigs();
      Assert.assertEquals(viewConfigMap.size(), 3);
      Assert.assertEquals(viewConfigMap.get("testView").getViewClassName(), TestView.class.getCanonicalName());
      Assert.assertEquals(
          viewConfigMap.get("changeCaptureView").getViewClassName(),
          ChangeCaptureView.class.getCanonicalName());
      Assert.assertEquals(viewConfigMap.get("changeCaptureView").getViewParameters().size(), 1);
      Assert.assertEquals(
          viewConfigMap.get("materializedView").getViewClassName(),
          MaterializedView.class.getCanonicalName());
      Assert.assertEquals(
          viewConfigMap.get("materializedView")
              .getViewParameters()
              .get(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name()),
          "1");
    });

    // Write Records to the store for version v1, the push job will contain 100 records.
    IntegrationTestPushUtils.runVPJ(props);

    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    ChangelogClientConfig globalChangelogClientConfig = new ChangelogClientConfig().setViewName("changeCaptureView")
        .setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(3L)
        .setIsBeforeImageView(true);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    ChangelogClientConfig globalAfterImageClientConfig =
        ChangelogClientConfig.cloneConfig(globalChangelogClientConfig).setViewName("");
    VeniceChangelogConsumerClientFactory veniceAfterImageConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalAfterImageClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> versionTopicConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    Assert.assertTrue(versionTopicConsumer instanceof VeniceAfterImageConsumerImpl);
    versionTopicConsumer.subscribeAll().get();

    ChangelogClientConfig viewChangeLogClientConfig = new ChangelogClientConfig().setViewName("materializedView")
        .setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setVersionSwapDetectionIntervalTimeInSeconds(3L)
        .setControllerRequestRetryCount(3)
        .setBootstrapFileSystemPath(getTempDataDirectory().getAbsolutePath());
    VeniceChangelogConsumerClientFactory veniceViewChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(viewChangeLogClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> viewTopicConsumer =
        veniceViewChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    Assert.assertTrue(viewTopicConsumer instanceof VeniceAfterImageConsumerImpl);
    viewTopicConsumer.subscribeAll().get();

    // Let's consume those 100 records off of version 1
    Map<String, Utf8> versionTopicEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 100, "Version topic consumer should consume 100 records.");
    });

    Map<String, Utf8> viewTopicEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(viewTopicEvents, viewTopicConsumer);
      Assert.assertEquals(viewTopicEvents.size(), 100, "View topic consumer should consume 100 records.");
    });

    VeniceChangelogConsumer<Utf8, Utf8> veniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    veniceChangelogConsumer.subscribeAll().get();

    // Validate change events for version 1. 100 records exist in version 1.
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents = new HashMap<>();
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> allChangeEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 100, "Venice change log consumer should consume 100 records.");
    });

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
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

    allChangeEvents.putAll(polledChangeEvents);
    polledChangeEvents.clear();

    // 21 changes in nearline. 10 puts, 10 deletes, and 1 record with a producer timestamp
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      // 21 events for nearline events
      Assert.assertEquals(polledChangeEvents.size(), 21);
      for (int i = 100; i < 110; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key).getValue();
        Assert.assertNotNull(changeEvent);
        if (i != 100) {
          Assert.assertNull(changeEvent.getPreviousValue());
        } else {
          Assert.assertTrue(changeEvent.getPreviousValue().toString().contains(key));
        }
        Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
      }
      for (int i = 110; i < 120; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key).getValue();
        Assert.assertNotNull(changeEvent);
        Assert.assertNull(changeEvent.getPreviousValue()); // schema id is negative, so we did not parse.
        Assert.assertNull(changeEvent.getCurrentValue());
      }
    });

    versionTopicEvents.clear();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 21);
    });

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 21);
    });

    allChangeEvents.putAll(polledChangeEvents);
    versionTopicEvents.clear();
    polledChangeEvents.clear();

    /**
     * Now we have store version v2.
     */

    // run repush. Repush will reapply all existing events to the new store and trim all events from the RT
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    // intentionally stop re-consuming from RT so stale records don't affect the testing results
    props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    IntegrationTestPushUtils.runVPJ(props);
    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));
    clusterWrapper.refreshAllRouterMetaData();
    // Validate repush from version 2
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // test single get
        for (int i = 100; i < 110; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertEquals(value.toString(), "stream_" + i);
        }
        // test deletes
        for (int i = 110; i < 120; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNull(value);
        }
        // test old data
        for (int i = 20; i < 100; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertTrue(value.toString().contains(String.valueOf(i).substring(0, 0)));
        }
      });
    }

    // we shouldn't pull anything on this version if filtering is working correctly
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 0);
    });

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // poll a few times in a row to make sure version jump happens
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Produce a new PUT with smaller logical timestamp, it is expected to be ignored as there was a DELETE with
      // larger timestamp
      sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 2, false);
      // Produce another record to the same partition to make sure the above PUT is processed during validation stage.
      sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex + 1, 1, false);
    }
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(deleteWithRmdKeyIndex + 1)).get());
      });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
      });
    }

    /**
     * Now we have store version v3.
     */

    // run empty push to clean up batch data
    parentControllerClient.sendEmptyPushAndWait(storeName, "Run empty push job", 1000, 30 * Time.MS_PER_SECOND);
    // set up mocked time for Samza records so some records can be stale intentionally.
    List<Long> mockTimestampInMs = new LinkedList<>();
    Instant now = Instant.now();
    // always-valid record
    mockTimestampInMs.add(now.toEpochMilli());
    // always-stale records since ttl time is 360 sec
    Instant past = now.minus(1, ChronoUnit.HOURS);
    mockTimestampInMs.add(past.toEpochMilli());
    Time mockTime = new MockCircularTime(mockTimestampInMs);

    // We should only poll 1 record as we produced 1 that would have been applied in LWW
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 1);
    });

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // poll a few times in a row to make sure version jump happens
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 1);
    });

    // Write 20 records
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // run samza to stream put and delete
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
    }

    // test pause and resume
    veniceChangelogConsumer.pause();
    polledChangeEvents.clear();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });
    veniceChangelogConsumer.resume();

    allChangeEvents.putAll(polledChangeEvents);
    polledChangeEvents.clear();

    /**
     * Now we have store version v4.
     */
    // enable repush ttl
    props.setProperty(REPUSH_TTL_ENABLE, "true");
    IntegrationTestPushUtils.runVPJ(props);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 4));

    // Validate repush from version 4
    clusterWrapper.refreshAllRouterMetaData();
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      // test single get
      int validGet = 0, filteredGet = 0;
      for (int i = 20; i < 30; i++) {
        Object result = client.get(Integer.toString(i)).get();
        if (result == null) {
          filteredGet++;
        } else {
          validGet++;
        }
      }
      // Half records are valid, another half is not
      Assert.assertEquals(validGet, 5);
      Assert.assertEquals(filteredGet, 5);
      // test deletes
      for (int i = 30; i < 40; i++) {
        // not matter the DELETE is TTLed or not, the value should always be null
        Assert.assertNull(client.get(Integer.toString(i)).get());
      }
      // test old data - should be empty due to empty push
      for (int i = 40; i < 100; i++) {
        Assert.assertNull(client.get(Integer.toString(i)).get());
      }
    }

    polledChangeEvents.clear();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 20);
    });

    // Save a checkpoint and clear the map
    Set<VeniceChangeCoordinate> checkpointSet = new HashSet<>();
    checkpointSet.add(polledChangeEvents.get(Integer.toString(20)).getOffset());
    allChangeEvents.putAll(polledChangeEvents);
    polledChangeEvents.clear();

    // Seek to a bogus checkpoint
    PubSubPosition badPubSubPosition = ApacheKafkaOffsetPosition.of(1337L);
    VeniceChangeCoordinate badCoordinate = new MockVeniceChangeCoordinate(storeName + "_v777777", badPubSubPosition, 0);
    Set<VeniceChangeCoordinate> badCheckpointSet = new HashSet<>();
    badCheckpointSet.add(badCoordinate);

    Assert.assertThrows(() -> veniceChangelogConsumer.seekToCheckpoint(badCheckpointSet).get());

    // Seek the consumer by checkpoint
    veniceChangelogConsumer.seekToCheckpoint(checkpointSet).join();
    allChangeEvents.putAll(polledChangeEvents);
    polledChangeEvents.clear();
    // Poll Change events again, verify we get everything
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      // Repush with TTL will include delete events in the topic
      Assert.assertEquals(polledChangeEvents.size(), 7);
    });
    allChangeEvents.putAll(polledChangeEvents);
    polledChangeEvents.clear();
    Assert.assertEquals(allChangeEvents.size(), 120);

    // Should be nothing on the tail
    veniceChangelogConsumer.seekToTail().join();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });

    // This should get everything submitted to the CC topic on this version (version 4 doesn't have anything)
    veniceChangelogConsumer.seekToTimestamp(timestamp);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });

    versionTopicEvents.clear();
    TestUtils.waitForNonDeterministicAssertion(1000, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      // At this point, the consumer should have auto tracked to version 4, and since we didn't apply any nearline
      // writes to version 4, there should be no events to consume at this point
      Assert.assertEquals(versionTopicEvents.size(), 0);
    });

    // The repush should result in nothing getting placed on the RT, so this seek should put us at the tail of events
    versionTopicConsumer.seekToEndOfPush().get();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      // Again, no events to consume here.
      Assert.assertEquals(versionTopicEvents.size(), 0);
    });

    // Following repush with TTL, we played 20 events, 10 puts, 10 deletes. This should result in a VT with 10 events in
    // it
    versionTopicConsumer.seekToBeginningOfPush().get();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 30);
    });

    // Verify version swap count matches with version count - 1 (since we don't transmit from version 0 to version 1).
    // This will include messages for all partitions, so (4 version -1)*3 partitions=9 messages
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(TestView.getInstance().getVersionSwapCountForStore(storeName), 9));
    // Verify total updates match up (first 20 + next 20 should make 40, And then double it again as rewind updates
    // are
    // applied to a version)
    TestUtils.waitForNonDeterministicAssertion(
        8,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(TestView.getInstance().getRecordCountForStore(storeName), 85));
    parentControllerClient.disableAndDeleteStore(storeName);
    // Verify that topics and store is cleaned up
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      MultiStoreTopicsResponse storeTopicsResponse = childControllerClient.getDeletableStoreTopics();
      Assert.assertFalse(storeTopicsResponse.isError());
      Assert.assertEquals(storeTopicsResponse.getTopics().size(), 0);
    });
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSpecificRecordBootstrappingVeniceChangelogConsumer() throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
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
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms));
    // Registering real data schema as schema v2.
    setupControllerClient.retryableRequest(
        5,
        controllerClient1 -> setupControllerClient.addValueSchema(storeName, NAME_RECORD_V1_SCHEMA.toString()));

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
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(3L)
            .setSpecificValue(TestChangelogValue.class)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    BootstrappingVeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory
            .getBootstrappingChangelogConsumer(storeName, "0", TestChangelogValue.class);
    specificChangelogConsumer.start().get();

    Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromSpecificBootstrappingChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          specificChangelogConsumer);
      Assert.assertEquals(polledChangeEventsList.size(), 101);
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
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecordV2, null);
    }

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromSpecificBootstrappingChangeCaptureConsumer(
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
  public void testSpecificRecordVeniceChangelogConsumer() throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
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
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(3L)
            .setSpecificValue(TestChangelogValue.class)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0", TestChangelogValue.class);

    specificChangelogConsumer.subscribeAll().get();

    Assert.assertFalse(specificChangelogConsumer.isCaughtUp());

    Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
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
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecordV2, null);
    }

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
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
    IntegrationTestPushUtils.runVPJ(props);
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
            .setVersionSwapDetectionIntervalTimeInSeconds(180)
            .setUseRequestBasedMetadataRepository(true)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");

    changeLogConsumer.subscribeAll().get();
    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
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
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 101);
    });
    Assert.assertNotNull(polledChangeEventsMap.get(Integer.toString(10000)));
    parentControllerClient.disableAndDeleteStore(storeName);
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
