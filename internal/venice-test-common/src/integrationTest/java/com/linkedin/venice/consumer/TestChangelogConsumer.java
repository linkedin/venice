package com.linkedin.venice.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LINGER_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithLogicalTimestamp;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V10_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V11_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V4_SCHEMA;
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

import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestChangelogConsumer {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;

  List<Schema> SCHEMA_HISTORY = new ArrayList<Schema>() {
    {
      add(NAME_RECORD_V1_SCHEMA);
      add(NAME_RECORD_V2_SCHEMA);
      add(NAME_RECORD_V3_SCHEMA);
      add(NAME_RECORD_V4_SCHEMA);
      add(NAME_RECORD_V5_SCHEMA);
      add(NAME_RECORD_V6_SCHEMA);
      add(NAME_RECORD_V7_SCHEMA);
      add(NAME_RECORD_V8_SCHEMA);
      add(NAME_RECORD_V9_SCHEMA);
      add(NAME_RECORD_V10_SCHEMA);
      add(NAME_RECORD_V11_SCHEMA);
    }
  };

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAAWCParallelProcessingEnabled());
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        Optional.empty(),
        Optional.empty(),
        Optional.of(serverProperties),
        false);

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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT * 3, priority = 3)
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
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
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
    MetricsRepository metricsRepository = new MetricsRepository();
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

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Map<String, ViewConfig> viewConfigMap = setupControllerClient.getStore(storeName).getStore().getViewConfigs();
      Assert.assertEquals(viewConfigMap.size(), 2);
      Assert.assertEquals(viewConfigMap.get("testView").getViewClassName(), TestView.class.getCanonicalName());
      Assert.assertEquals(
          viewConfigMap.get("changeCaptureView").getViewClassName(),
          ChangeCaptureView.class.getCanonicalName());
      Assert.assertEquals(viewConfigMap.get("changeCaptureView").getViewParameters().size(), 1);
    });

    // Write Records to the store for version v1, the push job will contain 100 records.
    TestWriteUtils.runPushJob("Run push job", props);

    // Write Records from nearline
    Map<String, String> samzaConfig = getSamzaProducerConfig(childDatacenters, 0, storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    TestMockTime testMockTime = new TestMockTime();
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    try (PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer)
            .setMockTime(testMockTime)
            .setRegionName("local-pubsub")
            .build())) {
      Properties consumerProperties = new Properties();
      String localKafkaUrl = localKafka.getAddress();
      consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
      ChangelogClientConfig globalChangelogClientConfig = new ChangelogClientConfig().setViewName("changeCaptureView")
          .setConsumerProperties(consumerProperties)
          .setControllerD2ServiceName(D2_SERVICE_NAME)
          .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
          .setLocalD2ZkHosts(localZkServer.getAddress())
          .setControllerRequestRetryCount(3);
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

      // Let's consume those 100 records off of version 1
      Map<String, Utf8> versionTopicEvents = new HashMap<>();
      pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
      Assert.assertEquals(versionTopicEvents.size(), 100);

      VeniceChangelogConsumer<Utf8, Utf8> veniceChangelogConsumer =
          veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
      veniceChangelogConsumer.subscribeAll().get();
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

      // Validate change events for version 1. 100 records exist in version 1.
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents = new HashMap<>();
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> allChangeEvents = new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 100);
      });

      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();

      // 21 changes in nearline. 10 puts, 10 deletes, and 1 record with a producer timestamp
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
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
        pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        Assert.assertEquals(versionTopicEvents.size(), 21);
      });

      /**
       * Now we have store version v2.
       */

      // run repush. Repush will reapply all existing events to the new store and trim all events from the RT
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      TestWriteUtils.runPushJob("Run repush job", props);
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
      try (VeniceSystemProducer veniceProducer =
          factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
        veniceProducer.start();
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

      // Validate changed events for version 2.
      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();
      // As records keys from VPJ start from 1, real-time produced records' key starts from 0, the message with key as 0
      // is new message.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        // poll enough to get through the empty push and the topic jump to RT.
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        String deleteWithRmdKey = Integer.toString(deleteWithRmdKeyIndex);
        String persistWithRmdKey = Integer.toString(deleteWithRmdKeyIndex + 1);
        Assert.assertNull(polledChangeEvents.get(deleteWithRmdKey));
        Assert.assertNotNull(polledChangeEvents.get(persistWithRmdKey));
        Assert.assertEquals(
            polledChangeEvents.get(persistWithRmdKey).getValue().getCurrentValue().toString(),
            "stream_" + persistWithRmdKey);
      });

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
      try (VeniceSystemProducer veniceProducer =
          factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
        veniceProducer.start();
        // run samza to stream put and delete
        runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
      }
      // Validate changed events for version 3.

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        // Filter previous 21 messages.
        Assert.assertEquals(polledChangeEvents.size(), 1);
      });

      // Drain the remaining events on version 3 and verify that we got everything. We don't verify the count
      // because at this stage, the total events which will get polled will be determined by how far back the rewind
      // managed to get (and test run duration might be variable)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        for (int i = 20; i < 40; i++) {
          String key = Integer.toString(i);
          ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key).getValue();
          Assert.assertNotNull(changeEvent);
          Assert.assertNull(changeEvent.getPreviousValue());
          if (i < 30) {
            Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
          } else {
            Assert.assertNull(changeEvent.getCurrentValue());
          }
        }
      });

      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();

      // This should get everything submitted to the CC topic on this version since the timestamp is before anything got
      // transmitted
      veniceChangelogConsumer.seekToTimestamp(timestamp);

      // test pause and resume
      veniceChangelogConsumer.pause();
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 0);
      });
      veniceChangelogConsumer.resume();

      // This should get everything submitted to the CC topic on this version since the timestamp is before anything got
      // transmitted
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 42);
      });
      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();

      /**
       * Now we have store version v4.
       */
      // enable repush ttl
      props.setProperty(REPUSH_TTL_ENABLE, "true");
      TestWriteUtils.runPushJob("Run repush job with TTL", props);
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

      // Since nothing is produced, so no changed events generated.
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer2(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 0);
      });

      // Seek to the beginning of the push
      veniceChangelogConsumer.seekToBeginningOfPush().join();
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 30);
      });

      // Save a checkpoint and clear the map
      Set<VeniceChangeCoordinate> checkpointSet = new HashSet<>();
      checkpointSet.add(polledChangeEvents.get(Integer.toString(20)).getOffset());
      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();

      // Seek to a bogus checkpoint
      PubSubPosition badPubSubPosition = new ApacheKafkaOffsetPosition(1337L);
      VeniceChangeCoordinate badCoordinate =
          new MockVeniceChangeCoordinate(storeName + "_v777777", badPubSubPosition, 0);
      Set<VeniceChangeCoordinate> badCheckpointSet = new HashSet<>();
      badCheckpointSet.add(badCoordinate);

      Assert.assertThrows(() -> veniceChangelogConsumer.seekToCheckpoint(badCheckpointSet).get());

      // Seek the consumer by checkpoint
      veniceChangelogConsumer.seekToCheckpoint(checkpointSet).join();
      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();

      // Poll Change events again, verify we get everything
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        // Repush with TTL will include delete events in the topic
        Assert.assertEquals(polledChangeEvents.size(), 16);
      });
      allChangeEvents.putAll(polledChangeEvents);
      polledChangeEvents.clear();
      Assert.assertEquals(allChangeEvents.size(), 121);

      // Seek the consumer to the beginning of push (since the latest is version 4 with no nearline writes, shouldn't
      // have any new writes)
      // veniceAfterImageConsumer.seekToEndOfPush().join();
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 0);
      });

      // Also should be nothing on the tail
      // veniceAfterImageConsumer.seekToTail().join();
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 0);
      });

      // This should get everything submitted to the CC topic on this version (version 4 doesn't have anything)
      veniceChangelogConsumer.seekToTimestamp(timestamp);
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
        Assert.assertEquals(polledChangeEvents.size(), 0);
      });

      versionTopicEvents.clear();
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        // At this point, the consumer should have auto tracked to version 4, and since we didn't apply any nearline
        // writes to version 4, there should be no events to consume at this point
        Assert.assertEquals(versionTopicEvents.size(), 0);
      });

      versionTopicConsumer.seekToEndOfPush().get();
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        // Again, no events to consume here.
        Assert.assertEquals(versionTopicEvents.size(), 0);
      });

      versionTopicConsumer.seekToBeginningOfPush().get();
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        pollAfterImageEventsFromChangeCaptureConsumer(versionTopicEvents, versionTopicConsumer);
        // Reconsuming the events from the version topic, which at this point should just contain the same 16
        // events we consumed with the before/after image consumer earlier.
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
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSpecificRecordBootstrappingVeniceChangelogConsumer() throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository = new MetricsRepository();
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms));
    // Registering real data schema as schema v2.
    setupControllerClient.retryableRequest(
        5,
        controllerClient1 -> setupControllerClient.addValueSchema(storeName, NAME_RECORD_V1_SCHEMA.toString()));

    TestWriteUtils.runPushJob("Run push job", props);

    TestMockTime testMockTime = new TestMockTime();
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    try (PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer)
            .setMockTime(testMockTime)
            .setRegionName("local-pubsub")
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
        pollChangeEventsFromSpecificBootstrappingChangeCaptureConsumer(
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

      VeniceSystemFactory factory = new VeniceSystemFactory();
      try (VeniceSystemProducer veniceProducer = factory
          .getClosableProducer("venice", new MapConfig(getSamzaProducerConfig(childDatacenters, 0, storeName)), null)) {
        veniceProducer.start();
        // Run Samza job to send PUT and DELETE requests.
        sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
        sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecordV2, null);
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromSpecificBootstrappingChangeCaptureConsumer(
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
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSpecificRecordVeniceChangelogConsumer() throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository = new MetricsRepository();
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms));
    // Registering real data schema as schema v2.

    for (Schema schema: SCHEMA_HISTORY) {
      setupControllerClient
          .retryableRequest(5, controllerClient1 -> setupControllerClient.addValueSchema(storeName, schema.toString()));
    }

    TestWriteUtils.runPushJob("Run push job", props);
    TestMockTime testMockTime = new TestMockTime();
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    try (PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer)
            .setMockTime(testMockTime)
            .setRegionName("local-pubsub")
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
      polledChangeEventsList.clear();
      polledChangeEventsMap.clear();

      GenericRecord genericRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      genericRecord.put("firstName", "Venice");
      genericRecord.put("lastName", "Italy");

      GenericRecord genericRecordV2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      genericRecordV2.put("firstName", "Barcelona");
      genericRecordV2.put("lastName", "Spain");

      VeniceSystemFactory factory = new VeniceSystemFactory();
      try (VeniceSystemProducer veniceProducer = factory
          .getClosableProducer("venice", new MapConfig(getSamzaProducerConfig(childDatacenters, 0, storeName)), null)) {
        veniceProducer.start();
        // Run Samza job to send PUT and DELETE requests.
        sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
        sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecordV2, null);
      }

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromSpecificChangeCaptureConsumer(
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

  private void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
  }

  private void pollChangeEventsFromSpecificBootstrappingChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEvents,
      List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      BootstrappingVeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  private void pollChangeEventsFromSpecificChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEvents,
      List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  private void pollChangeEventsFromChangeCaptureConsumer2(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    pubSubMessages.addAll(veniceChangelogConsumer.poll(1000));
    pubSubMessages.addAll(veniceChangelogConsumer.poll(1000));
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
  }

  private int pollAfterImageEventsFromChangeCaptureConsumer(
      Map<String, Utf8> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    int polledMessagesNum = 0;
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      Utf8 afterImageEvent = pubSubMessage.getValue().getCurrentValue();
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, afterImageEvent);
      polledMessagesNum++;
    }
    return polledMessagesNum;
  }

}
