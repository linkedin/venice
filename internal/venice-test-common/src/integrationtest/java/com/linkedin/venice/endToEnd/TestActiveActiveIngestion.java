package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJob.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.generateInput;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
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
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveIngestion {
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private VeniceServerWrapper serverWrapper;
  private AvroSerializer serializer;

  @BeforeClass
  public void setUp() {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
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
        Optional.of(new VeniceProperties(serverProperties)),
        false);

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
  }

  @AfterClass
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  private void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(100);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
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
    int a = 0;
  }

  private int pollAfterImageEventsFromChangeCaptureConsumer(
      Map<String, Utf8> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    int polledMessagesNum = 0;
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, Long>> pubSubMessages = veniceChangelogConsumer.poll(100);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, Long> pubSubMessage: pubSubMessages) {
      Utf8 afterImageEvent = pubSubMessage.getValue().getCurrentValue();
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, afterImageEvent);
      polledMessagesNum++;
    }
    return polledMessagesNum;
  }

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class)
  public void testLeaderLagWithIgnoredData() throws Exception {
    // We want to verify in this test if pushes will go through if the tail end of the RT is full of data which we drop
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(360)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(1);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    TestWriteUtils.runPushJob("Run push job", props);

    Map<String, String> samzaConfig = getSamzaConfig(storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // set up mocked time for Samza records so some records can be stale intentionally.
    List<Long> mockTimestampInMs = new LinkedList<>();
    List<Long> mockTimestampInMsInThePast = new LinkedList<>();
    Instant now = Instant.now();
    // always-valid record
    mockTimestampInMs.add(now.toEpochMilli());
    mockTimestampInMsInThePast.add(now.toEpochMilli() - 10000L);
    // always-stale records since ttl time is 360 sec
    Instant past = now.minus(1, ChronoUnit.HOURS);
    mockTimestampInMs.add(past.toEpochMilli());
    mockTimestampInMsInThePast.add(past.toEpochMilli() - 10000L);
    Time mockTime = new MockCircularTime(mockTimestampInMs);
    Time mockPastime = new MockCircularTime(mockTimestampInMsInThePast);

    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests with a mix of records that will land and some which won't.
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 0, 20);

      // Run it again but only add records to the end of the RT which will fail DCR. These records will try to delete
      // everything we just wrote
      runSamzaStreamJob(veniceProducer, storeName, mockPastime, 0, 10, 20);

    }

    // Now see if a push will succeed. There are 20 events at the front of the queue which are valid and another 20
    // which are ignored.
    // our rewind is set to 8 messages as acceptable lag. If the push succeeds, it means we used the drop messages as
    // part of our calculation
    parentControllerClient.sendEmptyPushAndWait(storeName, "Run empty push job", 1000, 30 * Time.MS_PER_SECOND);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      // We sent a bunch of deletes to the keys we wrote previously that should have been dropped by DCR. Validate
      // they're still there.
      int validGet = 0;
      for (int i = 20; i < 30; i++) {
        Object result = client.get(Integer.toString(i)).get();
        if (result != null) {
          validGet++;
        }
      }
      // Half records are valid, another half is not
      Assert.assertEquals(validGet, 10);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKIFRepushActiveActiveStore(boolean isChunkingEnabled) throws Exception {
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(360)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(isChunkingEnabled)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(1);
    MetricsRepository metricsRepository = new MetricsRepository();
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    TestWriteUtils.runPushJob("Run push job", props);

    Map<String, String> samzaConfig = getSamzaConfig(storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 0);
      // Produce a DELETE record with large timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
      });
    }

    // run repush
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getKafka().getAddress());
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
        for (int i = 0; i < 10; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertEquals(value.toString(), "stream_" + i);
        }
        // test deletes
        for (int i = 10; i < 20; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNull(value);
        }
        // test old data
        for (int i = 20; i < 100; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertEquals(value.toString(), "test_name_" + i);
        }
      });
    }
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Produce a new PUT with smaller logical timestamp, it is expected to be ignored as there was a DELETE with
      // larger
      // timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 2, false);
      // Produce another record to the same partition to make sure the above PUT is processed during validation stage.
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex + 1, 1, false);
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
     * Test Repush with TTL
     */
    // run empty push to clean up batch data
    parentControllerClient.sendEmptyPushAndWait(storeName, "Run empty push job", 1000, 30 * Time.MS_PER_SECOND);

    // enable repush ttl
    props.setProperty(REPUSH_TTL_ENABLE, "true");

    // set up mocked time for Samza records so some records can be stale intentionally.
    List<Long> mockTimestampInMs = new LinkedList<>();
    Instant now = Instant.now();
    // always-valid record
    mockTimestampInMs.add(now.toEpochMilli());
    // always-stale records since ttl time is 360 sec
    Instant past = now.minus(1, ChronoUnit.HOURS);
    mockTimestampInMs.add(past.toEpochMilli());
    Time mockTime = new MockCircularTime(mockTimestampInMs);

    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // run samza to stream put and delete
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
    }

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
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testActiveActiveStoreRestart() throws Exception {
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(5)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    // Create a new version
    VersionCreationResponse versionCreationResponse;
    versionCreationResponse = TestUtils.assertCommand(
        parentControllerClient.requestTopicForWrites(
            storeName,
            1024 * 1024,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1));

    String topic = versionCreationResponse.getKafkaTopic();
    String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafkaUrl);
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

      /**
       * Restart storage node during batch ingestion.
       */
      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, true, 0, serializer);
      Set<Integer> restartPointSetForSortedInput = new HashSet<>();
      restartPointSetForSortedInput.add(346);
      restartPointSetForSortedInput.add(543);
      serverWrapper = clusterWrapper.getVeniceServers().get(0);
      int cur = 0;
      for (Map.Entry<byte[], byte[]> entry: sortedInputRecords.entrySet()) {
        if (restartPointSetForSortedInput.contains(++cur)) {
          // Restart server
          clusterWrapper.stopVeniceServer(serverWrapper.getPort());
          clusterWrapper.restartVeniceServer(serverWrapper.getPort());
        }
        veniceWriter.put(entry.getKey(), entry.getValue(), 1, null);
      }
      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
      try {
        return clusterWrapper.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(clusterWrapper.getClusterName(), topic)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED);
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testAAIngestionWithStoreView() throws Exception {
    Long timestamp = System.currentTimeMillis();
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    Map<String, String> viewConfig = new HashMap<>();
    viewConfig.put(
        "testView",
        "{\"viewClassName\" : \"" + TestView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");

    viewConfig.put(
        "changeCaptureView",
        "{\"viewClassName\" : \"" + ChangeCaptureView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
    MetricsRepository metricsRepository = new MetricsRepository();
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    storeParms.setStoreViews(viewConfig);
    // IntegrationTestPushUtils.updateStore(clusterName, props, storeParms);
    setupControllerClient
        .retryableRequest(5, controllerClient1 -> setupControllerClient.updateStore(storeName, storeParms));
    // controllerClient.updateStore(storeName, storeParms);
    TestWriteUtils.runPushJob("Run push job", props);
    Map<String, String> samzaConfig = getSamzaConfig(storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    TestMockTime testMockTime = new TestMockTime();
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer).setMockTime(testMockTime).build());
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

    VeniceChangelogConsumer<Utf8, Utf8> veniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    veniceChangelogConsumer.subscribeAll().get();
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
      // Produce a DELETE record with large timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
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
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 100);
    });

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
    // run repush
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getKafka().getAddress());
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
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Produce a new PUT with smaller logical timestamp, it is expected to be ignored as there was a DELETE with
      // larger
      // timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 2, false);
      // Produce another record to the same partition to make sure the above PUT is processed during validation stage.
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex + 1, 1, false);
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
    VeniceChangelogConsumer<Utf8, Utf8> veniceAfterImageConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    veniceAfterImageConsumer.subscribeAll().get();
    // Validate changed events for version 2.
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
     * Test Repush with TTL
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
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // run samza to stream put and delete
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
    }
    // Validate changed events for version 3.
    AtomicInteger totalPolledAfterImageMessages = new AtomicInteger();
    Map<String, Utf8> polledAfterImageEvents = new HashMap<>();
    Map<String, Utf8> totalPolledAfterImageEvents = new HashMap<>();

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      // Filter previous 21 messages.
      Assert.assertEquals(polledChangeEvents.size(), 1);
    });

    // Consume from beginning of the version that was current at time of consumer subscription (version 2) since
    // version 2
    // was a repush of 101 records (0-100) with streaming updates on 100-110 and deletes on 110-119, then we expect
    // a grand total of 119 records in this version. We'll consume up to EOP

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      totalPolledAfterImageMessages
          .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
      Assert.assertEquals(polledAfterImageEvents.size(), 119);
      totalPolledAfterImageEvents.putAll(polledAfterImageEvents);
      polledAfterImageEvents.clear();
    });

    // We'll have consumed everything on version
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      totalPolledAfterImageMessages
          .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
      Assert.assertEquals(polledAfterImageEvents.size(), 0);
      totalPolledAfterImageEvents.putAll(polledAfterImageEvents);
      polledAfterImageEvents.clear();
    });

    // After image consumer consumed 3 different topics: v2, v2_cc and v3_cc.
    // The total messages: 102 (v2 repush from v1, key: 0-100, 1000) + 1 (v2_cc, key: 1001) + 42 (v3_cc, key: 0-39,
    // 1000, 1001) - 22 (filtered from v3_cc, key: 0-19, 1000 and 1001 as they were read already.)
    Assert.assertEquals(totalPolledAfterImageMessages.get(), 148);

    for (int i = 1; i < 100; i++) {
      String key = Integer.toString(i);
      Utf8 afterImageValue = totalPolledAfterImageEvents.get(key);
      if (i < 20) {
        Assert.assertNotNull(afterImageValue);
        Assert.assertEquals(afterImageValue.toString(), "test_name_" + i);
      } else if (i < 40 && i >= 30) {
        // Deleted
        Assert.assertNull(afterImageValue);
      } else {
        Assert.assertTrue(afterImageValue.toString().contains(String.valueOf(i).substring(0, 0)));
      }
    }

    // Drain the remaining events on version 3 and verify that we got everything
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 23);
      for (int i = 20; i < 40; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key).getValue();
        Assert.assertNotNull(changeEvent);
        Assert.assertNull(changeEvent.getPreviousValue());
        if (i >= 20 && i < 30) {
          Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
        } else {
          Assert.assertNull(changeEvent.getCurrentValue());
        }
      }
    });

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
    polledChangeEvents.clear();

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
      Assert.assertEquals(polledChangeEvents.size(), 15);
    });

    // Save a checkpoint and clear the map
    Set<VeniceChangeCoordinate> checkpointSet = new HashSet<>();
    checkpointSet.add(polledChangeEvents.get(Integer.toString(20)).getOffset());
    polledChangeEvents.clear();

    // Seek the consumer by checkpoint
    veniceChangelogConsumer.seekToCheckpoint(checkpointSet).join();
    polledChangeEvents.clear();

    // Poll Change events again, verify we get everything
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 8);
    });
    polledChangeEvents.clear();

    // Seek the consumer to the beginning of push (since the latest is version 4 with no nearline writes, shouldn't have
    // any new writes)
    veniceAfterImageConsumer.seekToEndOfPush().join();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });

    // Also should be nothing on the tail
    veniceAfterImageConsumer.seekToTail().join();
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
        5,
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

  private void produceRecordWithLogicalTimestamp(
      VeniceSystemProducer veniceProducer,
      String storeName,
      int index,
      long logicalTimestamp,
      boolean isDeleteOperation) {
    if (isDeleteOperation) {
      sendStreamingDeleteRecord(veniceProducer, storeName, Integer.toString(index), logicalTimestamp);
    } else {
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(index), "stream_" + index, logicalTimestamp);
    }
  }

  private Map<String, String> getSamzaConfig(String storeName) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
    samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDatacenters.get(0).getZkServerWrapper().getAddress());
    samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "dfd"); // parentController.getKafkaZkAddress());
    samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
    samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    return samzaConfig;
  }
}
