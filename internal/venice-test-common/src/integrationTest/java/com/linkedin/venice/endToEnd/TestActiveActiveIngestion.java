package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.generateInput;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TIMESTAMP_FIELD_PROP;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
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
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@PubSubAgnosticTest
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
  private ControllerClient parentControllerClient;

  protected boolean isLevel0CompactionTuningForReadWriteLeaderEnabled() {
    return false;
  }

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  protected boolean whetherToEnableNearlineProducerThroughputOptimizationInServer() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    serializer = new AvroSerializer(STRING_SCHEMA);
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, true);
    serverProperties.put(
        ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED,
        isLevel0CompactionTuningForReadWriteLeaderEnabled());
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    serverProperties.put(
        SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
        KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION.name());
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAAWCParallelProcessingEnabled());
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 20);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLeaderLagWithIgnoredData() throws Exception {
    // We want to verify in this test if pushes will go through even if the tail end of the RT is full of data which we
    // drop
    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    // create an active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(360)
        .setHybridOffsetLagThreshold(0)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(1);
    if (whetherToEnableNearlineProducerThroughputOptimizationInServer()) {
      storeParms.setNearlineProducerCountPerWriter(2);
      storeParms.setNearlineProducerCompressionEnabled(false);
    }
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    IntegrationTestPushUtils.runVPJ(props);

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

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Run Samza job to send PUT and DELETE requests with a mix of records that will land and some which won't.
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 0, 20);

      // Run it again but only add records to the end of the RT which will fail DCR. These records will try to delete
      // everything we just wrote
      runSamzaStreamJob(veniceProducer, storeName, mockPastime, 0, 10, 20);
    }

    // Now see if a push will succeed. There are 10 events at the front of the queue which are valid and another 10
    // which are ignored.
    // Our rewind is set to 0 messages as acceptable lag. Near the end of the RT there will be a version swap message,
    // but a lag of 0 is only achievable if the ingestion heartbeat feature is working.
    parentControllerClient.sendEmptyPushAndWait(storeName, "Run empty push job", 1000, 30 * Time.MS_PER_SECOND);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Append a MARKER record 222 to the end of the RT topic to act as an anchor.
      // This helps determine when all prior records have been processed (i.e., accepted or dropped in DCR),
      // ensuring deterministic behavior for subsequent verification steps.
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 1, 0, 222);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {

      // Wait until the MARKER record 222 is available, indicating all prior records have been handled.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(222)).get(), "Leader has not processed all records yet");
      });

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

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // Run Samza job to send PUT requests
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 0, 80);
    }

    // Alright. For our last trick, we'll try and push a version which has timestamps specified in the job from spark.
    long newTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1000);
    long oldTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1000);
    TestWriteUtils
        .writeSimpleAvroFileWithStringToStringAndTimestampSchema(inputDir, 100, "string2string.avro", oldTimestamp);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, String.valueOf(true));
    props.setProperty(TIMESTAMP_FIELD_PROP, "timestamp");
    IntegrationTestPushUtils.runVPJ(props);

    // All streaming writes should succeed
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      for (int i = 80; i < 90; i++) {
        Object result = client.get(Integer.toString(i)).get();
        if (result != null) {
          String value = result.toString();
          Assert.assertTrue(value.contains("stream_"), "Expected value to contain 'stream_' but got: " + value);
        }
      }
    }

    TestWriteUtils
        .writeSimpleAvroFileWithStringToStringAndTimestampSchema(inputDir, 100, "string2string.avro", newTimestamp);
    IntegrationTestPushUtils.runVPJ(props);

    // All of these writes should fail now
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      for (int i = 80; i < 90; i++) {
        Object result = client.get(Integer.toString(i)).get();
        if (result != null) {
          String value = result.toString();
          Assert.assertFalse(value.contains("stream_"), "Expected value to NOT contain 'stream_' but got: " + value);
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKIFRepushActiveActiveStore(boolean isChunkingEnabled) throws Exception {
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store-kif-repush");
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
    if (whetherToEnableNearlineProducerThroughputOptimizationInServer()) {
      storeParms.setNearlineProducerCountPerWriter(2);
      storeParms.setNearlineProducerCompressionEnabled(false);
    }
    MetricsRepository metricsRepository = new MetricsRepository();
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    IntegrationTestPushUtils.runVPJ(props);

    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;
    // Add a marker record to make sure we have consumed to all the RT record.
    int putWithRmdKeyIndex = 1001;

    int repeatedRecordIndex = 1000000;
    int repeatedRecordFrequency = 100;
    String lastExpectedValue = "stream_" + repeatedRecordIndex + "_" + (repeatedRecordFrequency - 1);

    // Enable concurrent producer
    try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils.getSamzaProducerForStream(
        multiRegionMultiClusterWrapper,
        0,
        storeName,
        new Pair<>(VeniceWriter.PRODUCER_THREAD_COUNT, "2"))) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 0);
      // Produce a DELETE record with large timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      // Produce a PUT record with large timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, putWithRmdKeyIndex, 1000, false);

      // Write same record multiple times with increasing logical timestamp and value
      for (int i = 0; i < repeatedRecordFrequency; i++) {
        sendStreamingRecord(
            veniceProducer,
            storeName,
            Integer.toString(repeatedRecordIndex),
            "stream_" + repeatedRecordIndex + "_" + i,
            1000L + i);
      }
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(putWithRmdKeyIndex)).get());
      });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // verify the value matches the last produced value
        Utf8 value = client.get(Integer.toString(repeatedRecordIndex)).get();
        Assert.assertNotNull(value, "Value for key: " + repeatedRecordIndex + " is null, but expected to be non-null");
        Assert.assertEquals(
            value.toString(),
            lastExpectedValue,
            "Value for key: " + repeatedRecordIndex + " does not match the expected value: " + lastExpectedValue);
      });
    }

    // run repush with targeted region push
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    props.setProperty(PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET, "false");
    props.setProperty(TARGETED_REGION_PUSH_ENABLED, "true");
    // intentionally stop re-consuming from RT so stale records don't affect the testing results
    props.setProperty(REWIND_TIME_IN_SECONDS_OVERRIDE, "0");
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
        for (int i = 0; i < 10; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value, "Value for key: " + key + " is null, but expected to be non-null");
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
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // verify the value matches the last produced value
        Utf8 value = client.get(Integer.toString(repeatedRecordIndex)).get();
        Assert.assertNotNull(value, "Value for key: " + repeatedRecordIndex + " is null, but expected to be non-null");
        Assert.assertEquals(
            value.toString(),
            lastExpectedValue,
            "Value for key: " + repeatedRecordIndex + " does not match the expected value: " + lastExpectedValue);
      });
    }
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
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

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
      // run samza to stream put and delete
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
    }

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
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testActiveActiveStoreRestart() throws Exception {
    // create a active-active enabled store and run batch push job
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
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
    if (whetherToEnableNearlineProducerThroughputOptimizationInServer()) {
      storeParms.setNearlineProducerCountPerWriter(2);
      storeParms.setNearlineProducerCompressionEnabled(false);
    }
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
    PubSubBrokerWrapper pubSubBrokerWrapper = clusterWrapper.getPubSubBrokerWrapper();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory veniceWriterFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchPushWithSeparateDrainer() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 10000);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store-batch-push");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms =
        new UpdateStoreQueryParams().setNativeReplicationEnabled(true).setPartitionCount(10);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    IntegrationTestPushUtils.runVPJ(props);
  }
}
