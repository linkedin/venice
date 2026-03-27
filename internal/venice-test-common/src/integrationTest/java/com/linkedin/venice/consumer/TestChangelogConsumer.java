package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.KAFKA_LINGER_MS;
import static com.linkedin.venice.integration.utils.IntegrationTestUtils.pollChangeEventsFromSpecificChangeCaptureConsumer;
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

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
  static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  static final List<Schema> SCHEMA_HISTORY = Arrays.asList(
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

  private ChangelogConsumerTestFixture fixture;

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    fixture = new ChangelogConsumerTestFixture(isAAWCParallelProcessingEnabled());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    fixture.cleanupAfterTest();
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testDisabledStoreVeniceChangelogConsumer() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
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

    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(IntegrationTestPushUtils.getD2Client(fixture.getLocalZkServer().getAddress()))
        .setSpecificValue(TestChangelogValue.class)
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0", TestChangelogValue.class);
    fixture.addCloseable(specificChangelogConsumer);

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
      Assert.assertTrue(fixture.getChildControllerClientRegion0().getStore(storeName).getStore().isEnableStoreReads());
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
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    props.put(KAFKA_LINGER_MS, 0);
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());

    // This is a dumb check that we're doing just to make static analysis happy
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(setupControllerClient.getStore(storeName).getStore().getCurrentVersion(), 0));

    // Write Records to the store for version v1, the push job will contain 100 records.
    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());

    // Write Records from nearline
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    ChangelogClientConfig globalAfterImageClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(IntegrationTestPushUtils.getD2Client(fixture.getLocalZkServer().getAddress()));

    VeniceChangelogConsumerClientFactory veniceAfterImageConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalAfterImageClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> versionTopicConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    fixture.addCloseable(versionTopicConsumer);
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
      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils
          .getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
        // Run Samza job to send PUT and DELETE requests.
        runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
        // Produce a DELETE record with large timestamp
        sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      }

      // run push job and wait for version to be active
      int expectedVersion = i + 2;
      IntegrationTestPushUtils.runVPJ(props, expectedVersion, fixture.getChildControllerClientRegion0());

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
    ControllerClient childControllerClient = new ControllerClient(
        fixture.getClusterName(),
        fixture.getChildDatacenters().get(0).getControllerConnectString());
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V2_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
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

    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    consumerProperties.put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true);
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(IntegrationTestPushUtils.getD2Client(fixture.getLocalZkServer().getAddress()))
        .setSpecificValue(TestChangelogValue.class)
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, TestChangelogValue> specificChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0", TestChangelogValue.class);
    fixture.addCloseable(specificChangelogConsumer);

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
        IntegrationTestPushUtils.getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
      // Run Samza job to send PUT and DELETE requests.
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
      // Ensure each operation gets a unique timestamp for deterministic LWW ordering
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
    fixture.getParentControllerClient().disableAndDeleteStore(storeName);
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
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    // setVersionSwapDetectionIntervalTimeInSeconds also controls native metadata repository refresh interval, setting
    // it to a large value to reproduce the scenario where the newly added schema is yet to be fetched.
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(fixture.getD2Client())
        .setVersionSwapDetectionIntervalTimeInSeconds(180)
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    fixture.addCloseable(changeLogConsumer);

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
        IntegrationTestPushUtils.getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
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
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(fixture.getD2Client())
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
        .setIsNewStatelessClientEnabled(true);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    fixture.addCloseable(changeLogConsumer);

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
        IntegrationTestPushUtils.getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
      // Run Samza job to send the new write with schema v2.
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(10000), genericRecord, null);
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 101);
    });
    Assert.assertNotNull(polledChangeEventsMap.get(Integer.toString(10000)));

    // Register a third schema that extends V2 with a nullable enum field. This exercises the
    // generateSupersetSchema ENUM path end-to-end through the changelog consumer pipeline.
    Schema nameRecordWithEnumSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"nameRecord\",\"namespace\":\"example.avro\"," + "\"fields\":["
            + "{\"name\":\"firstName\",\"type\":\"string\",\"default\":\"\"},"
            + "{\"name\":\"lastName\",\"type\":\"string\",\"default\":\"\"},"
            + "{\"name\":\"age\",\"type\":\"int\",\"default\":-1}," + "{\"name\":\"status\","
            + "\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"MemberStatus\","
            + "\"symbols\":[\"ACTIVE\",\"INACTIVE\"]}]," + "\"default\":null}" + "]}");
    TestUtils
        .assertCommand(
            setupControllerClient.retryableRequest(
                5,
                controllerClient -> setupControllerClient
                    .addValueSchema(storeName, nameRecordWithEnumSchema.toString())),
            "Failed to add enum schema to store " + storeName);

    // Write a record using the enum field.
    Schema statusSchema = nameRecordWithEnumSchema.getField("status").schema().getTypes().get(1);
    GenericRecord recordWithEnum = new GenericData.Record(nameRecordWithEnumSchema);
    recordWithEnum.put("firstName", "Venice");
    recordWithEnum.put("lastName", "Italy");
    recordWithEnum.put("age", 30);
    recordWithEnum.put("status", new GenericData.EnumSymbol(statusSchema, "ACTIVE"));
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(20000), recordWithEnum, null);
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      IntegrationTestUtils.pollChangeEventsFromChangeCaptureConsumer(polledChangeEventsMap, changeLogConsumer);
      Assert.assertEquals(polledChangeEventsMap.size(), 102);
    });
    Assert.assertNotNull(polledChangeEventsMap.get(Integer.toString(20000)));
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testChangeLogConsumerSequenceId() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = NAME_RECORD_V1_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
    IntegrationTestPushUtils.runVPJ(props, 1, fixture.getChildControllerClientRegion0());
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(fixture.getD2Client())
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    // Capture timestamp before consumer initialization to validate sequence IDs
    long initTimestampNs = Utils.getCurrentTimeInNanosForSeeding();
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    fixture.addCloseable(changeLogConsumer);

    changeLogConsumer.subscribeAll().get();
    final List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    // Short poll timeout since the outer retry loop handles waiting for data to arrive
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      pubSubMessages.addAll(changeLogConsumer.poll(5));
      Assert.assertEquals(pubSubMessages.size(), 100);
    });

    // Verify sequence IDs are greater than initialization timestamp
    long firstSequenceId = pubSubMessages.iterator().next().getPosition().getConsumerSequenceId();
    Assert.assertTrue(
        firstSequenceId >= initTimestampNs,
        String.format(
            "First sequence ID (%d) should be >= initialization timestamp (%d)",
            firstSequenceId,
            initTimestampNs));

    // The consumer sequence id should be consecutive and monotonically increasing within the same partition. All
    // partitions should start with the same sequence id (seeded by consumer initialization timestamp).
    long startingSequenceId = firstSequenceId;
    HashMap<Integer, Long> partitionSequenceIdMap = new HashMap<>();
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessages) {
      int partition = message.getPartition();
      long currentSequenceId = message.getPosition().getConsumerSequenceId();
      long expectedSequenceId = partitionSequenceIdMap.computeIfAbsent(partition, k -> startingSequenceId);
      Assert.assertEquals(currentSequenceId, expectedSequenceId);
      partitionSequenceIdMap.put(partition, expectedSequenceId + 1);
    }
    Assert.assertEquals(partitionSequenceIdMap.size(), 3);

    // Test with consumer restart to ensure new consumer gets new sequence IDs
    changeLogConsumer.close();
    long restartTimestampNs = Utils.getCurrentTimeInNanosForSeeding();
    VeniceChangelogConsumer<Utf8, Utf8> restartedConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "1");
    fixture.addCloseable(restartedConsumer);
    restartedConsumer.subscribeAll().get();

    final List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> restartedMessages = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      restartedMessages.addAll(restartedConsumer.poll(5));
      Assert.assertEquals(restartedMessages.size(), 100);
    });

    // Verify restarted consumer's sequence IDs are greater than restart timestamp
    long firstRestartedSequenceId = restartedMessages.iterator().next().getPosition().getConsumerSequenceId();
    Assert.assertTrue(
        firstRestartedSequenceId >= restartTimestampNs,
        String.format(
            "Restarted consumer first sequence ID (%d) should be >= restart timestamp (%d)",
            firstRestartedSequenceId,
            restartTimestampNs));

    // Verify sequence IDs from restarted consumer are monotonically increasing per partition
    partitionSequenceIdMap.clear();
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: restartedMessages) {
      int partition = message.getPartition();
      long currentSequenceId = message.getPosition().getConsumerSequenceId();
      long expectedSequenceId = partitionSequenceIdMap.computeIfAbsent(partition, k -> firstRestartedSequenceId);
      Assert.assertEquals(currentSequenceId, expectedSequenceId);
      partitionSequenceIdMap.put(partition, expectedSequenceId + 1);
    }
  }

  void runSamzaStreamJob(
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
