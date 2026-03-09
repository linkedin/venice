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
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration tests for the after-image changelog consumer: basic consumption, store enable/disable,
 * specific record deserialization, schema evolution, version swap tracking, and sequence IDs.
 */
@Test(singleThreaded = true)
public class TestChangelogConsumer extends AbstractChangelogConsumerTest {
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
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

    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
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

  /**
   * Tests that the after-image consumer correctly tracks version swaps across multiple consecutive
   * push jobs. Reduced from 10 to 3 iterations since the version swap mechanism is stateless per swap —
   * 3 iterations is sufficient to prove correctness across consecutive swaps.
   */
  @Test(timeOut = TEST_TIMEOUT, priority = 3)
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);

    // This is a dumb check that we're doing just to make static analysis happy
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(setupControllerClient.getStore(storeName).getStore().getCurrentVersion(), 0));

    // Write Records to the store for version v1, the push job will contain 100 records.
    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);

    // Write Records from nearline
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    ChangelogClientConfig globalAfterImageClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()));

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

    // Reduced from 10 to 3 iterations: the version swap mechanism is stateless per swap,
    // so 3 iterations suffices to prove correctness across consecutive swaps (v1->v2->v3->v4).
    for (int i = 0; i < 3; i++) {
      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
        // Run Samza job to send PUT and DELETE requests.
        runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100);
        // Produce a DELETE record with large timestamp
        sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
      }

      // run push job and wait for version to be active
      int expectedVersion = i + 2;
      IntegrationTestPushUtils.runVPJ(props, expectedVersion, childControllerClientRegion0);

      // poll data from version topic
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
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

    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    consumerProperties.put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true);
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
            .setD2Client(IntegrationTestPushUtils.getD2Client(localZkServer.getAddress()))
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    // setVersionSwapDetectionIntervalTimeInSeconds also controls native metadata repository refresh interval, setting
    // it to a large value to reproduce the scenario where the newly added schema is yet to be fetched.
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
            .setD2Client(d2Client)
            .setVersionSwapDetectionIntervalTimeInSeconds(180)
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ControllerClient setupControllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
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
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready right after store creation
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(storeName, childControllerClientRegion0, clusterWrapper);
    IntegrationTestPushUtils.runVPJ(props, 1, childControllerClientRegion0);
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, clusterName, localZkServer);
    ChangelogClientConfig globalChangelogClientConfig =
        ChangelogConsumerTestUtils.buildBaseChangelogClientConfig(consumerProperties, localZkServer.getAddress(), 1)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    // Capture timestamp before consumer initialization to validate sequence IDs
    long initTimestampNs = Utils.getCurrentTimeInNanosForSeeding();
    VeniceChangelogConsumer<Utf8, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "0");
    testCloseables.add(changeLogConsumer);

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
    testCloseables.add(restartedConsumer);
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
