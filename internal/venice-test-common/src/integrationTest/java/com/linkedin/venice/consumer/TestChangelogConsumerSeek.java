package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.ImmutableChangeCapturePubSubMessage;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestChangelogConsumerSeek {
  private static final Logger LOGGER = LogManager.getLogger(TestChangelogConsumerSeek.class);
  static final int TEST_TIMEOUT = 180_000; // 3 minutes

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

  private void pollAndCollectVersionSpecificMessages(
      VeniceChangelogConsumer<Integer, Utf8> consumer,
      Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> map,
      int expectedCount) {
    // Short poll timeout since the outer retry loop handles waiting for data to arrive
    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> messages = consumer.poll(5);
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: messages) {
        map.put(message.getKey(), message);
      }
      assertEquals(map.size(), expectedCount);
    });
  }

  private void validateVersionSpecificMessages(
      Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> map,
      int version,
      int numKeys) {
    for (int i = 1; i <= numKeys; i++) {
      ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
          (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) map.get(i);
      assertEquals(message.getValue().getCurrentValue().toString(), Integer.toString(version) + i);
      assertTrue(message.getPayloadSize() > 0);
      assertNotNull(message.getPosition());
      assertTrue(message.getWriterSchemaId() > 0);
      assertNotNull(message.getReplicationMetadataPayload());
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2, priority = 3)
  public void testVersionSpecificSeekingChangeLogConsumer()
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 100;
    int partitionCount = 3;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
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
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap();
    Map<Integer, VeniceChangeCoordinate> partitionToChangeCoordinateMap = new HashMap();

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {

      for (int partition = 0; partition < partitionCount; partition++) {
        // Ensure we can "seek" multiple times on the same version
        changeLogConsumer.subscribe(Collections.singleton(partition)).get();
      }
      pollAndCollectVersionSpecificMessages(changeLogConsumer, pubSubMessagesMap, numKeys);

      // All data should be from version 1; also extract partition coordinates for seek test
      for (int i = 1; i <= numKeys; i++) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> message =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) pubSubMessagesMap.get(i);
        partitionToChangeCoordinateMap.put(message.getPartition(), message.getPosition());
      }
      validateVersionSpecificMessages(pubSubMessagesMap, version, numKeys);
    }

    // Restart client and resume from the last consumed checkpoints
    pubSubMessagesMap.clear();

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      pubSubMessagesMap.clear();
      changeLogConsumer.seekToCheckpoint(new HashSet<>(partitionToChangeCoordinateMap.values())).get();

      Set<Integer> partitions = new HashSet<>(partitionCount);
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer.poll(5);
        // After seekToCheckpoint (inclusive), we expect to re-consume at least one message (the checkpoint or later)
        // from each partition; track the partitions we've seen so far.
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          partitions.add(message.getPartition());
        }
        assertEquals(partitions.size(), partitionCount);
      });

      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils
          .getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
        // Run Samza job to send PUT and DELETE requests.
        sendStreamingRecord(veniceProducer, storeName, 10000, "10000", null);
      }

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> tempPubSubMessagesList =
            changeLogConsumer.poll(5);
        assertEquals(tempPubSubMessagesList.size(), 1);

        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: tempPubSubMessagesList) {
          assertEquals((int) pubSubMessage.getKey(), 10000);
          assertEquals(pubSubMessage.getValue().getCurrentValue().toString(), "10000");
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSeekToCheckpointPastSOPWithChunking() throws IOException, ExecutionException, InterruptedException {
    runSeekToCheckpointPastSOP(CompressionStrategy.NO_OP);
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSeekToCheckpointPastSOPWithGzipChunking()
      throws IOException, ExecutionException, InterruptedException {
    runSeekToCheckpointPastSOP(CompressionStrategy.GZIP);
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testSeekToCheckpointPastSOPWithZstdChunking()
      throws IOException, ExecutionException, InterruptedException {
    runSeekToCheckpointPastSOP(CompressionStrategy.ZSTD_WITH_DICT);
  }

  /**
   * Verifies that seekToCheckpoint works when SOP was never consumed (fresh RocksDB, no persisted SVS).
   * Uses large (>950KB) values to trigger chunking on VT. Without SVS synthesis, the consumer fails
   * because chunked records need SVS for validation (waitVersionStateAvailable), ZSTD_WITH_DICT needs
   * SVS for the compression dictionary, and all records eventually fail when waitForStateVersion kills
   * the ingestion task.
   */
  private void runSeekToCheckpointPastSOP(CompressionStrategy compressionStrategy)
      throws IOException, ExecutionException, InterruptedException {
    int numKeys = 2;
    char[] largeChars = new char[1024 * 1024];
    Arrays.fill(largeChars, 'x');
    String largeValue = new String(largeChars);
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, largeValue, numKeys);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
    UpdateStoreQueryParams storeParms =
        ChangelogConsumerTestUtils.buildDefaultStoreParams().setCompressionStrategy(compressionStrategy);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
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
        .setD2Client(fixture.getD2Client());

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    // Subscribe normally to consume batch data and collect checkpoints
    Map<Integer, VeniceChangeCoordinate> partitionToCheckpoint = new HashMap<>();
    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();
      Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap =
          new HashMap<>();
      pollAndCollectVersionSpecificMessages(changeLogConsumer, pubSubMessagesMap, numKeys);
      for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> msg: pubSubMessagesMap.values()) {
        ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>> immMsg =
            (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Utf8>>) msg;
        partitionToCheckpoint.put(immMsg.getPartition(), immMsg.getPosition());
      }
    }

    // seekToCheckpoint with a fresh RocksDB path (no persisted SVS/dictionary from first subscription)
    Properties freshConsumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer(),
        Utils.getUniqueString(inputDirPath));
    ChangelogClientConfig freshChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(freshConsumerProperties, fixture.getLocalZkServer().getAddress(), 1)
        .setD2Client(fixture.getD2Client());
    VeniceChangelogConsumerClientFactory freshFactory =
        new VeniceChangelogConsumerClientFactory(freshChangelogClientConfig, metricsRepository);

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        freshFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.seekToCheckpoint(new HashSet<>(partitionToCheckpoint.values())).get();

      try (VeniceSystemProducer veniceProducer = IntegrationTestPushUtils
          .getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
        sendStreamingRecord(veniceProducer, storeName, 10000, "10000", null);
      }

      Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> receivedMessages =
          new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> messages =
            changeLogConsumer.poll(5);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> msg: messages) {
          receivedMessages.put(msg.getKey(), msg);
        }
        assertTrue(receivedMessages.containsKey(10000), "Should have received streaming record with key 10000");
        assertEquals(receivedMessages.get(10000).getValue().getCurrentValue().toString(), "10000");
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT, priority = 3)
  public void testVersionSpecificChangeLogConsumer() throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    int numKeys = 100;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    fixture.addStoreToDelete(storeName);
    Properties props = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();
    UpdateStoreQueryParams storeParms = ChangelogConsumerTestUtils.buildDefaultStoreParams();
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, props, storeParms);
    // Wait for meta system store to be ready before creating changelog consumer
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
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap<>();

    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      pollAndCollectVersionSpecificMessages(changeLogConsumer, pubSubMessagesMap, numKeys);

      // All data should be from version 1
      validateVersionSpecificMessages(pubSubMessagesMap, version, numKeys);
    }

    // Restart client to ensure it seeks to the beginning of the topic and all record metadata is available
    pubSubMessagesMap.clear();
    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      pollAndCollectVersionSpecificMessages(changeLogConsumer, pubSubMessagesMap, numKeys);

      // All data should be from version 1
      validateVersionSpecificMessages(pubSubMessagesMap, version, numKeys);

      // Push version 2
      version++;
      TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
      IntegrationTestPushUtils.runVPJ(props, 2, fixture.getChildControllerClientRegion0());

      // Client shouldn't be able to poll anything, since it's still on version 1
      assertEquals(changeLogConsumer.poll(5).size(), 0);
    }

    // Restart client
    pubSubMessagesMap.clear();
    try (VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1)) {
      changeLogConsumer.subscribeAll().get();

      pollAndCollectVersionSpecificMessages(changeLogConsumer, pubSubMessagesMap, numKeys);

      // All data should be from version 1
      validateVersionSpecificMessages(pubSubMessagesMap, version - 1, numKeys);

      // Push version 3 with deferred version swap and subscribe to the future version
      changeLogConsumer.close();
      pubSubMessagesMap.clear();
      version++;
      TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
      props.put(DEFER_VERSION_SWAP, true);
      IntegrationTestPushUtils.runVPJ(props);
      // Wait for version 3 push to complete (but current version stays at 2 due to deferred swap)
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          fixture.getChildControllerClientRegion0(),
          90,
          TimeUnit.SECONDS);

      VeniceChangelogConsumer<Integer, Utf8> changeLogConsumer3 =
          veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, version);
      fixture.addCloseable(changeLogConsumer3);
      changeLogConsumer3.subscribeAll().get();

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
            changeLogConsumer3.poll(1000);
        for (PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
          pubSubMessagesMap.put(message.getKey(), message);
        }
        assertEquals(pubSubMessagesMap.size(), numKeys);
      });

      // All data should be from future version 3
      validateVersionSpecificMessages(pubSubMessagesMap, version, numKeys);
    }
  }

}
