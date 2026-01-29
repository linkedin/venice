package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.INSTANCE_ID;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_SOP;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_TERM_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestHybridMultiRegion {
  /**
   * IMPORTANT NOTE: if you use this sharedVenice cluster, please do not close it. The {@link #cleanUp()} function
   *                 will take care of it. Besides, if any backend component of the shared cluster is stopped in
   *                 the middle of the test, please restart them at the end of your test.
   */
  private VeniceTwoLayerMultiRegionMultiClusterWrapper sharedVenice;
  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  /**
   * This cluster is re-used by some of the tests, in order to speed up the suite. Some other tests require
   * certain specific characteristics which makes it awkward to re-use, though not necessarily impossible.
   * Further reuse of this shared cluster can be attempted later.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    sharedVenice = setUpCluster();
    pubSubPositionTypeRegistry = sharedVenice.getParentKafkaBrokerWrapper().getPubSubPositionTypeRegistry();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
  }

  @Test(timeOut = 180
      * Time.MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHybridBatchPushWithRmd(boolean useNativeInputFormat) throws IOException {
    String clusterName = sharedVenice.getClusterNames()[0];
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, sharedVenice.getControllerConnectString())) {
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;
      final String storeName = Utils.getUniqueString("multi-colo-hybrid-store");

      // Create store at parent, make it a hybrid store
      TestUtils.assertCommand(
          controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString()));
      TestUtils.assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
                  .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                  .setHybridRewindSeconds(streamingRewindSeconds)
                  .setHybridOffsetLagThreshold(streamingMessageLag)));
      controllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);

      // Prepare input for a batch push with RMD data
      long recordTimestamp = 123456789L;
      long ttlStartTimestamp = 123456799L;
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringAndTimestampSchema(inputDir, recordTimestamp); // records
                                                                                                         // 1-100
      Properties vpjProperties = defaultVPJProps(sharedVenice, inputDirPath, storeName);
      vpjProperties.setProperty(RMD_FIELD_PROP, "rmd");
      vpjProperties.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
      vpjProperties.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, String.valueOf(useNativeInputFormat));

      // VPJ push is expected to succeed, and we validate for new version post the push
      IntegrationTestPushUtils.runVPJ(vpjProperties);
      assertExpectedVersionCountAndVersionStatus(controllerClient, storeName, 2, 2);
      StoreResponse storeResponse;

      // Do a TTL repush to ensure valid RMD was written and can be read properly.
      VeniceClusterWrapper sharedVeniceClusterWrapper =
          sharedVenice.getChildRegions().get(0).getClusters().get(clusterName);
      Properties props = IntegrationTestPushUtils.defaultVPJProps(sharedVenice, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, sharedVeniceClusterWrapper.getPubSubBrokerWrapper().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      props.setProperty(REPUSH_TTL_ENABLE, "true");
      // Override the TTL repush start TS to work with logical TS setup.
      props.setProperty(REPUSH_TTL_START_TIMESTAMP, String.valueOf(ttlStartTimestamp));
      // Override the rewind time to make sure not to consume 24hrs data from RT topic.
      props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      IntegrationTestPushUtils.runVPJ(props);
      assertExpectedVersionCountAndVersionStatus(controllerClient, storeName, 3, 3);
    }
  }

  @Test(timeOut = 180
      * Time.MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHybridBatchPushWithInvalidRmd(boolean useNativeInputFormat) throws IOException {
    String clusterName = sharedVenice.getClusterNames()[0];
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, sharedVenice.getControllerConnectString())) {
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;
      final String storeName = Utils.getUniqueString("multi-colo-hybrid-store");

      // Create store at parent, make it a hybrid store
      TestUtils.assertCommand(
          controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString()));
      ControllerResponse response = TestUtils.assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
                  .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                  .setHybridRewindSeconds(streamingRewindSeconds)
                  .setHybridOffsetLagThreshold(streamingMessageLag)));
      TestUtils.assertCommand(
          controllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, 120000));
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      // records 1-100
      TestWriteUtils
          .writeSimpleAvroFileWithStringToStringAndTimestampSchema(inputDir, String.valueOf(123456789L).getBytes());
      Properties vpjProperties = defaultVPJProps(sharedVenice, inputDirPath, storeName);
      vpjProperties.setProperty(RMD_FIELD_PROP, "rmd");
      vpjProperties.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
      vpjProperties.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, String.valueOf(useNativeInputFormat));

      Assert.assertFalse(response.isError());
      // push should fail validation
      VeniceException failureException =
          Assert.expectThrows(VeniceException.class, () -> IntegrationTestPushUtils.runVPJ(vpjProperties));
      Assert.assertTrue(
          failureException.getMessage().contains("Input rmd schema does not match the server side RMD schema"),
          "The exception message does not match with the expected one RMD validation failure. Got: "
              + failureException.getMessage());
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridInitializationOnMultiColo() throws IOException {
    String clusterName = sharedVenice.getClusterNames()[0];
    VeniceClusterWrapper sharedVeniceClusterWrapper =
        sharedVenice.getChildRegions().get(0).getClusters().get(clusterName);
    try (
        ControllerClient controllerClient =
            new ControllerClient(clusterName, sharedVenice.getControllerConnectString());
        TopicManager topicManager =
            IntegrationTestPushUtils
                .getTopicManagerRepo(
                    PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                    100,
                    0l,
                    sharedVeniceClusterWrapper.getPubSubBrokerWrapper(),
                    sharedVeniceClusterWrapper.getPubSubTopicRepository())
                .getLocalTopicManager()) {
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;
      final String storeName = Utils.getUniqueString("multi-colo-hybrid-store");

      // Create store at parent, make it a hybrid store
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
          streamingRewindSeconds,
          streamingMessageLag,
          HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
          REWIND_FROM_EOP);
      // There should be no version on the store yet
      assertEquals(
          controllerClient.getStore(storeName).getStore().getCurrentVersion(),
          0,
          "The newly created store must have a current version of 0");

      // Create a new version, and do an empty push for that version
      VersionCreationResponse vcr =
          controllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      int versionNumber = vcr.getVersion();
      assertNotEquals(versionNumber, 0, "requesting a topic for a push should provide a non zero version number");

      TestUtils.waitForNonDeterministicAssertion(100, TimeUnit.SECONDS, true, () -> {
        // Now the store should have version 1
        JobStatusQueryResponse jobStatus =
            controllerClient.queryJobStatus(Version.composeKafkaTopic(storeName, versionNumber));
        Assert.assertFalse(jobStatus.isError(), "Error in getting JobStatusResponse: " + jobStatus.getError());
        assertEquals(jobStatus.getStatus(), "COMPLETED");
      });

      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
      String realTimeTopicName = Utils.getRealTimeTopicName(storeInfo);

      // And real-time topic should exist now.
      assertTrue(
          topicManager.containsTopicAndAllPartitionsAreOnline(
              sharedVeniceClusterWrapper.getPubSubTopicRepository().getTopic(realTimeTopicName)));
      // Creating a store object with default values since we're not updating bootstrap to online timeout
      StoreProperties storeProperties = AvroRecordUtils.prefillAvroRecordWithDefaultValue(new StoreProperties());
      storeProperties.name = storeName;
      storeProperties.owner = "owner";
      storeProperties.createdTime = System.currentTimeMillis();
      Store store = new ZKStore(storeProperties);
      assertEquals(
          topicManager
              .getTopicRetention(sharedVeniceClusterWrapper.getPubSubTopicRepository().getTopic(realTimeTopicName)),
          StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig),
          "RT retention not configured properly");
      // Make sure RT retention is updated when the rewind time is updated
      long newStreamingRewindSeconds = 600;
      hybridStoreConfig.setRewindTimeInSeconds(newStreamingRewindSeconds);
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(newStreamingRewindSeconds));
      assertEquals(
          topicManager
              .getTopicRetention(sharedVeniceClusterWrapper.getPubSubTopicRepository().getTopic(realTimeTopicName)),
          StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig),
          "RT retention not updated properly");
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridSplitBrainIssue() {
    String clusterName = sharedVenice.getClusterNames()[0];
    VeniceClusterWrapper sharedVeniceClusterWrapper =
        sharedVenice.getChildRegions().get(0).getClusters().get(clusterName);
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, sharedVenice.getControllerConnectString())) {
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;
      final String storeName = Utils.getUniqueString("hybrid-store");

      // Create store at parent, make it a hybrid store
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      // There should be no version on the store yet
      assertEquals(
          controllerClient.getStore(storeName).getStore().getCurrentVersion(),
          0,
          "The newly created store must have a current version of 0");

      VersionResponse versionResponse = controllerClient.addVersionAndStartIngestion(
          storeName,
          Utils.getUniqueString("test-hybrid-push"),
          1,
          3,
          Version.PushType.BATCH,
          null,
          -1,
          1);
      assertFalse(
          versionResponse.isError(),
          "Version creation shouldn't return error, but received: " + versionResponse.getError());
      String versionTopicName = Version.composeKafkaTopic(storeName, 1);

      String writer1 = "writer_1_hostname";
      String writer2 = "writer_2_hostname";
      Properties veniceWriterProperties1 = new Properties();
      veniceWriterProperties1
          .put(KAFKA_BOOTSTRAP_SERVERS, sharedVeniceClusterWrapper.getPubSubBrokerWrapper().getAddress());
      veniceWriterProperties1.putAll(
          PubSubBrokerWrapper.getBrokerDetailsForClients(
              Collections.singletonList(sharedVeniceClusterWrapper.getPubSubBrokerWrapper())));
      veniceWriterProperties1.put(INSTANCE_ID, writer1);

      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          sharedVeniceClusterWrapper.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

      Properties veniceWriterProperties2 = new Properties();
      veniceWriterProperties2
          .put(KAFKA_BOOTSTRAP_SERVERS, sharedVeniceClusterWrapper.getPubSubBrokerWrapper().getAddress());
      veniceWriterProperties2.putAll(
          PubSubBrokerWrapper.getBrokerDetailsForClients(
              Collections.singletonList(sharedVeniceClusterWrapper.getPubSubBrokerWrapper())));
      veniceWriterProperties2.put(INSTANCE_ID, writer2);

      try (
          VeniceWriter<byte[], byte[], byte[]> veniceWriter1 = TestUtils
              .getVeniceWriterFactory(veniceWriterProperties1, pubSubProducerAdapterFactory, pubSubPositionTypeRegistry)
              .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build());
          VeniceWriter<byte[], byte[], byte[]> veniceWriter2 = TestUtils
              .getVeniceWriterFactory(veniceWriterProperties2, pubSubProducerAdapterFactory, pubSubPositionTypeRegistry)
              .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build())) {
        veniceWriter1.broadcastStartOfPush(false, Collections.emptyMap());

        /**
         * Explicitly simulate split-brain issue.
         * Writer1:
         *
         * key_0: value_0 with upstream offset: 5
         * key_1: value_1 with upstream offset: 6
         * key_2: value_2 with upstream offset: 7
         * key_3: value_3 with upstream offset: 8
         * key_4: value_4 with upstream offset: 9
         * Writer2:
         * key_0: value_x with upstream offset: 3
         * key_5: value_5 with upstream offset: 10
         * key_6: value_6 with upstream offset: 11
         * key_7: value_7 with upstream offset: 12
         * key_8: value_8 with upstream offset: 13
         * key_9: value_9 with upstream offset: 14
         */

        PubSubPosition upstreamPosition1 = new ApacheKafkaOffsetPosition(0);
        // Sending out dummy records first to push out SOS messages first.
        veniceWriter1.put(
            stringSerializer.serialize("key_writer_1"),
            stringSerializer.serialize("value_writer_1"),
            1,
            null,
            new LeaderMetadataWrapper(upstreamPosition1, 0, DEFAULT_TERM_ID));
        veniceWriter1.flush();

        PubSubPosition upstreamPosition2 = new ApacheKafkaOffsetPosition(1);
        veniceWriter2.put(
            stringSerializer.serialize("key_writer_2"),
            stringSerializer.serialize("value_writer_2"),
            1,
            null,
            new LeaderMetadataWrapper(upstreamPosition2, 0, DEFAULT_TERM_ID));
        veniceWriter2.flush();

        PubSubPosition upstreamPositionIPlusFive;
        for (int i = 0; i < 5; ++i) {
          upstreamPositionIPlusFive = new ApacheKafkaOffsetPosition(i + 5);
          veniceWriter1.put(
              stringSerializer.serialize("key_" + i),
              stringSerializer.serialize("value_" + i),
              1,
              null,
              new LeaderMetadataWrapper(upstreamPositionIPlusFive, 0, DEFAULT_TERM_ID));
        }
        veniceWriter1.flush();

        PubSubPosition upstreamPosition3 = new ApacheKafkaOffsetPosition(2);
        veniceWriter2.put(
            stringSerializer.serialize("key_" + 0),
            stringSerializer.serialize("value_x"),
            1,
            null,
            new LeaderMetadataWrapper(upstreamPosition3, 0, DEFAULT_TERM_ID));

        for (int i = 5; i < 10; ++i) {
          upstreamPositionIPlusFive = new ApacheKafkaOffsetPosition(i + 5);
          veniceWriter2.put(
              stringSerializer.serialize("key_" + i),
              stringSerializer.serialize("value_" + i),
              1,
              null,
              new LeaderMetadataWrapper(upstreamPositionIPlusFive, 0, DEFAULT_TERM_ID));
        }
        veniceWriter2.flush();
        veniceWriter1.broadcastEndOfPush(Collections.emptyMap());
        veniceWriter1.flush();
      }

      TestUtils.waitForNonDeterministicAssertion(100, TimeUnit.SECONDS, true, () -> {
        // Now the store should have version 1
        JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(Version.composeKafkaTopic(storeName, 1));
        Assert.assertFalse(jobStatus.isError(), "Error in getting JobStatusResponse: " + jobStatus.getError());
        assertEquals(jobStatus.getStatus(), "ERROR");
      });
    }
  }

  /**
   * N.B.: Non-L/F does not support chunking, so this permutation is skipped.
   */
  @DataProvider(name = "testPermutations", parallel = false)
  public static Object[][] testPermutations() {
    return new Object[][] { { false, false, REWIND_FROM_EOP }, { false, true, REWIND_FROM_EOP },
        { true, false, REWIND_FROM_EOP }, { true, true, REWIND_FROM_EOP }, { false, false, REWIND_FROM_SOP },
        { false, true, REWIND_FROM_SOP }, { true, false, REWIND_FROM_SOP }, { true, true, REWIND_FROM_SOP } };
  }

  private static void assertExpectedVersionCountAndVersionStatus(
      ControllerClient controllerClient,
      String storeName,
      int expectedVersionCount,
      int versionToCheckStatus) {
    StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
    Assert.assertEquals(storeResponse.getStore().getVersions().size(), expectedVersionCount);
    boolean foundVersionOnline = storeResponse.getStore()
        .getVersion(versionToCheckStatus)
        .map(Version::getStatus)
        .filter(status -> status.equals(VersionStatus.ONLINE))
        .isPresent();
    Assert.assertTrue(foundVersionOnline, "Version " + versionToCheckStatus + " was not found to be ONLINE");
  }

  private static VeniceTwoLayerMultiRegionMultiClusterWrapper setUpCluster() {
    Properties parentControllerProps = new Properties();
    parentControllerProps.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");

    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");

    Properties serverProperties = new Properties();
    serverProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    serverProperties.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(Optional.of(parentControllerProps).orElse(null))
            .childControllerProperties(Optional.of(childControllerProperties).orElse(null))
            .serverProperties(serverProperties);
    VeniceTwoLayerMultiRegionMultiClusterWrapper cluster =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    return cluster;
  }
}
