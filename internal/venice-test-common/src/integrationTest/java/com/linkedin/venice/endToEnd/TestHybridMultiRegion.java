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
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_SOP;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
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

  /**
   * This cluster is re-used by some of the tests, in order to speed up the suite. Some other tests require
   * certain specific characteristics which makes it awkward to re-use, though not necessarily impossible.
   * Further reuse of this shared cluster can be attempted later.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    sharedVenice = setUpCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
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
      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
      String realTimeTopicName = Utils.getRealTimeTopicName(storeInfo);
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
          streamingRewindSeconds,
          streamingMessageLag,
          HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
          DataReplicationPolicy.NON_AGGREGATE,
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
          VeniceWriter<byte[], byte[], byte[]> veniceWriter1 =
              TestUtils.getVeniceWriterFactory(veniceWriterProperties1, pubSubProducerAdapterFactory)
                  .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build());
          VeniceWriter<byte[], byte[], byte[]> veniceWriter2 =
              TestUtils.getVeniceWriterFactory(veniceWriterProperties2, pubSubProducerAdapterFactory)
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

        // Sending out dummy records first to push out SOS messages first.
        veniceWriter1.put(
            stringSerializer.serialize("key_writer_1"),
            stringSerializer.serialize("value_writer_1"),
            1,
            null,
            new LeaderMetadataWrapper(0, 0));
        veniceWriter1.flush();
        veniceWriter2.put(
            stringSerializer.serialize("key_writer_2"),
            stringSerializer.serialize("value_writer_2"),
            1,
            null,
            new LeaderMetadataWrapper(1, 0));
        veniceWriter2.flush();

        for (int i = 0; i < 5; ++i) {
          veniceWriter1.put(
              stringSerializer.serialize("key_" + i),
              stringSerializer.serialize("value_" + i),
              1,
              null,
              new LeaderMetadataWrapper(i + 5, 0));
        }
        veniceWriter1.flush();
        veniceWriter2.put(
            stringSerializer.serialize("key_" + 0),
            stringSerializer.serialize("value_x"),
            1,
            null,
            new LeaderMetadataWrapper(3, 0));
        for (int i = 5; i < 10; ++i) {
          veniceWriter2.put(
              stringSerializer.serialize("key_" + i),
              stringSerializer.serialize("value_" + i),
              1,
              null,
              new LeaderMetadataWrapper(i + 5, 0));
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

  private static VeniceTwoLayerMultiRegionMultiClusterWrapper setUpCluster() {
    Properties parentControllerProps = new Properties();
    parentControllerProps.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");

    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");

    Properties serverProperties = new Properties();
    serverProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    serverProperties.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");

    VeniceTwoLayerMultiRegionMultiClusterWrapper cluster =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            1,
            1,
            1,
            1,
            2,
            1,
            1,
            Optional.of(parentControllerProps),
            Optional.of(childControllerProperties),
            Optional.of(serverProperties),
            false);

    return cluster;
  }
}
