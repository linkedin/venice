package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_TO_SEPARATE_REALTIME_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveReplicationForIncPush {
  private static final Logger LOGGER = LogManager.getLogger(TestActiveActiveReplicationForIncPush.class);

  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private String[] clusterNames;
  private String parentRegionName;
  private String[] dcNames;
  private String clusterName;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  PubSubBrokerWrapper veniceParentDefaultKafka;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED, "true");

    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "true");
    controllerProps.put(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, true);
    controllerProps.put(ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, true);

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(controllerProps),
        Optional.of(controllerProps),
        Optional.of(serverProperties),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    clusterNames = multiRegionMultiClusterWrapper.getClusterNames();
    clusterName = this.clusterNames[0];
    parentRegionName = multiRegionMultiClusterWrapper.getParentRegionName();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
    veniceParentDefaultKafka = multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForIncPushWithSeparateRT() throws Exception {
    // TODO: Remove this hack if we solve the test-retry plugin's flakiness with DataProviders
    testAAReplicationForIncPush(true);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForIncPush() throws Exception {
    // TODO: Remove this hack if we solve the test-retry plugin's flakiness with DataProviders
    testAAReplicationForIncPush(false);
  }

  /**
   * This test reproduces an issue where the real-time topic partition count did not match the hybrid version
   * partition count under the following scenario:
   *
   * 1. Create a store with 1 partition.
   * 2. Perform a batch push, resulting in a batch version with 1 partition.
   * 3. Update the store to have 3 partitions and convert it into a hybrid store.
   * 4. Start real-time writes using push type {@link com.linkedin.venice.meta.Version.PushType#STREAM}.
   * 5. Perform a full push, which creates a hybrid version with 3 partitions. This push results in an error
   *    because, after the topic switch to real-time consumers, partitions 1 and 2 of the real-time topic cannot
   *    be found, as it has only 1 partition (partition: 0).
   *
   * The root cause of the issue lies in step 4, where the real-time topic was created if it did not already exist.
   * The partition count for the real-time topic was derived from the largest existing version, which in this case
   * was the batch version with 1 partition. This caused the real-time topic to have incorrect partition count (1
   * instead of 3).
   *
   * To resolve this issue:
   * - STREAM push type is no longer allowed if there is no online hybrid version.
   * - If there is an online hybrid version, it is safe to assume that the real-time topic partition count matches
   *   the hybrid version partition count.
   * - The real-time topic is no longer created if it does not exist as part of the `requestTopicForPushing` method.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testRealTimeTopicPartitionCountMatchesHybridVersion() throws Exception {
    File inputDirBatch = getTempDataDirectory();
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String inputDirPathBatch = "file:" + inputDirBatch.getAbsolutePath();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      String storeName = Utils.getUniqueString("store");
      Properties propsBatch =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathBatch, storeName);
      propsBatch.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDirBatch);
      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

      TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, "owner", keySchemaStr, valueSchemaStr));
      UpdateStoreQueryParams updateStoreParams1 =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setPartitionCount(1);
      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, updateStoreParams1));

      // Run a batch push first to create a batch version with 1 partition
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR + A/A all fabrics", propsBatch)) {
        job.run();
      }

      // wait until version is created and verify the partition count
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        StoreResponse storeResponse = assertCommand(parentControllerClient.getStore(storeName));
        StoreInfo storeInfo = storeResponse.getStore();
        assertNotNull(storeInfo, "Store info is null.");
        assertNull(storeInfo.getHybridStoreConfig(), "Hybrid store config is not null.");
        assertNotNull(storeInfo.getVersion(1), "Version 1 is not present.");
        Optional<Version> version = storeInfo.getVersion(1);
        assertTrue(version.isPresent(), "Version 1 is not present.");
        assertNull(version.get().getHybridStoreConfig(), "Version level hybrid store config is not null.");
        assertEquals(version.get().getPartitionCount(), 1, "Partition count is not 1.");
      });

      // Update the store to have 3 partitions and convert it into a hybrid store
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(3)
              .setHybridOffsetLagThreshold(TEST_TIMEOUT / 2)
              .setHybridRewindSeconds(2L);
      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, updateStoreParams));

      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        StoreResponse storeResponse = assertCommand(parentControllerClient.getStore(storeName));
        StoreInfo storeInfo = storeResponse.getStore();
        assertNotNull(storeInfo, "Store info is null.");
        assertNotNull(storeInfo.getHybridStoreConfig(), "Hybrid store config is null.");
        // verify that there is just one version and it is batch version
        assertEquals(storeInfo.getVersions().size(), 1, "Version count is not 1.");
        Optional<Version> version = storeInfo.getVersion(1);
        assertTrue(version.isPresent(), "Version 1 is not present.");
        assertNull(version.get().getHybridStoreConfig(), "Version level hybrid store config is not null.");
        assertEquals(version.get().getPartitionCount(), 1, "Partition count is not 1.");
      });

      // Push job step was disabled to reproduce the issue
      // Run a full push to create a hybrid version with 3 partitions
      try (VenicePushJob job = new VenicePushJob("push_job_to_create_hybrid_version", propsBatch)) {
        job.run();
      }

      // wait until hybrid version is created and verify the partition count
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        StoreResponse storeResponse = assertCommand(parentControllerClient.getStore(storeName));
        StoreInfo storeInfo = storeResponse.getStore();
        assertNotNull(storeInfo, "Store info is null.");
        assertNotNull(storeInfo.getHybridStoreConfig(), "Hybrid store config is null.");
        assertNotNull(storeInfo.getVersion(2), "Version 2 is not present.");
        Optional<Version> version = storeInfo.getVersion(2);
        assertTrue(version.isPresent(), "Version 2 is not present.");
        assertNotNull(version.get().getHybridStoreConfig(), "Version level hybrid store config is null.");
        assertEquals(version.get().getPartitionCount(), 3, "Partition count is not 3.");
      });

      VeniceSystemFactory factory = new VeniceSystemFactory();
      Map<String, String> samzaConfig = IntegrationTestPushUtils.getSamzaProducerConfig(childDatacenters, 1, storeName);
      VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();

      PubSubTopicRepository pubSubTopicRepository =
          childDatacenters.get(1).getClusters().get(clusterName).getPubSubTopicRepository();
      PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));

      // wait for 120 secs and check producer getTopicName
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Assert.assertEquals(veniceProducer.getTopicName(), realTimeTopic.getName());
      });

      try (TopicManager topicManager =
          IntegrationTestPushUtils
              .getTopicManagerRepo(
                  PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                  100,
                  0l,
                  childDatacenters.get(1).getClusters().get(clusterName).getPubSubBrokerWrapper(),
                  pubSubTopicRepository)
              .getLocalTopicManager()) {
        int partitionCount = topicManager.getPartitionCount(realTimeTopic);
        assertEquals(partitionCount, 3, "Partition count is not 3.");
      }
    }
  }

  /**
   * The purpose of this test is to verify that incremental push with RT policy succeeds when A/A is enabled in all
   * regions. And also incremental push can push to the closes kafka cluster from the grid using the SOURCE_GRID_CONFIG.
   */
  private void testAAReplicationForIncPush(boolean isSeparateRealTimeTopicEnabled) throws Exception {
    File inputDirBatch = getTempDataDirectory();
    File inputDirInc1 = getTempDataDirectory();
    File inputDirInc2 = getTempDataDirectory();

    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String inputDirPathBatch = "file:" + inputDirBatch.getAbsolutePath();
    String inputDirPathInc1 = "file:" + inputDirInc1.getAbsolutePath();
    String inputDirPathInc2 = "file:" + inputDirInc2.getAbsolutePath();
    Function<Integer, String> connectionString = i -> childDatacenters.get(i).getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls);
        ControllerClient dc0ControllerClient = new ControllerClient(clusterName, connectionString.apply(0));
        ControllerClient dc1ControllerClient = new ControllerClient(clusterName, connectionString.apply(1));
        ControllerClient dc2ControllerClient = new ControllerClient(clusterName, connectionString.apply(2))) {
      String storeName = Utils.getUniqueString("store");
      Properties propsBatch =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathBatch, storeName);
      propsBatch.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Properties propsInc1 =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathInc1, storeName);
      propsInc1.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Properties propsInc2 =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathInc2, storeName);
      propsInc2.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDirBatch);
      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

      propsInc1.setProperty(INCREMENTAL_PUSH, "true");
      propsInc1.put(SOURCE_GRID_FABRIC, dcNames[2]);
      if (isSeparateRealTimeTopicEnabled) {
        propsInc1.put(PUSH_TO_SEPARATE_REALTIME_TOPIC, "true");
      }
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema2(inputDirInc1);

      propsInc2.setProperty(INCREMENTAL_PUSH, "true");
      propsInc2.put(SOURCE_GRID_FABRIC, dcNames[1]);
      TestWriteUtils.writeSimpleAvroFileWithString2StringSchema3(inputDirInc2);

      TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, "owner", keySchemaStr, valueSchemaStr));

      StoreInfo storeInfo = TestUtils.assertCommand(parentControllerClient.getStore(storeName)).getStore();

      verifyHybridAndIncPushConfig(
          storeName,
          false,
          false,
          parentControllerClient,
          dc0ControllerClient,
          dc1ControllerClient,
          dc2ControllerClient);

      // Store Setup
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setHybridOffsetLagThreshold(TEST_TIMEOUT / 2)
              .setHybridRewindSeconds(2L)
              .setNativeReplicationSourceFabric("dc-2")
              .setSeparateRealTimeTopicEnabled(isSeparateRealTimeTopicEnabled);

      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, updateStoreParams));

      // Print all the kafka cluster URLs
      LOGGER.info("KafkaURL {}:{}", dcNames[0], childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL {}:{}", dcNames[1], childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL {}:{}", dcNames[2], childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL {}:{}", parentRegionName, veniceParentDefaultKafka.getAddress());

      // verify store configs
      TestUtils.verifyDCConfigNativeAndActiveRepl(
          storeName,
          true,
          true,
          parentControllerClient,
          dc0ControllerClient,
          dc1ControllerClient,
          dc2ControllerClient);

      verifyHybridAndIncPushConfig(
          storeName,
          true,
          true,
          parentControllerClient,
          dc0ControllerClient,
          dc1ControllerClient,
          dc2ControllerClient);

      // Run a batch push first
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR + A/A all fabrics", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }
      if (isSeparateRealTimeTopicEnabled) {
        verifyForSeparateIncrementalPushTopic(storeName, propsInc1, 2, storeInfo);
      } else {
        verifyForRealTimeIncrementalPushTopic(storeName, propsInc1, propsInc2);
      }
    }
  }

  private void verifyForSeparateIncrementalPushTopic(
      String storeName,
      Properties propsInc1,
      int dcIndexForSourceRegion,
      StoreInfo storeInfo) {
    // Prepare TopicManagers
    List<TopicManager> topicManagers = new ArrayList<>();
    for (VeniceMultiClusterWrapper childDataCenter: childDatacenters) {
      PubSubTopicRepository pubSubTopicRepository =
          childDataCenter.getClusters().get(clusterNames[0]).getPubSubTopicRepository();
      topicManagers.add(
          IntegrationTestPushUtils
              .getTopicManagerRepo(
                  PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                  100,
                  0L,
                  childDataCenter.getKafkaBrokerWrapper(),
                  pubSubTopicRepository)
              .getLocalTopicManager());
    }
    // Run inc push with source fabric preference taking effect.
    PubSubTopicPartition separateRealTimeTopicPartition = new PubSubTopicPartitionImpl(
        PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeSeparateRealTimeTopic(storeName)),
        0);
    PubSubTopicPartition realTimeTopicPartition =
        new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.getRealTimeTopicName(storeInfo)), 0);
    try (VenicePushJob job = new VenicePushJob("Test push job incremental with NR + A/A from dc-2", propsInc1)) {
      // TODO: Once server part separate topic ingestion logic is ready, we should avoid runAsync here and add extra
      // check
      CompletableFuture.runAsync(job::run);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        Assert.assertEquals(
            job.getKafkaUrl(),
            childDatacenters.get(dcIndexForSourceRegion).getKafkaBrokerWrapper().getAddress());
        for (int dcIndex = 0; dcIndex < childDatacenters.size(); dcIndex++) {
          long separateTopicOffset =
              topicManagers.get(dcIndex).getLatestOffsetWithRetries(separateRealTimeTopicPartition, 3);
          long realTimeTopicOffset = topicManagers.get(dcIndex).getLatestOffsetWithRetries(realTimeTopicPartition, 3);
          // Real-time topic will have heartbeat messages, so the offset will be non-zero but smaller than the record
          // count.
          // DC 2 separeate real-time topic should get enough data.
          if (dcIndex == dcIndexForSourceRegion) {
            Assert.assertTrue(
                separateTopicOffset > TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT,
                "Records # is not enough: " + separateTopicOffset);
            Assert.assertTrue(
                realTimeTopicOffset < TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT / 10,
                "Records # is more than expected: " + realTimeTopicOffset);
          } else {
            assertEquals(separateTopicOffset, 0, "Records # is not enough: " + separateTopicOffset);
            Assert.assertTrue(
                realTimeTopicOffset < TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT / 10,
                "Records # is more than expected: " + realTimeTopicOffset);
          }
        }
      });
      job.cancel();
    }
  }

  private void verifyForRealTimeIncrementalPushTopic(String storeName, Properties propsInc1, Properties propsInc2)
      throws Exception {
    // Run inc push with source fabric preference taking effect.
    try (VenicePushJob job = new VenicePushJob("Test push job incremental with NR + A/A from dc-2", propsInc1)) {
      job.run();
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
    }

    // Verify
    for (int i = 0; i < childDatacenters.size(); i++) {
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
      // Verify the current version should be 1.
      Version version =
          childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
      Assert.assertNotNull(version, "Version 1 is not present for DC: " + dcNames[i]);
    }
    NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);

    // Run another inc push with a different source fabric preference taking effect.
    try (VenicePushJob job = new VenicePushJob("Test push job incremental with NR + A/A from dc-1", propsInc2)) {
      job.run();
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
    }
    NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 200, 3);
  }

  public static void verifyHybridAndIncPushConfig(
      String storeName,
      boolean expectedIncPushStatus,
      boolean isNonNullHybridStoreConfig,
      ControllerClient... controllerClients) {
    for (ControllerClient controllerClient: controllerClients) {
      waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = assertCommand(controllerClient.getStore(storeName));
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(
            storeInfo.isIncrementalPushEnabled(),
            expectedIncPushStatus,
            "The incremental push config does not match.");
        if (!isNonNullHybridStoreConfig) {
          assertNull(storeInfo.getHybridStoreConfig(), "The hybrid store config is not null.");
          return;
        }
        HybridStoreConfig hybridStoreConfig = storeInfo.getHybridStoreConfig();
        assertNotNull(hybridStoreConfig, "The hybrid store config is null.");
        DataReplicationPolicy policy = hybridStoreConfig.getDataReplicationPolicy();
        assertNotNull(policy, "The data replication policy is null.");
      });
    }
  }
}
