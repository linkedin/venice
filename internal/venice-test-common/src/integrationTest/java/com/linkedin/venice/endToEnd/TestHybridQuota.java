package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.io.File;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestHybridQuota {
  private static final Logger LOGGER = LogManager.getLogger(TestHybrid.class);

  private VeniceClusterWrapper sharedVenice;

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));

    // N.B.: RF 2 with 3 servers is important, in order to test both the leader and follower code paths
    sharedVenice = ServiceFactory.getVeniceCluster(1, 0, 0, 2, 1000000, false, false, extraProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(HELIX_HYBRID_STORE_QUOTA_ENABLED, true);

    sharedVenice.addVeniceRouter(routerProperties);
    // Added a server with shared consumer enabled.
    Properties serverPropertiesWithSharedConsumer = new Properties();
    serverPropertiesWithSharedConsumer.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    extraProperties.put(HELIX_HYBRID_STORE_QUOTA_ENABLED, true);
    extraProperties.put(HYBRID_QUOTA_ENFORCEMENT_ENABLED, true);
    extraProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    LOGGER.info("Finished creating VeniceClusterWrapper");
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
  }

  /**
   * N.B.: Non-L/F does not support chunking and querying push status from router, so this permutation is skipped.
   */
  @DataProvider(name = "testHybridQuotaPermutations", parallel = false)
  public static Object[][] testHybridQuotaPermutations() {
    return new Object[][] { { false, false, true },
        // { false, true, true },
        // { true, true, true },
        { true, false, true }, { false, false, false },
        // { false, true, false },
        { false, false, false },
        // { true, true, false },
        { true, false, false } };
  }

  @Test(dataProvider = "testHybridQuotaPermutations", timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridStoreQuota(boolean chunkingEnabled, boolean isStreamReprocessing, boolean recoverFromViolation)
      throws Exception {
    SystemProducer veniceProducer = null;
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;

    // Setting a store name that looks like a VT to verify that the quota logic can still work
    String storeName = Utils.getUniqueString("test-store") + "_v1";
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(sharedVenice, inputDirPath, storeName);

    SafeHelixManager readManager = null;
    HelixCustomizedViewOfflinePushRepository offlinePushRepository = null;
    HelixHybridStoreQuotaRepository hybridStoreQuotaOnlyRepository = null;
    try (
        ControllerClient controllerClient =
            createStoreForJob(sharedVenice.getClusterName(), recordSchema, vpjProperties);
        TopicManager topicManager =
            IntegrationTestPushUtils
                .getTopicManagerRepo(
                    DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
                    100L,
                    0L,
                    sharedVenice.getKafka().getAddress(),
                    sharedVenice.getPubSubTopicRepository())
                .getTopicManager()) {

      // Setting the hybrid store quota here will cause the VPJ push failed.
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(2)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setChunkingEnabled(chunkingEnabled)
              .setHybridStoreDiskQuotaEnabled(true));

      Assert.assertFalse(response.isError());

      HelixAdmin helixAdmin = null;
      try {
        helixAdmin = new ZKHelixAdmin(sharedVenice.getZk().getAddress());
        helixAdmin.addCluster(sharedVenice.getClusterName());
      } finally {
        if (helixAdmin != null) {
          helixAdmin.close();
        }
      }

      readManager = new SafeHelixManager(
          HelixManagerFactory.getZKHelixManager(
              sharedVenice.getClusterName(),
              "reader",
              InstanceType.SPECTATOR,
              sharedVenice.getZk().getAddress()));
      readManager.connect();
      String zkAddress = sharedVenice.getZk().getAddress();
      ZkClient zkClient = ZkClientFactory.newZkClient(zkAddress);
      HelixAdapterSerializer adapter = new HelixAdapterSerializer();
      HelixReadWriteStoreRepository writeStoreRepository = new HelixReadWriteStoreRepository(
          zkClient,
          adapter,
          zkAddress,
          Optional.empty(),
          new ClusterLockManager(sharedVenice.getClusterName()));

      Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
      store.setPartitionCount(3);
      Version version = new VersionImpl(storeName, 1, "pushId");
      version.setPartitionCount(3);
      store.addVersion(version);
      version = new VersionImpl(storeName, 2, "pushId");
      version.setPartitionCount(3);
      store.addVersion(version);
      version = new VersionImpl(storeName, 3, "pushId");
      version.setPartitionCount(3);
      store.addVersion(version);
      writeStoreRepository.addStore(store);
      offlinePushRepository = new HelixCustomizedViewOfflinePushRepository(readManager, writeStoreRepository, false);
      hybridStoreQuotaOnlyRepository = new HelixHybridStoreQuotaRepository(readManager);
      offlinePushRepository.refresh();
      hybridStoreQuotaOnlyRepository.refresh();

      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);
      String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);

      // Do a VPJ push
      runVPJ(vpjProperties, 2, controllerClient);
      String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
      Assert.assertTrue(
          topicManager
              .isTopicCompactionEnabled(sharedVenice.getPubSubTopicRepository().getTopic(topicForStoreVersion1)),
          "topic: " + topicForStoreVersion1 + " should have compaction enabled");
      // We did not do any STREAM push here. For a version topic, it should have both hybrid store quota status and
      // offline
      // push status.
      assertEquals(
          hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicForStoreVersion1),
          HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
      assertTrue(offlinePushRepository.containsKafkaTopic(topicForStoreVersion1));
      assertEquals(
          hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicForStoreVersion2),
          HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
      assertTrue(offlinePushRepository.containsKafkaTopic(topicForStoreVersion2));

      // Do a VPJ push
      runVPJ(vpjProperties, 3, controllerClient);
      String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
      long storageQuotaInByte = 60000; // A small quota, easily violated.

      // Need to update store with quota here.
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(2)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setChunkingEnabled(chunkingEnabled)
              .setHybridStoreDiskQuotaEnabled(true)
              .setStorageQuotaInByte(storageQuotaInByte));
      if (isStreamReprocessing) {
        veniceProducer = getSamzaProducer(sharedVenice, storeName, Version.PushType.STREAM_REPROCESSING); // new
                                                                                                          // producer,
                                                                                                          // new DIV
                                                                                                          // segment.
      } else {
        veniceProducer = getSamzaProducer(sharedVenice, storeName, Version.PushType.STREAM); // new producer, new DIV
                                                                                             // segment.
      }
      for (int i = 1; i <= 20; i++) {
        try {
          sendCustomSizeStreamingRecord(veniceProducer, storeName, i, TestHybrid.STREAMING_RECORD_SIZE);
        } catch (VeniceException e) {
          // Expected exception.
          LOGGER.info(e.getMessage());
        }
      }
      long normalTimeForConsuming = TimeUnit.SECONDS.toMillis(15);
      LOGGER.info("normalTimeForConsuming:{}", normalTimeForConsuming);
      Utils.sleep(normalTimeForConsuming);

      String topicVersion = isStreamReprocessing ? Version.composeKafkaTopic(storeName, 4) : topicForStoreVersion3;
      assertEquals(
          hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicVersion),
          HybridStoreQuotaStatus.QUOTA_VIOLATED);
      assertTrue(offlinePushRepository.containsKafkaTopic(topicVersion));

      if (!recoverFromViolation) {
        sendStreamingRecord(veniceProducer, storeName, 21);
        Assert.fail("Exception should be thrown because quota violation happens.");
      } else {
        // Disable HybridStoreDiskQuota and verify quota status is changed to QUOTA_NOT_VIOLATED.
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridStoreDiskQuotaEnabled(false));
        Utils.sleep(normalTimeForConsuming);
        assertEquals(
            hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicVersion),
            HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);

        // Re-enable HybridStoreDiskQuota and increase the quota to 100x so that disk quota is not violated.
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridStoreDiskQuotaEnabled(true)
                .setStorageQuotaInByte(storageQuotaInByte * 100));
        Utils.sleep(normalTimeForConsuming);
        // Previous venice producer get exception thrown by quota violation. Need to create a new venice producer.
        if (veniceProducer != null) {
          veniceProducer.stop();
        }
        if (isStreamReprocessing) {
          veniceProducer = getSamzaProducer(sharedVenice, storeName, Version.PushType.STREAM_REPROCESSING); // new
                                                                                                            // producer,
                                                                                                            // new DIV
                                                                                                            // segment.
        } else {
          veniceProducer = getSamzaProducer(sharedVenice, storeName, Version.PushType.STREAM); // new producer, new DIV
                                                                                               // segment.
        }

        sendStreamingRecord(veniceProducer, storeName, 21);
        if (isStreamReprocessing) {
          // Version 4 does not exist anymore, new version created.
          String topicForStoreVersion5 = Version.composeKafkaTopic(storeName, 5);
          assertEquals(
              hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicForStoreVersion5),
              HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
          assertTrue(offlinePushRepository.containsKafkaTopic(topicForStoreVersion5));
        } else {
          assertEquals(
              hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(topicForStoreVersion3),
              HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
          assertTrue(offlinePushRepository.containsKafkaTopic(topicForStoreVersion3));
        }
      }

    } catch (VeniceException e) {
      if (recoverFromViolation) {
        LOGGER.error("Exception got during test of recovering exception: ", e);
        throw e;
      }
      // Expected
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
      if (offlinePushRepository != null) {
        offlinePushRepository.clear();
      }
      if (hybridStoreQuotaOnlyRepository != null) {
        hybridStoreQuotaOnlyRepository.clear();
      }
      if (readManager != null) {
        readManager.disconnect();
      }
    }
  }

}
