package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.HELIX_HYBRID_STORE_QUOTA_ENABLED;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_IDLE_INGESTION_TASK_CLEANUP_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
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
    // Added a server with shared consumer enabled.
    Properties serverPropertiesWithSharedConsumer = new Properties();
    serverPropertiesWithSharedConsumer.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    extraProperties.put(HELIX_HYBRID_STORE_QUOTA_ENABLED, true);
    extraProperties.put(HYBRID_QUOTA_ENFORCEMENT_ENABLED, true);
    extraProperties.put(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    extraProperties.put(CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS, TimeUnit.DAYS.toMillis(7) + "");
    extraProperties.put(CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS, 0);
    extraProperties.put(SERVER_IDLE_INGESTION_TASK_CLEANUP_INTERVAL_IN_SECONDS, "2");
    extraProperties.put(SERVER_UNSUB_AFTER_BATCHPUSH, "false");

    // N.B.: RF 2 with 3 servers is important, in order to test both the leader and follower code paths
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(3)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(1000000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(extraProperties)
        .build();
    sharedVenice = ServiceFactory.getVeniceCluster(options);
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
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
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
                    PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                    100L,
                    0L,
                    sharedVenice.getPubSubBrokerWrapper(),
                    sharedVenice.getPubSubTopicRepository())
                .getLocalTopicManager()) {

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
      long storageQuotaInByte = 20000; // A small quota, easily violated.

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

  @Test
  // test batch store quota updates
  public void testBatchStoreQuotaUpdates() throws IOException {
    String storeName = Utils.getUniqueString("test-batch-store-quota-updates");
    // Create store
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 100);

    Properties props = IntegrationTestPushUtils.defaultVPJProps(sharedVenice, inputDirPath, storeName);
    try (ControllerClient controllerClient = createStoreForJob(sharedVenice.getClusterName(), recordSchema, props)) {
      // run a push to create the store
      IntegrationTestPushUtils.runVPJ(props, 1, controllerClient);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });

      // Create a store with hybrid quota enabled
      ControllerResponse response =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(1024));
      Assert.assertFalse(response.isError());
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getStorageQuotaInByte(), 1024);
      });

      Utils.sleep(TimeUnit.SECONDS.toMillis(10));
      for (VeniceServerWrapper veniceServerWrapper: sharedVenice.getVeniceServers()) {
        printStorageQuotaUsage(veniceServerWrapper, storeName);
      }

      LOGGER.info("### new version push job started for store: {}", storeName);
      response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(-1));
      Assert.assertFalse(response.isError());
      // run another push to create a second version
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 10000);
      IntegrationTestPushUtils.runVPJ(props, 2, controllerClient);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });
      IntegrationTestPushUtils.runVPJ(props, 3, controllerClient);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 3);
      });
      // LOGGER.info(
      // "### All metrics: {}",
      // sharedVenice.getVeniceServers().get(0).getVeniceServer().getMetricsRepository().metrics().keySet());

      for (VeniceServerWrapper veniceServerWrapper: sharedVenice.getVeniceServers()) {
        boolean isQuotaViolated = printStorageQuotaUsage(veniceServerWrapper, storeName);
        LOGGER.info(
            "### Storage quota usage for store {} on server {}: isQuotaViolated: {}",
            storeName,
            veniceServerWrapper.getAddress(),
            isQuotaViolated);
        if (isQuotaViolated) {
          VeniceServerWrapper serverWrapper = sharedVenice.getVeniceServers().get(0);
          VeniceHelixAdmin helixAdmin = sharedVenice.getVeniceControllers().get(0).getVeniceHelixAdmin();
          String instance = Utils.getHelixNodeIdentifier(serverWrapper.getHost(), serverWrapper.getPort());
          helixAdmin.disableReplica(sharedVenice.getClusterName(), storeName + "_v3", instance, 1);
          break;
        }
      }

      Utils.sleep(30 * Time.MS_PER_SECOND);

      // update quota to 2048
      response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(512));
      Assert.assertFalse(response.isError());
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getStorageQuotaInByte(), 512);
      });
      for (VeniceServerWrapper veniceServerWrapper: sharedVenice.getVeniceServers()) {
        printStorageQuotaUsage(veniceServerWrapper, storeName);
      }
      Utils.sleep(TimeUnit.SECONDS.toMillis(20));
      for (VeniceServerWrapper veniceServerWrapper: sharedVenice.getVeniceServers()) {
        printStorageQuotaUsage(veniceServerWrapper, storeName);
      }

    }
  }

  private boolean printStorageQuotaUsage(VeniceServerWrapper veniceServerWrapper, String storeName) {
    MetricsRepository metricsRepository = veniceServerWrapper.getVeniceServer().getMetricsRepository();

    // Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedQPSString).value(), 1d / 30d, 0.05);
    // prod-ltx1/venice-server/venice-server-war.i001.venice-server.LSSSearchL2MemberEmbeddings--storage_quota_used.Gauge.rrd
    // String readQuotaRejectedQPSString = "." + storeName + "--storage_quota_used.Gauge";

    String storageQuotaUsedString = "." + storeName + "--storage_quota_used.Gauge";
    // _current--storage_quota_used.IngestionStatsGauge
    String currentVersionStorageQuotaUsedString = "." + storeName + "_current--storage_quota_used.IngestionStatsGauge";
    String futureVersionStorageQuotaUsedString = "." + storeName + "_future--storage_quota_used.IngestionStatsGauge";
    // .test-batch-store-quota-updates_67af944501383_cad489f0_total--storage_quota_used.IngestionStatsGauge
    String totalStorageQuotaUsedString = "." + storeName + "_total--storage_quota_used.IngestionStatsGauge";

    // add null check for the metric
    // double storageQuotaUsed = metricsRepository.getMetric(storageQuotaUsedString).value();
    double storageQuotaUsed = metricsRepository.getMetric(storageQuotaUsedString) != null
        ? metricsRepository.getMetric(storageQuotaUsedString).value()
        : 0.0;
    double currentVersionStorageQuotaUsed = metricsRepository.getMetric(currentVersionStorageQuotaUsedString) != null
        ? metricsRepository.getMetric(currentVersionStorageQuotaUsedString).value()
        : 0.0;
    double futureVersionStorageQuotaUsed = metricsRepository.getMetric(futureVersionStorageQuotaUsedString) != null
        ? metricsRepository.getMetric(futureVersionStorageQuotaUsedString).value()
        : 0.0;
    double totalStorageQuotaUsed = metricsRepository.getMetric(totalStorageQuotaUsedString) != null
        ? metricsRepository.getMetric(totalStorageQuotaUsedString).value()
        : 0.0;

    LOGGER.info(
        "#### ==> Storage quota used on: {} for store {}: storageQuotaUsed: {}, currentVersionStorageQuotaUsed: {}, futureVersionStorageQuotaUsed: {}, totalStorageQuotaUsed: {}",
        veniceServerWrapper.getAddress(),
        storeName,
        storageQuotaUsed,
        currentVersionStorageQuotaUsed,
        futureVersionStorageQuotaUsed,
        totalStorageQuotaUsed);

    // if non zero return true
    return currentVersionStorageQuotaUsed > 1.0d;
  }
}
