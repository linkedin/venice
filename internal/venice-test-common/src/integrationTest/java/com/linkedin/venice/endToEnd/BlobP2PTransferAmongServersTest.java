package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ADAPTIVE_THROTTLER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOB_TRANSFER_ADAPTIVE_THROTTLER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.StoreStatus.FULLLY_REPLICATED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class BlobP2PTransferAmongServersTest {
  private static final Logger LOGGER = LogManager.getLogger(BlobP2PTransferAmongServersTest.class);
  private static int PARTITION_COUNT = 3;
  private static final int STREAMING_RECORD_SIZE = 1024;
  private String path1;
  private String path2;
  private int server1Port;
  private int server2Port;
  private VeniceClusterWrapper cluster;

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(cluster);

    try {
      FileUtils.deleteDirectory(new File(path1));
      FileUtils.deleteDirectory(new File(path2));
    } catch (Exception e) {
      LOGGER.error("Failed to delete path1 or path2", e);
    }
  }

  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferAmongServersForBatchStore() throws Exception {
    cluster = initializeVeniceCluster();

    String storeName = "test-store";
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED);
    setUpBatchStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
    VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

    // verify the snapshot is not generated for both servers after the job is done
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath2)));
    }

    cluster.stopAndRestartVeniceServer(server1Port);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      Assert.assertTrue(server1.isRunning());
    });

    // wait for server 1
    cluster.getVeniceControllers().forEach(controller -> {
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        Assert.assertEquals(
            controller.getController()
                .getVeniceControllerService()
                .getVeniceHelixAdmin()
                .getAllStoreStatuses(cluster.getClusterName())
                .get(storeName),
            FULLLY_REPLICATED.toString());
      });
    });

    // the partition files should be transferred to server 1 and offset should be the same
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // ensure the snapshot file is not generated
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
        // at that moment, the path 2 snapshot should be created
        String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
        Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
      }
    });

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetServer1 = server1.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                "test-store_v1",
                partitionId,
                server1.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        OffsetRecord offsetServer2 = server2.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                "test-store_v1",
                partitionId,
                server2.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        Assert.assertEquals(
            offsetServer1.getCheckpointedLocalVtPosition(),
            offsetServer2.getCheckpointedLocalVtPosition());
      }
    });
  }

  /**
   * Test when the format of the rocksdb is different between two servers,
   * the blob transfer should throw an exception and return a 404 error.
   */
  @Test(singleThreaded = true, timeOut = 180000)
  public void testBlobTransferThrowExceptionIfTableFormatNotMatch() throws Exception {
    cluster = initializeVeniceCluster(Collections.singletonMap(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true"));

    String storeName = "test-store-format-not-match";
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED);
    setUpBatchStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);

    // The snapshot will not be generated, as none traffic is sent yet.
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath2)));
    }

    // restart server 1
    cluster.stopAndRestartVeniceServer(server1Port);
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      Assert.assertTrue(server1.isRunning());
    });

    // wait for server 1
    cluster.getVeniceControllers().forEach(controller -> {
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Assert.assertEquals(
            controller.getController()
                .getVeniceControllerService()
                .getVeniceHelixAdmin()
                .getAllStoreStatuses(cluster.getClusterName())
                .get(storeName),
            FULLLY_REPLICATED.toString());
      });
    });

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // the snapshot from server 2 should not be generated as it should throw 404 error once detect the table format
        // is not match.
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId));
        boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
      }
    });
  }

  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferAmongServersForBatchStoreWithRestoreTempFolder() throws Exception {
    cluster = initializeVeniceCluster();

    String storeName = "test-store";
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED);
    setUpBatchStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
    VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

    // verify the snapshot is not generated for both servers after the job is done
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertFalse(Files.exists(Paths.get(snapshotPath2)));
    }

    // stop server 1
    cluster.stopVeniceServer(server1Port);

    // prepare temp folder: move the partition folders to temp folders to simulate the restore from temp folder scenario
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String partitionPath = RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      String tempPartitionFolder =
          RocksDBUtils.composeTempPartitionDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Files.move(Paths.get(partitionPath), Paths.get(tempPartitionFolder));
      Assert.assertFalse(Files.exists(Paths.get(partitionPath)));
      Assert.assertTrue(Files.exists(Paths.get(tempPartitionFolder)));
    }

    // restart server 1
    cluster.restartVeniceServer(server1.getPort());
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      Assert.assertTrue(server1.isRunning());
    });

    // wait for server 1 is fully replicated
    cluster.getVeniceControllers().forEach(controller -> {
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        Assert.assertEquals(
            controller.getController()
                .getVeniceControllerService()
                .getVeniceHelixAdmin()
                .getAllStoreStatuses(cluster.getClusterName())
                .get(storeName),
            FULLLY_REPLICATED.toString());
      });
    });

    // the partition files should be transferred to server 1 and offset should be the same
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // ensure the snapshot file is not generated
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
        // at that moment, the path 2 snapshot should be created
        String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
        Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));

        // validate that no temp folder existed anymore
        String tempPartitionFolderPath1 =
            RocksDBUtils.composeTempPartitionDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
        Assert.assertFalse(Files.exists(Paths.get(tempPartitionFolderPath1)));
      }
    });

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetServer1 = server1.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                "test-store_v1",
                partitionId,
                server1.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        OffsetRecord offsetServer2 = server2.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                "test-store_v1",
                partitionId,
                server2.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        Assert.assertEquals(
            offsetServer1.getCheckpointedLocalVtPosition(),
            offsetServer2.getCheckpointedLocalVtPosition());
      }
    });
  }

  public VeniceClusterWrapper initializeVeniceCluster() {
    return initializeVeniceCluster(Collections.emptyMap());
  }

  public VeniceClusterWrapper initializeVeniceCluster(Map<String, String> configOverrideForSecondServer) {
    server1Port = -1;
    server2Port = -1;
    path1 = Utils.getTempDataDirectory().getAbsolutePath();
    path2 = Utils.getTempDataDirectory().getAbsolutePath();

    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(2)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .build();

    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(options);
    // add first server
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, path1);
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(port1));
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(port2));
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_SSL_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_ACL_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, "-1000000");

    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");

    server1Port = veniceClusterWrapper.addVeniceServer(serverFeatureProperties, serverProperties).getPort();

    // add second server
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, path2);
    serverProperties.setProperty(ConfigKeys.ENABLE_BLOB_TRANSFER, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(port2));
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(port1));

    // Add additional information for 2nd server.
    serverProperties.putAll(configOverrideForSecondServer);

    server2Port = veniceClusterWrapper.addVeniceServer(serverFeatureProperties, serverProperties).getPort();

    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceClusterWrapper.addVeniceRouter(routerProperties);

    return veniceClusterWrapper;
  }

  private void setUpBatchStore(
      VeniceClusterWrapper cluster,
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore) throws Exception {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    // Produce input data.
    TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir);

    // Setup VPJ job properties.
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(vpjProperties);
    // Create & update store for test.
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT); // Update the
    // partition count.
    paramsConsumer.accept(params);

    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, "\"string\"", vpjProperties)) {
      cluster.createMetaSystemStore(storeName);
      if (useDVCPushStatusStore) {
        cluster.createPushStatusSystemStore(storeName);
      }
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
      runVPJ(vpjProperties, 1, cluster);
    }
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    IntegrationTestPushUtils.runVPJ(vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  @Test(singleThreaded = true, timeOut = 240000, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBlobP2PTransferAmongServersForHybridStore(boolean enableAdaptiveThrottling) {
    Map<String, String> configOverride = new VeniceConcurrentHashMap<>();
    if (enableAdaptiveThrottling) {
      configOverride.put(SERVER_ADAPTIVE_THROTTLER_ENABLED, "true");
      configOverride.put(SERVER_BLOB_TRANSFER_ADAPTIVE_THROTTLER_ENABLED, "true");
    }
    cluster = initializeVeniceCluster(configOverride);

    ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    // prepare hybrid store.
    String storeName = "test-store-hybrid";
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;
    controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
    controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(streamingRewindSeconds)
            .setHybridOffsetLagThreshold(streamingMessageLag)
            .setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED));

    TestUtils.assertCommand(
        controllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, 120000));
    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
    VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

    // offset record should be same after the empty push
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetRecord1 = server1.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                storeName + "_v1",
                partitionId,
                server1.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        OffsetRecord offsetRecord2 = server2.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                storeName + "_v1",
                partitionId,
                server2.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        Assert.assertEquals(
            offsetRecord2.getCheckpointedLocalVtPosition(),
            offsetRecord1.getCheckpointedLocalVtPosition());
      }
    });

    // get the store info
    StoreInfo storeInfo = controllerClient.getClusterStores(cluster.getClusterName())
        .getStoreInfoList()
        .stream()
        .filter(store -> store.getName().equals(storeName))
        .findFirst()
        .get();

    Version latestVersion =
        storeInfo.getVersions().stream().max(Comparator.comparingInt(Version::getNumber)).orElse(null);

    Assert.assertEquals(
        latestVersion.getBlobTransferInServerEnabled(),
        ConfigCommonUtils.ActivationState.ENABLED.toString(),
        "Latest version " + latestVersion.getNumber() + " should have blob transfer enabled");

    // cleanup and stop server 1
    cluster.stopVeniceServer(server1Port);
    // verify that the server 1 is stopped but the files are retained.
    // No need to check the snapshot as when server 1 and 2 initially started, blob transfer is not generated.
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
      boolean fileExisted = Files.exists(file.toPath());
      Assert.assertTrue(fileExisted);
    }

    // send records to server 2 only
    SystemProducer veniceProducer = null;
    for (int i = 1; i <= 10; i++) {
      veniceProducer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
      IntegrationTestPushUtils.sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
    }
    if (veniceProducer != null) {
      veniceProducer.stop();
    }

    // restart server 1
    cluster.restartVeniceServer(server1.getPort());
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      Assert.assertTrue(server1.isRunning());
    });

    // wait for server 1 is fully replicated
    cluster.getVeniceControllers().forEach(controller -> {
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Assert.assertEquals(
            controller.getController()
                .getVeniceControllerService()
                .getVeniceHelixAdmin()
                .getAllStoreStatuses(cluster.getClusterName())
                .get(storeName),
            FULLLY_REPLICATED.toString());
      });
    });

    // Verify server 1 blob transfer is completed, and the snapshot is not generated.
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
        // Server 2 should have the snapshot file
        String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
        Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
      }
    });

    // server 1 and 2 offset record should be the same
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetServer1 = server1.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                storeName + "_v1",
                partitionId,
                server1.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        OffsetRecord offsetServer2 = server2.getVeniceServer()
            .getStorageMetadataService()
            .getLastOffset(
                storeName + "_v1",
                partitionId,
                server2.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
        Assert.assertEquals(
            offsetServer1.getCheckpointedLocalVtPosition(),
            offsetServer2.getCheckpointedLocalVtPosition());
      }
    });
  }

  /**
   * Test blob P2P transfer when incremental push is done before the blob transfer starts.
   * @throws Exception
   */
  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferWithIncrementalPushDoneBeforeBlobTransfer() throws Exception {
    cluster = initializeVeniceCluster();
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = "test-store-incremental-push";

    try (ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
      controllerClient
          .createNewStore(storeName, "owner", keySchemaStr, TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString());
      cluster.createMetaSystemStore(storeName);
      cluster.createPushStatusSystemStore(storeName);

      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setUnusedSchemaDeletionEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L)
              .setIncrementalPushEnabled(true)
              .setIsDavinciHeartbeatReported(true)
              .setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED));

      // Create initial version V1
      Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
      IntegrationTestPushUtils.runVPJ(props);

      VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
      VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

      // Incremental Push
      TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, 5);
      Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
      vpjProperties.put(INCREMENTAL_PUSH, true);
      IntegrationTestPushUtils.runVPJ(vpjProperties);
      cluster.waitVersion(storeName, 1);

      // Stop server 1
      cluster.stopVeniceServer(server1Port);

      // Verify partition files still exist on server 1
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Assert.assertTrue(Files.exists(file.toPath()));
      }

      // Restart server 1
      cluster.restartVeniceServer(server1.getPort());
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Assert.assertTrue(server1.isRunning());
      });

      // Wait for server 1 to be fully replicated
      cluster.getVeniceControllers().forEach(controller -> {
        TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
          Assert.assertEquals(
              controller.getController()
                  .getVeniceControllerService()
                  .getVeniceHelixAdmin()
                  .getAllStoreStatuses(cluster.getClusterName())
                  .get(storeName),
              FULLLY_REPLICATED.toString());
        });
      });

      // Verify blob transfer completed and snapshots are created appropriately
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          // Server 1 partition files should exist
          File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
          Assert.assertTrue(Files.exists(file.toPath()));

          // Server 1 snapshot should not be generated (receiving end)
          File snapshotFile =
              new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
          Assert.assertFalse(Files.exists(snapshotFile.toPath()));

          // Server 2 snapshot should be created (sending end)
          String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
          Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
        }
      });

      // Verify offset records for incremental push status are the same
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          OffsetRecord offsetServer1 = server1.getVeniceServer()
              .getStorageMetadataService()
              .getLastOffset(
                  storeName + "_v1",
                  partitionId,
                  server1.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
          OffsetRecord offsetServer2 = server2.getVeniceServer()
              .getStorageMetadataService()
              .getLastOffset(
                  storeName + "_v1",
                  partitionId,
                  server2.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());

          Assert.assertEquals(
              offsetServer1.getTrackingIncrementalPushStatus(),
              offsetServer2.getTrackingIncrementalPushStatus());
        }
      });
    }
  }

  /**
   * Test blob P2P transfer when incremental push is still in progress during the blob transfer.
   * @throws Exception
   */
  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferWithIncrementalPushInProgressDuringBlobTransfer() throws Exception {
    cluster = initializeVeniceCluster();
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = "test-store-incremental-push";

    try (ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
      controllerClient
          .createNewStore(storeName, "owner", keySchemaStr, TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString());
      cluster.createMetaSystemStore(storeName);
      cluster.createPushStatusSystemStore(storeName);

      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setUnusedSchemaDeletionEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L)
              .setIncrementalPushEnabled(true)
              .setIsDavinciHeartbeatReported(true)
              .setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED));

      // Create initial version V1
      Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
      IntegrationTestPushUtils.runVPJ(props);

      VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
      VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

      // Stop server 1
      cluster.stopVeniceServer(server1Port);
      // Restart server 1
      cluster.restartVeniceServer(server1.getPort());

      // Incremental Push, make the push take longer,
      // make sure that the blob transfer happens when incremental push is still in progress
      TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, 100000);
      Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
      vpjProperties.put(INCREMENTAL_PUSH, true);
      IntegrationTestPushUtils.runVPJ(vpjProperties);

      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Assert.assertTrue(server1.isRunning());
      });

      // Wait for server 1 to be fully replicated
      cluster.getVeniceControllers().forEach(controller -> {
        TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
          Assert.assertEquals(
              controller.getController()
                  .getVeniceControllerService()
                  .getVeniceHelixAdmin()
                  .getAllStoreStatuses(cluster.getClusterName())
                  .get(storeName),
              FULLLY_REPLICATED.toString());
        });
      });

      // Verify blob transfer completed and snapshots are created appropriately
      // Please noted that we don't need to compare the offset records,
      // because the blob transfer completed before incremental push is done, resulting that their offset record
      // incremental push status may not be the same.
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          // Server 1 partition files should exist
          File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
          Assert.assertTrue(Files.exists(file.toPath()));

          // Server 1 snapshot should not be generated (receiving end)
          File snapshotFile =
              new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
          Assert.assertFalse(Files.exists(snapshotFile.toPath()));

          // Server 2 snapshot should be created (sending end)
          String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
          Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
        }
      });
    }
  }
}
