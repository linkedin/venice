package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.StoreStatus.FULLLY_REPLICATED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
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
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
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
        OffsetRecord offsetServer1 =
            server1.getVeniceServer().getStorageMetadataService().getLastOffset("test-store_v1", partitionId);
        OffsetRecord offsetServer2 =
            server2.getVeniceServer().getStorageMetadataService().getLastOffset("test-store_v1", partitionId);
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
    cluster = initializeVeniceCluster(false);

    String storeName = "test-store-format-not-match";
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
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

  public VeniceClusterWrapper initializeVeniceCluster() {
    return initializeVeniceCluster(true);
  }

  public VeniceClusterWrapper initializeVeniceCluster(boolean sameRocksDBFormat) {
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
    serverProperties.setProperty(ConfigKeys.ENABLE_BLOB_TRANSFER, "true");
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, path1);
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(port1));
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(port2));
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_SSL_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_ACL_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, "-1000000");

    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");

    veniceClusterWrapper.addVeniceServer(serverFeatureProperties, serverProperties);
    // get the first port id for finding first server.
    server1Port = veniceClusterWrapper.getVeniceServers().get(0).getPort();

    // add second server
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, path2);
    serverProperties.setProperty(ConfigKeys.ENABLE_BLOB_TRANSFER, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(port2));
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(port1));

    if (!sameRocksDBFormat) {
      // the second server use PLAIN_TABLE_FORMAT
      serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    }

    veniceClusterWrapper.addVeniceServer(serverFeatureProperties, serverProperties);
    // get the second port num for finding second server,
    // because the order of servers is not guaranteed, need to exclude the first server.
    for (VeniceServerWrapper server: veniceClusterWrapper.getVeniceServers()) {
      if (server.getPort() != server1Port) {
        server2Port = server.getPort();
      }
    }

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

  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferAmongServersForHybridStore() {
    cluster = initializeVeniceCluster();

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
            .setBlobTransferEnabled(true));

    TestUtils.assertCommand(
        controllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, 120000));
    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
    VeniceServerWrapper server2 = cluster.getVeniceServerByPort(server2Port);

    // offset record should be same after the empty push
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetRecord1 =
            server1.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        OffsetRecord offsetRecord2 =
            server2.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        Assert.assertEquals(
            offsetRecord2.getCheckpointedLocalVtPosition(),
            offsetRecord1.getCheckpointedLocalVtPosition());
      }
    });

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
        OffsetRecord offsetServer1 =
            server1.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        OffsetRecord offsetServer2 =
            server2.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        Assert.assertEquals(
            offsetServer1.getCheckpointedLocalVtPosition(),
            offsetServer2.getCheckpointedLocalVtPosition());
      }
    });
  }
}
