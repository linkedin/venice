package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;
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
import java.util.Arrays;
import java.util.List;
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

    // verify the snapshot is generated for both servers after the job is done
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
    }

    // cleanup and restart server 1
    FileUtils.deleteDirectory(
        new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", METADATA_PARTITION_ID)));
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      FileUtils.deleteDirectory(
          new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId)));
      // both partition db and snapshot should be deleted
      Assert.assertFalse(
          Files.exists(
              Paths.get(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
      Assert.assertFalse(
          Files.exists(Paths.get(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
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
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // ensure the snapshot file is not generated
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
      }
    });

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetServer1 =
            server1.getVeniceServer().getStorageMetadataService().getLastOffset("test-store_v1", partitionId);
        OffsetRecord offsetServer2 =
            server2.getVeniceServer().getStorageMetadataService().getLastOffset("test-store_v1", partitionId);
        Assert.assertEquals(offsetServer1.getLocalVersionTopicOffset(), offsetServer2.getLocalVersionTopicOffset());
      }
    });
  }

  /**
   * If there are no snapshots available for the store on server2, the blob transfer should throw an exception and return a 404 error.
   * When server1 restarts and receives the 404 error from server2, it will switch to using Kafka to ingest the data.
   */
  @Test(singleThreaded = true, timeOut = 180000)
  public void testBlobTransferThrowExceptionIfSnapshotNotExisted() throws Exception {
    cluster = initializeVeniceCluster();
    String storeName = "test-store-snapshot-not-existed";
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
    setUpBatchStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServerByPort(server1Port);
    // verify the snapshot is generated for both servers after the job is done
    for (int i = 0; i < PARTITION_COUNT; i++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", i);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", i);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
    }

    // cleanup and restart server 1
    FileUtils.deleteDirectory(
        new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", METADATA_PARTITION_ID)));

    List<String> paths = Arrays.asList(path1, path2);

    // clean up all snapshot files on server 1 and server 2,
    // to simulate the case that snapshot is not existed on server2,
    // should return 404 in response
    for (String path: paths) {
      for (int i = 0; i < PARTITION_COUNT; i++) {
        FileUtils
            .deleteDirectory(new File(RocksDBUtils.composePartitionDbDir(path + "/rocksdb", storeName + "_v1", i)));
        // both partition db and snapshot should be deleted
        Assert.assertFalse(
            Files.exists(Paths.get(RocksDBUtils.composePartitionDbDir(path + "/rocksdb", storeName + "_v1", i))));
        Assert.assertFalse(
            Files.exists(Paths.get(RocksDBUtils.composeSnapshotDir(path + "/rocksdb", storeName + "_v1", i))));
      }
    }

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
      for (int i = 0; i < PARTITION_COUNT; i++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", i));
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        File snapshotFile = new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", i));
        Boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        // snapshot file should be generated as it is re-ingested, not from server2 file transfer
        Assert.assertTrue(snapshotFileExisted);
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

    // verify the snapshot is generated for both servers after the job is done
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      String snapshotPath1 = RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath1)));
      String snapshotPath2 = RocksDBUtils.composeSnapshotDir(path2 + "/rocksdb", storeName + "_v1", partitionId);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath2)));
    }

    // cleanup and restart server 1
    FileUtils.deleteDirectory(
        new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", METADATA_PARTITION_ID)));
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      FileUtils.deleteDirectory(
          new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId)));
      // both partition db and snapshot should be deleted
      Assert.assertFalse(
          Files.exists(
              Paths.get(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
      Assert.assertFalse(
          Files.exists(Paths.get(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
    }

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
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // ensure that the snapshot file is generated as it is re-ingested, not from server2 file transfer
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertTrue(snapshotFileExisted);
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
    serverProperties.put(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
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
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    TestWriteUtils.runPushJob(jobName, vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  @Test(singleThreaded = true, timeOut = 240000)
  public void testBlobP2PTransferAmongServersForHybridStore() throws Exception {
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
        Assert.assertEquals(offsetRecord2.getLocalVersionTopicOffset(), offsetRecord1.getLocalVersionTopicOffset());
      }
    });

    // cleanup and stop server 1
    cluster.stopVeniceServer(server1Port);
    FileUtils.deleteDirectory(
        new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", METADATA_PARTITION_ID)));
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      FileUtils.deleteDirectory(
          new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId)));
      // both partition db and snapshot should be deleted
      Assert.assertFalse(
          Files.exists(
              Paths.get(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
      Assert.assertFalse(
          Files.exists(Paths.get(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId))));
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

    cluster.restartVeniceServer(server1.getPort());
    // restart server 1
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

    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
        // ensure that the snapshot is not generated.
        File snapshotFile =
            new File(RocksDBUtils.composeSnapshotDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean snapshotFileExisted = Files.exists(snapshotFile.toPath());
        Assert.assertFalse(snapshotFileExisted);
      }
    });

    // server 1 and 2 offset record should be the same
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        OffsetRecord offsetServer1 =
            server1.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        OffsetRecord offsetServer2 =
            server2.getVeniceServer().getStorageMetadataService().getLastOffset(storeName + "_v1", partitionId);
        Assert.assertEquals(offsetServer1.getLocalVersionTopicOffset(), offsetServer2.getLocalVersionTopicOffset());
      }
    });
  }
}
