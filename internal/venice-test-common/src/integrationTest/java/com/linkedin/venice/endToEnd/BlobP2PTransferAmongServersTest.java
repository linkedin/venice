package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.StoreStatus.FULLLY_REPLICATED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobP2PTransferAmongServersTest {
  private static final Logger LOGGER = LogManager.getLogger(BlobP2PTransferAmongServersTest.class);
  private static int PARTITION_COUNT = 3;
  private String path1;
  private String path2;
  private VeniceClusterWrapper cluster;

  @BeforeMethod
  public void setUp() {
    cluster = initializeVeniceCluster();
  }

  @AfterMethod
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(cluster);

    try {
      FileUtils.deleteDirectory(new File(path1));
      FileUtils.deleteDirectory(new File(path2));
    } catch (Exception e) {
      LOGGER.error("Failed to delete path1 or path2", e);
    }
  }

  @Test(singleThreaded = true)
  public void testBlobP2PTransferAmongServers() throws Exception {
    String storeName = "test-store";
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
    setUpStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServers().get(0);
    VeniceServerWrapper server2 = cluster.getVeniceServers().get(1);

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

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      cluster.stopAndRestartVeniceServer(server1.getPort());
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

    // the partition files should be transferred to server 1 and offset should be the same
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        File file = new File(RocksDBUtils.composePartitionDbDir(path1 + "/rocksdb", storeName + "_v1", partitionId));
        Boolean fileExisted = Files.exists(file.toPath());
        Assert.assertTrue(fileExisted);
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
  @Test(singleThreaded = true)
  public void testBlobTransferThrowExceptionIfSnapshotNotExisted() throws Exception {
    String storeName = "test-store-snapshot-not-existed";
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
    setUpStore(cluster, storeName, paramsConsumer, properties -> {}, true);

    VeniceServerWrapper server1 = cluster.getVeniceServers().get(0);

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

    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      cluster.stopAndRestartVeniceServer(server1.getPort());
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

  public VeniceClusterWrapper initializeVeniceCluster() {
    path1 = Utils.getTempDataDirectory().getAbsolutePath();
    path2 = Utils.getTempDataDirectory().getAbsolutePath();

    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }

    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1, 0, 0, 2);
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
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);

    // add second server
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, path2);
    serverProperties.setProperty(ConfigKeys.ENABLE_BLOB_TRANSFER, "true");
    serverProperties.setProperty(ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(port2));
    serverProperties.setProperty(ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(port1));
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceClusterWrapper.addVeniceRouter(routerProperties);

    return veniceClusterWrapper;
  }

  private void setUpStore(
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
}
