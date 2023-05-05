package com.linkedin.venice.restart;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngine;
import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test was added to mimic {@link org.rocksdb.RocksDB#ingestExternalFile} completed moving
 * the locally created sst files to the DB but {@link com.linkedin.venice.offsets.OffsetRecord}
 * with EOP is not synced yet leading to restart of ingestion which will notice that the sst
 * files are missing and start the ingestion from scratch
 */
public class TestRestartServerAfterDeletingSstFiles {
  private static final Logger LOGGER = LogManager.getLogger(TestRestartServerAfterDeletingSstFiles.class);

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private VeniceWriter<String, String, byte[]> veniceWriter;

  final int numKeys = 300;
  final String keyPrefix = "key_";
  final String valuePrefix = "value_";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfRouters(1)
            .numberOfServers(0)
            .replicationFactor(1)
            .partitionSize(1)
            .build());

    // Create store first
    storeName = Utils.getUniqueString("testRestart");
    veniceCluster.getNewStore(storeName);

    // Create two servers, and one with early termination enabled, and one without
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.setProperty(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath());
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(0));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
  }

  /**
   * Baseline: Ingestion and no servers restarted
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithOutServerRestart() throws Exception {
    // Create new version
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, true);
    storeVersionName = creationResponse.getKafkaTopic();
    veniceWriter = veniceCluster.getVeniceWriter(storeVersionName);
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);
    final int numKeys = 300;
    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    // Insert test records
    for (int i = 0; i < numKeys; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId);
    }

    // Write end of push message to finish the job
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });

    // Test with AvroGenericStoreClient: verify if all the data is ingested even with a restart
    AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));

    int currkey = 0;
    while (++currkey < numKeys) {
      Assert.assertEquals(storeClient.get(keyPrefix + currkey).get().toString(), valuePrefix + currkey);
    }
    IOUtils.closeQuietly(veniceWriter);
  }

  /**
   * Servers are restarted in between ingestion but SST files are not manually deleted
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithServerRestart() throws Exception {
    // Create new version
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, true);
    storeVersionName = creationResponse.getKafkaTopic();
    veniceWriter = veniceCluster.getVeniceWriter(storeVersionName);
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);
    final int numKeys = 300;
    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    // Insert test records
    for (int i = 0; i < numKeys; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId);
    }

    // Get the only available server and the storage partition and delete the sst files mimicing
    // the ingestExternalFile deleting the files by moving them to DB
    VeniceServerWrapper server = veniceCluster.getVeniceServers().get(0);

    // restart the venice servers: Mimic SN restart after ingestExternalFile() but before EOP sync to OffsetRecord
    veniceCluster.stopVeniceServer(server.getPort());
    veniceCluster.restartVeniceServer(server.getPort());

    // Write end of push message to finish the job
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      LOGGER.info("currentVersion {}, pushVersion {}", currentVersion, pushVersion);
      return currentVersion == pushVersion;
    });

    // Test with AvroGenericStoreClient: verify if all the data is ingested even with a restart
    AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));

    int currkey = 0;
    while (++currkey < numKeys) {
      Assert.assertEquals(storeClient.get(keyPrefix + currkey).get().toString(), valuePrefix + currkey);
    }
    IOUtils.closeQuietly(veniceWriter);
  }

  /**
   * Servers are restarted in between ingestion and SST files are manually deleted
   * to force reingest from beginning
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithServerRestartWithDeletedSSTFiles() throws Exception {
    // Create new version
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, true);
    storeVersionName = creationResponse.getKafkaTopic();
    veniceWriter = veniceCluster.getVeniceWriter(storeVersionName);
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);
    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    // Insert test records
    for (int i = 0; i < numKeys; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId);
    }

    // Get the only available server and the storage partition and delete the sst files mimicing
    // the ingestExternalFile deleting the files by moving them to DB
    VeniceServerWrapper server = veniceCluster.getVeniceServers().get(0);
    TestVeniceServer testVeniceServer = server.getVeniceServer();
    StorageService storageService = testVeniceServer.getStorageService();
    RocksDBStorageEngine rocksDBStorageEngine =
        (RocksDBStorageEngine) storageService.getStorageEngineRepository().getLocalStorageEngine(storeVersionName);
    List<RocksDBStoragePartition> rocksDBStoragePartitions = new ArrayList<>();
    rocksDBStoragePartitions.add((RocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(0));
    rocksDBStoragePartitions.add((RocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(1));
    rocksDBStoragePartitions.add((RocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(2));

    LOGGER.info("Waiting for the SN to Finish ingesting all the data to sst files");
    // 1. wait for rocksDBSstFileWritter to be opened
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      rocksDBStoragePartitions.stream().forEach(partition -> {
        Assert.assertNotNull(partition.rocksDBSstFileWritter);
      });
    });

    // 2. verify the total number of records ingested
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      AtomicInteger totalIngestedKeys = new AtomicInteger();
      rocksDBStoragePartitions.stream().forEach(partition -> {
        totalIngestedKeys.addAndGet((int) partition.rocksDBSstFileWritter.getRecordNumInAllSSTFiles());
      });
      Assert.assertEquals(totalIngestedKeys.get(), numKeys);
    });

    LOGGER.info("Finished Ingestion of all data to SST Files: Delete the sst files");
    rocksDBStoragePartitions.stream().forEach(partition -> {
      partition.deleteSSTFiles(partition.getFullPathForTempSSTFileDir());
    });

    // restart the venice servers: Mimic SN restart after ingestExternalFile() but before EOP sync to OffsetRecord
    veniceCluster.stopVeniceServer(server.getPort());
    veniceCluster.restartVeniceServer(server.getPort());

    // Write end of push message to finish the job
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      LOGGER.info("currentVersion {}, pushVersion {}", currentVersion, pushVersion);
      return currentVersion == pushVersion;
    });

    // Test with AvroGenericStoreClient: verify if all the data is ingested even with a restart
    AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));

    int currkey = 0;
    while (++currkey < numKeys) {
      Assert.assertEquals(storeClient.get(keyPrefix + currkey).get().toString(), valuePrefix + currkey);
    }
    IOUtils.closeQuietly(veniceWriter);
  }
}
