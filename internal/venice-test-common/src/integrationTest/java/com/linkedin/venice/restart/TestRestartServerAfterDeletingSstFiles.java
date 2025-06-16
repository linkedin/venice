package com.linkedin.venice.restart;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;

import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngineAccessor;
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
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test was added to mimic the below state:
 * 1. {@link org.rocksdb.RocksDB#ingestExternalFile} moved the locally created sst files to the DB
 *    after receiving EOP.
 * 2. {@link com.linkedin.venice.offsets.OffsetRecord} with EOP set as true is not synced yet
 * 3. process crashes
 *
 * When the process restarts, it will notice the sst files are not found and restart the ingestion from scratch.
 */
public class TestRestartServerAfterDeletingSstFiles {
  private static final Logger LOGGER = LogManager.getLogger(TestRestartServerAfterDeletingSstFiles.class);

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private int newVersion = 0;

  private VeniceWriter<String, String, byte[]> veniceWriter;

  private final int numKeys = 300;
  private int startingKey = 0;
  private final String keyPrefix = "key_";
  private final String valuePrefix = "value_";
  AvroGenericStoreClient<String, Object> storeClient;

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

    // Create one server
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.setProperty(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath());
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(0));

    // AvroGenericStoreClient: to verify if the data is ingested
    storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
  }

  /**
   * Baseline: Server is not restarted
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithOutServerRestart() throws Exception {
    // Create new version
    createNewVersionAndStartIngestion();
    endIngestion();

    verifyIngestion();
    IOUtils.closeQuietly(veniceWriter);
  }

  /**
   * Servers are restarted in between ingestion but SST files are not deleted,
   * so the ingestion will use the existing SST files based on checkpointing.
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithServerRestart() throws Exception {
    createNewVersionAndStartIngestion();

    VeniceServerWrapper server = veniceCluster.getVeniceServers().get(0);

    // restart the venice servers: Mimic Process crash and restart
    restartServerByDeletingSSTFiles(server, false, null);

    endIngestion();

    verifyIngestion();
    IOUtils.closeQuietly(veniceWriter);
  }

  /**
   * Servers are restarted in between ingestion and SST files are manually deleted
   * to force reingest from beginning
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWithServerRestartWithDeletedSSTFiles() throws Exception {
    createNewVersionAndStartIngestion();

    VeniceServerWrapper server = veniceCluster.getVeniceServers().get(0);
    TestVeniceServer testVeniceServer = server.getVeniceServer();
    StorageService storageService = testVeniceServer.getStorageService();
    RocksDBStorageEngine rocksDBStorageEngine = StorageEngineAccessor
        .getInnerStorageEngine(storageService.getStorageEngineRepository().getLocalStorageEngine(storeVersionName));

    LOGGER.info("Waiting for the process to Finish ingesting all the data to sst files");
    // Verify the total number of records ingested
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      long totalIngestedKeys = rocksDBStorageEngine.getPartitionIds().stream().mapToLong(id -> {
        ReadWriteLock rwLock = rocksDBStorageEngine.getRWLockForPartitionOrThrow(id);
        try {
          rwLock.readLock().lock();
          RocksDBStoragePartition partition = (RocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(id);
          Assert.assertNotNull(partition.getRocksDBSstFileWriter());
          return partition.getRocksDBSstFileWriter().getRecordNumInAllSSTFiles();
        } finally {
          rwLock.readLock().unlock();
        }
      }).sum();
      Assert.assertEquals(totalIngestedKeys, numKeys);
    });

    restartServerByDeletingSSTFiles(server, true, rocksDBStorageEngine);

    endIngestion();
    verifyIngestion();
    IOUtils.closeQuietly(veniceWriter);
  }

  private void createNewVersionAndStartIngestion() {
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, true, true);
    storeVersionName = creationResponse.getKafkaTopic();
    veniceWriter = veniceCluster.getVeniceWriter(storeVersionName);
    int versionToBePushed = Version.parseVersionFromKafkaTopicName(storeVersionName);
    Assert.assertEquals(newVersion + 1, versionToBePushed);
    newVersion = versionToBePushed;
    LOGGER.info("Store's current version is: {} and Push will create a new version: {}", newVersion - 1, newVersion);

    // Different set of keys for different version
    startingKey += numKeys;

    int currkey = startingKey;
    int endKey = startingKey + numKeys;

    // Start of Push initiated
    veniceWriter.broadcastStartOfPush(true, new HashMap<>());

    // Insert test records
    while (currkey < endKey) {
      veniceWriter.put(keyPrefix + currkey, valuePrefix + currkey, valueSchemaId);
      currkey++;
    }

    // EOP will be sent in endIngestion()
  }

  private void endIngestion() {
    // Write end of push message to finish the job
    veniceWriter.broadcastEndOfPush(new HashMap<>());
  }

  private void verifyIngestion() throws ExecutionException, InterruptedException {
    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      LOGGER.info("currentVersion {}, pushVersion {}", currentVersion, newVersion);
      return currentVersion == newVersion;
    });

    // use client to query for all the keys
    // 1. invalid key
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertNull(storeClient.get(keyPrefix + (startingKey - 1)).get());
    });

    // 2. all valid keys
    int currkey = startingKey;
    int endKey = startingKey + numKeys;
    while (currkey < endKey) {
      Assert.assertEquals(storeClient.get(keyPrefix + currkey).get().toString(), valuePrefix + currkey);
      currkey++;
    }
  }

  /**
   * Mimic non-graceful shutdown of the servers in the midst of sst files being moved to RocksDB: Mimic process crash
   * and restart after ingestExternalFile() completes but before EOP was synced to OffsetRecord
   *
   * 1. Stop the server
   * 2. delete the SST files (Deleting before graceful shutdown will not help
   *    mimic this scenario as there will be a sync during the graceful shutdown)
   * 3. start the server
   */
  private void restartServerByDeletingSSTFiles(
      VeniceServerWrapper server,
      boolean deleteSSTFiles,
      RocksDBStorageEngine rocksDBStorageEngine) {
    veniceCluster.stopVeniceServer(server.getPort());

    if (deleteSSTFiles) {
      // Delete the sst files to mimic how ingestExternalFile() moves them to RocksDB.
      LOGGER.info("Delete the sst files");
      rocksDBStorageEngine.getPartitionIds().forEach(id -> {
        ReadWriteLock rwLock = rocksDBStorageEngine.getRWLockForPartitionOrThrow(id);
        try {
          rwLock.writeLock().lock();
          RocksDBStoragePartition partition = (RocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(id);
          partition.deleteFilesInDirectory(partition.getFullPathForTempSSTFileDir());
        } finally {
          rwLock.writeLock().unlock();
        }
      });
    }

    veniceCluster.restartVeniceServer(server.getPort());
  }
}
