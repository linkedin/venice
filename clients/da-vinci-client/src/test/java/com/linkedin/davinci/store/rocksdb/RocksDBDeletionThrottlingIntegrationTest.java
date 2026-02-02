package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RocksDBDeletionThrottlingIntegrationTest {
  private RocksDBStorageEngineFactory factory;
  private VeniceProperties veniceServerProperties;
  private String testDataPath;

  @BeforeMethod
  public void setUp() {
    testDataPath = Utils.getUniqueTempPath();
  }

  @AfterMethod
  public void tearDown() {
    if (factory != null) {
      factory.close();
    }
    if (testDataPath != null) {
      try {
        FileUtils.deleteDirectory(new File(testDataPath));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Test backup version deletion with deletion rate limiting enabled.
   * This test creates a store with ~200 MB of data, then deletes it with rate limiting
   * configured to 20 MB/s. The deletion should take around 10 seconds.
   */
  @Test
  public void testBackupVersionDeletionWithRateLimitingLargeStore() throws Exception {
    long deletionRateBytesPerSec = 20L * 1024 * 1024; // 20 MB/s (slower to verify throttling)

    veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put("data.base.path", testDataPath)
        .put(
            RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND,
            String.valueOf(deletionRateBytesPerSec))
        .build();

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);
    Assert.assertNotNull(factory.getSstFileManager(), "SstFileManager should be created");

    // Create a storage engine (simulating a store version)
    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("large_test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);

    // Add multiple partitions
    int numPartitions = 4;
    for (int p = 0; p < numPartitions; p++) {
      storeEngine.addStoragePartitionIfAbsent(p);
    }

    // Write ~200 MB of data to generate SST files
    long targetDataSizeBytes = 200L * 1024 * 1024; // 200 MB
    long valueSizeBytes = 50 * 1024; // 50 KB per value
    long numRecordsPerPartition = targetDataSizeBytes / (numPartitions * valueSizeBytes);

    Random random = new Random(42);
    byte[] valueBytes = new byte[(int) valueSizeBytes];

    for (int partition = 0; partition < numPartitions; partition++) {
      for (int i = 0; i < numRecordsPerPartition; i++) {
        // Generate unique key for each record
        String keyString = "key_p" + partition + "_" + i;
        byte[] keyBytes = keyString.getBytes();

        // Generate pseudo-random value data
        random.nextBytes(valueBytes);

        storeEngine.put(partition, keyBytes, ByteBuffer.wrap(valueBytes));
      }
    }

    // Verify storage engine is created and has data
    Assert.assertTrue(!storeEngine.getPartitionIds().isEmpty(), "Storage engine should have partitions");
    File storeDir = new File(factory.getRocksDBPath(testStore, 0)).getParentFile();
    Assert.assertTrue(storeDir.exists(), "Store directory should exist");

    // Record start time
    long startTimeMs = System.currentTimeMillis();

    // Delete the storage engine (simulating backup version deletion)
    // This should trigger RocksDB.destroyDB() with rate-limited SstFileManager
    factory.removeStorageEngine(storeEngine);

    long deletionTimeMs = System.currentTimeMillis() - startTimeMs;
    double deletionTimeSec = deletionTimeMs / 1000.0;

    // Verify the store directory is deleted
    Assert.assertFalse(storeDir.exists(), "Store directory should be deleted after removeStorageEngine");

    // With 20 MB/s rate limit and ~200 MB of data, deletion should take around 10 seconds
    Assert.assertTrue(
        deletionTimeSec >= 8,
        String.format(
            "Deletion should take at least 8 seconds with 20 MB/s rate limit but took %.2f seconds. "
                + "This suggests rate limiting may not be working correctly.",
            deletionTimeSec));
  }

  /**
   * Test backup version deletion WITHOUT rate limiting for comparison.
   *
   * This test creates a similar store but with rate limiting disabled (deletionRate=0).
   * The deletion should complete much faster than the rate-limited test.
   */
  @Test
  public void testBackupVersionDeletionWithoutRateLimiting() throws Exception {
    // Configure with rate limiting DISABLED (0 = disabled)
    long deletionRateBytesPerSec = 0L; // Disabled

    veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put("data.base.path", testDataPath)
        .put(
            RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND,
            String.valueOf(deletionRateBytesPerSec))
        .build();

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("unthrottled_test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);

    // Add multiple partitions
    int numPartitions = 4;
    for (int p = 0; p < numPartitions; p++) {
      storeEngine.addStoragePartitionIfAbsent(p);
    }

    // Write ~200 MB of data
    long targetDataSizeBytes = 200L * 1024 * 1024;
    long valueSizeBytes = 50 * 1024;
    long numRecordsPerPartition = targetDataSizeBytes / (numPartitions * valueSizeBytes);

    Random random = new Random(42);
    byte[] valueBytes = new byte[(int) valueSizeBytes];

    for (int partition = 0; partition < numPartitions; partition++) {
      for (int i = 0; i < numRecordsPerPartition; i++) {
        String keyString = "key_p" + partition + "_" + i;
        byte[] keyBytes = keyString.getBytes();
        random.nextBytes(valueBytes);
        storeEngine.put(partition, keyBytes, ByteBuffer.wrap(valueBytes));
      }
    }

    File storeDir = new File(factory.getRocksDBPath(testStore, 0)).getParentFile();
    long startTimeMs = System.currentTimeMillis();

    factory.removeStorageEngine(storeEngine);

    long deletionTimeMs = System.currentTimeMillis() - startTimeMs;
    double deletionTimeSec = deletionTimeMs / 1000.0;

    Assert.assertFalse(storeDir.exists(), "Store directory should be deleted");

    // Without rate limiting, deletion should be fast (< 5 seconds even on slow HDDs)
    Assert.assertTrue(
        deletionTimeSec < 5.0,
        String.format("Unthrottled deletion should be fast (< 5 seconds), but took %.2f seconds", deletionTimeSec));
  }

}
