package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOB_FILES_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOB_FILE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOB_FILE_STARTING_LEVEL;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_IMPLEMENTATION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_FOR_READ_WRITE_LEADER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_FOR_READ_WRITE_LEADER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_FOR_READ_WRITE_LEADER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_MAX_MEMTABLE_COUNT;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_MEMTABLE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_MIN_BLOB_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.Checkpoint;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBStoragePartitionTest {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBStoragePartitionTest.class);
  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();
  private static final String KEY_PREFIX = "key_";
  private static final String VALUE_PREFIX = "value_";
  private static final RocksDBThrottler ROCKSDB_THROTTLER = new RocksDBThrottler(3);

  private static final String BLOB_GARBAGE_METRIC = "rocksdb.live-blob-file-garbage-size";
  private static final List<String> BLOB_METRIC_LIST = Arrays.asList(
      "rocksdb.num-blob-files",
      "rocksdb.total-blob-file-size",
      "rocksdb.live-blob-file-size",
      BLOB_GARBAGE_METRIC);

  private Map<String, String> generateInput(int recordCnt, boolean sorted, int padLength) {
    return generateInput(recordCnt, sorted, padLength, 0);
  }

  private Map<String, String> generateInput(int recordCnt, boolean sorted, int padLength, int startIdx) {
    Map<String, String> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        ByteBuffer b1 = ByteBuffer.wrap(o1.getBytes());
        ByteBuffer b2 = ByteBuffer.wrap(o2.getBytes());
        return comparator.compare(b1, b2);
      });
    } else {
      records = new HashMap<>();
    }
    for (int i = startIdx; i < recordCnt + startIdx; ++i) {
      String value = VALUE_PREFIX + i;
      if (padLength > 0) {
        value += RandomStringUtils.random(padLength, true, true);
      }
      records.put(KEY_PREFIX + i, value);
    }
    return records;
  }

  private String getTempDatabaseDir(String storeName) {
    File storeDir = new File(DATA_BASE_DIR, storeName).getAbsoluteFile();
    if (!storeDir.mkdirs()) {
      throw new VeniceException("Failed to mkdirs for path: " + storeDir.getPath());
    }
    storeDir.deleteOnExit();
    return storeDir.getPath();
  }

  private void removeDir(String path) {
    File file = new File(path);
    if (file.exists() && !file.delete()) {
      throw new VeniceException("Failed to remove path: " + path);
    }
  }

  @Test
  public void testBlobDBCompatibility() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    String dbFolder = RocksDBUtils.composePartitionDbDir(DATA_BASE_DIR, storeName, partitionId);
    File dbDir = new File(dbFolder);

    Supplier<String[]> sstFileFinder = () -> dbDir.list(((dir, name) -> name.endsWith(".sst")));
    Supplier<String[]> blobFileFinder = () -> dbDir.list(((dir, name) -> name.endsWith(".blob")));

    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);

    int largeRecordPaddingLength = 10000;
    Map<String, String> largeInputRecords = generateInput(1000, false, largeRecordPaddingLength, 0);
    Map<String, String> smallInputRecords = generateInput(1000, false, 10, 10000);
    List<Map.Entry<String, String>> largeEntryList = new ArrayList<>(largeInputRecords.entrySet());
    List<Map.Entry<String, String>> smallEntryList = new ArrayList<>(smallInputRecords.entrySet());
    Properties extraProps = new Properties();
    // Disable blob files
    extraProps.put(ROCKSDB_BLOB_FILES_ENABLED, "false");
    extraProps.put(ROCKSDB_MIN_BLOB_SIZE_IN_BYTES, "1000"); // make sure the threshold is larger than small records
                                                            // generated
    extraProps.put(ROCKSDB_BLOB_FILE_SIZE_IN_BYTES, "2097152");
    extraProps.put(ROCKSDB_BLOB_FILE_STARTING_LEVEL, "0");
    extraProps.put(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, "1048576"); // 1MB

    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    // Insert the first 300 [0, 300) entries with blob db disabled
    for (int i = 0; i < 300; i++) {
      storagePartition.put(largeEntryList.get(i).getKey().getBytes(), largeEntryList.get(i).getValue().getBytes());
      storagePartition.put(smallEntryList.get(i).getKey().getBytes(), smallEntryList.get(i).getValue().getBytes());
    }
    storagePartition.close();
    // Make sure no blob files were generated
    assertTrue(sstFileFinder.get().length > 0);
    assertTrue(blobFileFinder.get().length == 0);

    // Enable blob files
    extraProps.put(ROCKSDB_BLOB_FILES_ENABLED, "true");

    veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
    rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);
    serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);
    storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    // Insert [300, 700) entries with blob db enabled
    for (int i = 300; i < 700; i++) {
      storagePartition.put(largeEntryList.get(i).getKey().getBytes(), largeEntryList.get(i).getValue().getBytes());
      storagePartition.put(smallEntryList.get(i).getKey().getBytes(), smallEntryList.get(i).getValue().getBytes());
    }
    storagePartition.sync();
    // Make sure blob files were generated
    assertTrue(sstFileFinder.get().length > 0);
    int blobFileCnt = blobFileFinder.get().length;
    assertTrue(blobFileCnt > 0);
    // Validate all the entries inserted so far
    for (int i = 0; i < 700; i++) {
      Assert.assertEquals(
          storagePartition.get(largeEntryList.get(i).getKey().getBytes()),
          largeEntryList.get(i).getValue().getBytes());
      Assert.assertEquals(
          storagePartition.get(smallEntryList.get(i).getKey().getBytes()),
          smallEntryList.get(i).getValue().getBytes());
    }
    storagePartition.sync();
    assertTrue(
        storagePartition.getPartitionSizeInBytes() > 700l
            * (KEY_PREFIX.length() + VALUE_PREFIX.length() * 2 + largeRecordPaddingLength));

    storagePartition.close();

    // Disable blob files
    extraProps.put(ROCKSDB_BLOB_FILES_ENABLED, "false");

    veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
    rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);
    serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);
    storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    // Insert [700, 1000) entries with blob db enabled
    for (int i = 700; i < 1000; i++) {
      storagePartition.put(largeEntryList.get(i).getKey().getBytes(), largeEntryList.get(i).getValue().getBytes());
      storagePartition.put(smallEntryList.get(i).getKey().getBytes(), smallEntryList.get(i).getValue().getBytes());
    }

    storagePartition.sync();
    assertTrue(
        storagePartition.getPartitionSizeInBytes() > 1000l
            * (KEY_PREFIX.length() + VALUE_PREFIX.length() * 2 + largeRecordPaddingLength));
    // Make sure no new blob files were generated
    assertEquals(blobFileFinder.get().length, blobFileCnt);
    // Validate all the entries inserted previously
    for (Map.Entry<String, String> entry: largeEntryList) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }
    for (Map.Entry<String, String> entry: smallEntryList) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }

    storagePartition.close();
    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test(dataProvider = "Six-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIngestion(
      boolean sorted,
      boolean interrupted,
      boolean reopenDatabaseDuringInterruption,
      boolean verifyChecksum,
      boolean enableBlobFile,
      boolean enablePlainTable) {
    CheckSum runningChecksum = CheckSum.getInstance(CheckSumType.MD5);
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(sorted);
    Options options = new Options();
    options.setCreateIfMissing(true);

    int padding = 100;
    int numberOfRecords = 101000;
    Map<String, String> inputRecords = generateInput(numberOfRecords, sorted, padding);
    Properties extraProps = new Properties();
    if (enableBlobFile) {
      extraProps.put(ROCKSDB_BLOB_FILES_ENABLED, "true");
      extraProps.put(ROCKSDB_MIN_BLOB_SIZE_IN_BYTES, "10");
      extraProps.put(ROCKSDB_BLOB_FILE_SIZE_IN_BYTES, "2097152");
      extraProps.put(ROCKSDB_BLOB_FILE_STARTING_LEVEL, "0");
    }
    if (enablePlainTable) {
      extraProps.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    }
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    final int syncPerRecords = 100;
    final int interruptedRecord = 345;

    Optional<Supplier<byte[]>> checksumSupplier = Optional.empty();
    if (verifyChecksum) {
      checksumSupplier = Optional.of(() -> {
        byte[] checksum = runningChecksum.getCheckSum();
        runningChecksum.reset();
        return checksum;
      });
    }
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);
    }
    int currentRecordNum = 0;
    int currentFileNo = 0;
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      storagePartition.put(entry.getKey().getBytes(), entry.getValue().getBytes());
      if (verifyChecksum) {
        runningChecksum.update(entry.getKey().getBytes());
        runningChecksum.update(entry.getValue().getBytes());
      }
      if (++currentRecordNum % syncPerRecords == 0) {
        checkpointingInfo = storagePartition.sync();
        if (sorted && !enablePlainTable) {
          Assert.assertEquals(
              checkpointingInfo.get(RocksDBSstFileWriter.ROCKSDB_LAST_FINISHED_SST_FILE_NO),
              String.valueOf(currentFileNo++));
        } else {
          assertTrue(checkpointingInfo.isEmpty(), "For non-deferred-write database, sync() should return empty map");
        }
      }
      if (interrupted) {
        if (currentRecordNum == interruptedRecord) {
          if (reopenDatabaseDuringInterruption) {
            storagePartition.close();
            rocksDBServerConfig.setBlockBaseFormatVersion(5);
            storagePartition = new RocksDBStoragePartition(
                partitionConfig,
                factory,
                DATA_BASE_DIR,
                null,
                ROCKSDB_THROTTLER,
                rocksDBServerConfig,
                storeConfig);
            Options storeOptions = storagePartition.getOptions();
            Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 100);
          }
          if (sorted && !enablePlainTable) {
            storagePartition.beginBatchWrite(checkpointingInfo, checksumSupplier);
          }

          // Pass last checkpointed info.
          // Need to re-consume from the offset when last checkpoint happens
          // inclusive [replayStart, replayEnd]
          int replayStart = (interruptedRecord / syncPerRecords) * syncPerRecords + 1;
          int replayEnd = interruptedRecord;
          int replayCnt = 0;
          runningChecksum.reset();
          for (Map.Entry<String, String> innerEntry: inputRecords.entrySet()) {
            ++replayCnt;
            if (replayCnt >= replayStart && replayCnt <= replayEnd) {
              storagePartition.put(innerEntry.getKey().getBytes(), innerEntry.getValue().getBytes());
              if (verifyChecksum) {
                runningChecksum.update(innerEntry.getKey().getBytes());
                runningChecksum.update(innerEntry.getValue().getBytes());
              }
            }
            if (replayCnt > replayEnd) {
              break;
            }
          }
        }
      }
    }

    if (sorted && !enablePlainTable) {
      Assert.assertFalse(storagePartition.validateBatchIngestion());
      storagePartition.endBatchWrite();
      assertTrue(storagePartition.validateBatchIngestion());
    }

    // Verify all the key/value pairs
    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }

    if (sorted && !enablePlainTable) {
      if (enableBlobFile) {
        // Verify some Blob file related metrics
        for (String metric: BLOB_METRIC_LIST) {
          Assert.assertEquals(storagePartition.getRocksDBStatValue(metric), 0);
        }
      }
    } else {
      if (enableBlobFile && !enablePlainTable) {
        // Verify some Blob file related metrics
        for (String metric: BLOB_METRIC_LIST) {
          if (!metric.equals(BLOB_GARBAGE_METRIC)) {
            Assert.assertTrue(storagePartition.getRocksDBStatValue(metric) > 0);
          } else {
            // No garbage so far.
            Assert.assertEquals(storagePartition.getRocksDBStatValue(metric), 0);
          }
        }
      }
    }

    // Verify current ingestion mode is in deferred-write mode
    assertTrue(storagePartition.verifyConfig(partitionConfig));

    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    partitionConfig.setWriteOnlyConfig(false);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    // Test deletion
    String toBeDeletedKey = KEY_PREFIX + 10;
    Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes()));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes()));

    long minimalPartitionSize = (long) numberOfRecords * (KEY_PREFIX.length() + VALUE_PREFIX.length() + padding);
    long maxPartitionSize = 2 * minimalPartitionSize; // Add some buffer;
    long partitionSize = storagePartition.getPartitionSizeInBytes();
    assertTrue(partitionSize > minimalPartitionSize && partitionSize < maxPartitionSize);

    Options storeOptions = storagePartition.getOptions();
    Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 40);
    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIngestionFormatVersionChange(boolean sorted) throws RocksDBException {
    CheckSum runningChecksum = CheckSum.getInstance(CheckSumType.MD5);
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(sorted);
    Options options = new Options();
    options.setCreateIfMissing(true);
    Map<String, String> inputRecords = generateInput(1010, sorted, 0);
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    final int syncPerRecords = 100;
    final int interruptedRecord1 = 345;
    final int interruptedRecord2 = 645;

    Optional<Supplier<byte[]>> checksumSupplier = Optional.empty();
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);
    }
    int currentRecordNum = 0;
    int currentFileNo = 0;
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      storagePartition.put(entry.getKey().getBytes(), entry.getValue().getBytes());
      if (false) {
        runningChecksum.update(entry.getKey().getBytes());
        runningChecksum.update(entry.getValue().getBytes());
      }
      if (++currentRecordNum % syncPerRecords == 0) {
        checkpointingInfo = storagePartition.sync();
        if (sorted) {
          Assert.assertEquals(
              checkpointingInfo.get(RocksDBSstFileWriter.ROCKSDB_LAST_FINISHED_SST_FILE_NO),
              String.valueOf(currentFileNo++));
        } else {
          assertTrue(checkpointingInfo.isEmpty(), "For non-deferred-write database, sync() should return empty map");
        }
      }
      if (currentRecordNum == interruptedRecord1 || currentRecordNum == interruptedRecord2) {
        storagePartition.close();
        // Flip format version from 2 -> 5 -> 2 to simulation rollback of version.
        rocksDBServerConfig.setBlockBaseFormatVersion(currentRecordNum == interruptedRecord1 ? 5 : 2);
        storagePartition = new RocksDBStoragePartition(
            partitionConfig,
            factory,
            DATA_BASE_DIR,
            null,
            ROCKSDB_THROTTLER,
            rocksDBServerConfig,
            storeConfig);
        Options storeOptions = storagePartition.getOptions();
        Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 100);
        if (sorted) {
          storagePartition.beginBatchWrite(checkpointingInfo, checksumSupplier);
        }

        // Pass last checkpointed info.
        // Need to re-consume from the offset when last checkpoint happens
        // inclusive [replayStart, replayEnd]
        int interruptedRecord = currentRecordNum == interruptedRecord1 ? interruptedRecord1 : interruptedRecord2;
        int replayStart = (interruptedRecord / syncPerRecords) * syncPerRecords + 1;
        int replayEnd = interruptedRecord;
        int replayCnt = 0;
        runningChecksum.reset();
        for (Map.Entry<String, String> innerEntry: inputRecords.entrySet()) {
          ++replayCnt;
          if (replayCnt >= replayStart && replayCnt <= replayEnd) {
            storagePartition.put(innerEntry.getKey().getBytes(), innerEntry.getValue().getBytes());
          }
          if (replayCnt > replayEnd) {
            break;
          }
        }
      }
    }

    if (sorted) {
      Assert.assertFalse(storagePartition.validateBatchIngestion());
      storagePartition.endBatchWrite();
      assertTrue(storagePartition.validateBatchIngestion());
    }

    // Verify all the key/value pairs
    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }

    // Verify current ingestion mode is in deferred-write mode
    assertTrue(storagePartition.verifyConfig(partitionConfig));

    storagePartition.rocksDB.compactRange();
    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    partitionConfig.setWriteOnlyConfig(false);
    // Set format version to 5
    rocksDBServerConfig.setBlockBaseFormatVersion(5);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    storagePartition.rocksDB.compactRange();

    // Verify all the key/value pairs can be read using the new format
    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
      // Try to read via multi-get API
      List<byte[]> values = storagePartition.multiGet(Arrays.asList(entry.getKey().getBytes()));
      Assert.assertEquals(values.get(0), entry.getValue().getBytes());

      // Try to read via multi-get buffer reuse API
      List<ByteBuffer> keys = new ArrayList<>();
      ByteBuffer key = ByteBuffer.allocateDirect(100);
      key.put(entry.getKey().getBytes());
      key.flip();
      keys.add(key);
      List<ByteBuffer> byteBufferList = new ArrayList<>();
      byteBufferList.add(ByteBuffer.allocateDirect(100));
      // Test with a large enough buffer
      Assert.assertEquals(
          ByteUtils.copyByteArray(storagePartition.multiGet(keys, byteBufferList).get(0)),
          entry.getValue().getBytes());

      // Test with a small buffer
      byteBufferList.set(0, ByteBuffer.allocateDirect(1));
      Assert.assertEquals(
          ByteUtils.copyByteArray(storagePartition.multiGet(keys, byteBufferList).get(0)),
          entry.getValue().getBytes());
      // test it again with the internally enlarged buffer
      assertTrue(byteBufferList.get(0).capacity() > 1);
      Assert.assertEquals(
          ByteUtils.copyByteArray(storagePartition.multiGet(keys, byteBufferList).get(0)),
          entry.getValue().getBytes());

      // Test with a non-existing key
      keys.set(0, ByteBuffer.allocateDirect(1));
      Assert.assertNull(storagePartition.multiGet(keys, byteBufferList).get(0));
    }

    // Test deletion
    String toBeDeletedKey = KEY_PREFIX + 10;
    Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes()));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes()));

    Options storeOptions = storagePartition.getOptions();
    Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 40);
    storagePartition.drop();
    options.close();
    removeDir(storeDir);
  }

  @Test(dataProvider = "Four-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIngestionWithClockCache(
      boolean sorted,
      boolean interrupted,
      boolean reopenDatabaseDuringInterruption,
      boolean verifyChecksum) {
    CheckSum runningChecksum = CheckSum.getInstance(CheckSumType.MD5);
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(sorted);
    Options options = new Options();
    options.setCreateIfMissing(true);
    Map<String, String> inputRecords = generateInput(1010, sorted, 0);
    Properties properties = new Properties();
    properties.put(ROCKSDB_BLOCK_CACHE_IMPLEMENTATION, RocksDBBlockCacheImplementations.CLOCK.toString());
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    final int syncPerRecords = 100;
    final int interruptedRecord = 345;

    Optional<Supplier<byte[]>> checksumSupplier = Optional.empty();
    if (verifyChecksum) {
      checksumSupplier = Optional.of(() -> {
        byte[] checksum = runningChecksum.getCheckSum();
        runningChecksum.reset();
        return checksum;
      });
    }
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);
    }
    int currentRecordNum = 0;
    int currentFileNo = 0;
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      storagePartition.put(entry.getKey().getBytes(), entry.getValue().getBytes());
      if (verifyChecksum) {
        runningChecksum.update(entry.getKey().getBytes());
        runningChecksum.update(entry.getValue().getBytes());
      }
      if (++currentRecordNum % syncPerRecords == 0) {
        checkpointingInfo = storagePartition.sync();
        if (sorted) {
          Assert.assertEquals(
              checkpointingInfo.get(RocksDBSstFileWriter.ROCKSDB_LAST_FINISHED_SST_FILE_NO),
              String.valueOf(currentFileNo++));
        } else {
          assertTrue(checkpointingInfo.isEmpty(), "For non-deferred-write database, sync() should return empty map");
        }
      }
      if (interrupted) {
        if (currentRecordNum == interruptedRecord) {
          if (reopenDatabaseDuringInterruption) {
            storagePartition.close();
            storagePartition = new RocksDBStoragePartition(
                partitionConfig,
                factory,
                DATA_BASE_DIR,
                null,
                ROCKSDB_THROTTLER,
                rocksDBServerConfig,
                storeConfig);
            Options storeOptions = storagePartition.getOptions();
            Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 100);
          }
          if (sorted) {
            storagePartition.beginBatchWrite(checkpointingInfo, checksumSupplier);
          }

          // Pass last checkpointed info.
          // Need to re-consume from the offset when last checkpoint happens
          // inclusive [replayStart, replayEnd]
          int replayStart = (interruptedRecord / syncPerRecords) * syncPerRecords + 1;
          int replayEnd = interruptedRecord;
          int replayCnt = 0;
          runningChecksum.reset();
          for (Map.Entry<String, String> innerEntry: inputRecords.entrySet()) {
            ++replayCnt;
            if (replayCnt >= replayStart && replayCnt <= replayEnd) {
              storagePartition.put(innerEntry.getKey().getBytes(), innerEntry.getValue().getBytes());
              if (verifyChecksum) {
                runningChecksum.update(innerEntry.getKey().getBytes());
                runningChecksum.update(innerEntry.getValue().getBytes());
              }
            }
            if (replayCnt > replayEnd) {
              break;
            }
          }
        }
      }
    }

    if (sorted) {
      Assert.assertFalse(storagePartition.validateBatchIngestion());
      storagePartition.endBatchWrite();
      assertTrue(storagePartition.validateBatchIngestion());
    }

    // Verify all the key/value pairs
    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }

    // Verify current ingestion mode is in deferred-write mode
    assertTrue(storagePartition.verifyConfig(partitionConfig));

    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    partitionConfig.setWriteOnlyConfig(false);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    // Test deletion
    String toBeDeletedKey = KEY_PREFIX + 10;
    Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes()));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes()));

    Options storeOptions = storagePartition.getOptions();
    Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 40);
    storagePartition.drop();
    options.close();
    removeDir(storeDir);
  }

  @Test
  public void testChecksumVerificationFailure() {
    String storeName = Version.composeKafkaTopic("test_store_c1", 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(true);
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    Optional<Supplier<byte[]>> checksumSupplier = Optional.of(() -> new byte[16]);
    storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);

    Map<String, String> inputRecords = generateInput(1024, true, 230);
    for (Map.Entry<String, String> entry: inputRecords.entrySet()) {
      storagePartition.put(entry.getKey().getBytes(), entry.getValue().getBytes());
    }
    VeniceException ex = Assert.expectThrows(VeniceException.class, storagePartition::endBatchWrite);
    assertTrue(ex.getMessage().contains("last sstFile checksum didn't match for store"));

    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test
  public void testRocksDBValidityCheck() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(false);
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    storagePartition.close();
    try {
      storagePartition.get((KEY_PREFIX + "10").getBytes());
      Assert.fail("VeniceException is expected when looking up an already closed DB");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("RocksDB has been closed for replica"));
    }

    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test
  public void testVerifyConfig() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    Properties properties = new Properties();
    properties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString());
    properties.put(ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED, "true");

    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    RocksDBServerConfig rocksDBServerConfig =
        new RocksDBServerConfig(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties));
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setWriteOnlyConfig(true);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    StoragePartitionConfig testConfig = new StoragePartitionConfig(storeName, partitionId);
    testConfig.setReadWriteLeaderForRMDCF(true);
    assertFalse(storagePartition.verifyConfig(testConfig));

    testConfig = new StoragePartitionConfig(storeName, partitionId);
    testConfig.setReadWriteLeaderForDefaultCF(true);
    assertFalse(storagePartition.verifyConfig(testConfig));

    testConfig = new StoragePartitionConfig(storeName, partitionId);
    assertTrue(storagePartition.verifyConfig(testConfig));
    storagePartition.drop();

    // Create a new storage partition with read-write leader tuning disabled.
    properties = new Properties();
    properties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString());
    properties.put(ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED, "false");
    veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    rocksDBServerConfig =
        new RocksDBServerConfig(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties));
    serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);
    storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    testConfig = new StoragePartitionConfig(storeName, partitionId);
    testConfig.setReadWriteLeaderForRMDCF(true);
    assertTrue(storagePartition.verifyConfig(testConfig));

    testConfig = new StoragePartitionConfig(storeName, partitionId);
    testConfig.setReadWriteLeaderForDefaultCF(true);
    assertTrue(storagePartition.verifyConfig(testConfig));

    testConfig = new StoragePartitionConfig(storeName, partitionId);
    assertTrue(storagePartition.verifyConfig(testConfig));

    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test
  public void testCompactionTriggerSetting() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    Properties properties = new Properties();
    properties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString());
    properties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    properties.put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 10);
    properties.put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20);
    properties.put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 30);
    properties.put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40);
    properties.put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 50);
    properties.put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60);
    properties.put(ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED, "true");
    properties.put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_FOR_READ_WRITE_LEADER, 1);
    properties.put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_FOR_READ_WRITE_LEADER, 2);
    properties.put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_FOR_READ_WRITE_LEADER, 4);
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setWriteOnlyConfig(true);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    // By default, it is write only
    Options writeOnlyOptions = storagePartition.getOptions();
    Assert.assertEquals(writeOnlyOptions.level0FileNumCompactionTrigger(), 40);
    Assert.assertEquals(writeOnlyOptions.level0SlowdownWritesTrigger(), 50);
    Assert.assertEquals(writeOnlyOptions.level0StopWritesTrigger(), 60);
    StoragePartitionConfig newStoragePartitionConfig = new StoragePartitionConfig(storeName, partitionId);
    newStoragePartitionConfig.setWriteOnlyConfig(false);
    // VerifyConfig should return false because of different write config
    Assert.assertFalse(storagePartition.verifyConfig(newStoragePartitionConfig));

    storagePartition.close();
    // Reopen with write-only disabled
    partitionConfig.setWriteOnlyConfig(false);
    storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    Options readWriteOptions = storagePartition.getOptions();
    Assert.assertEquals(readWriteOptions.level0FileNumCompactionTrigger(), 10);
    Assert.assertEquals(readWriteOptions.level0SlowdownWritesTrigger(), 20);
    Assert.assertEquals(readWriteOptions.level0StopWritesTrigger(), 30);

    // Check compaction setting with read-write leader
    StoragePartitionConfig storagePartitionConfig = new StoragePartitionConfig(storeName, partitionId);
    storagePartitionConfig.setReadWriteLeaderForDefaultCF(true);
    storagePartitionConfig.setReadWriteLeaderForRMDCF(false);
    // Verify default CF
    Options readWriterLeaderForDefaultCF = storagePartition.getStoreOptions(storagePartitionConfig, false);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0FileNumCompactionTrigger(), 1);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0SlowdownWritesTrigger(), 2);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0StopWritesTrigger(), 4);
    // Verify RMD CF
    Options readWriterLeaderForRMDCF = storagePartition.getStoreOptions(storagePartitionConfig, true);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0FileNumCompactionTrigger(), 40);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0SlowdownWritesTrigger(), 50);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0StopWritesTrigger(), 60);

    storagePartitionConfig.setReadWriteLeaderForDefaultCF(false);
    storagePartitionConfig.setReadWriteLeaderForRMDCF(true);
    // Verify default CF
    readWriterLeaderForDefaultCF = storagePartition.getStoreOptions(storagePartitionConfig, false);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0FileNumCompactionTrigger(), 40);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0SlowdownWritesTrigger(), 50);
    Assert.assertEquals(readWriterLeaderForDefaultCF.level0StopWritesTrigger(), 60);
    // Verify RMD CF
    readWriterLeaderForRMDCF = storagePartition.getStoreOptions(storagePartitionConfig, true);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0FileNumCompactionTrigger(), 1);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0SlowdownWritesTrigger(), 2);
    Assert.assertEquals(readWriterLeaderForRMDCF.level0StopWritesTrigger(), 4);

    storagePartition.drop();
    removeDir(storeDir);
  }

  @Test
  public void checkMemoryLimitAtDatabaseOpen() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    RocksDBStoragePartition storagePartition = null;
    try {
      Properties extraProps = new Properties();
      extraProps.setProperty(INGESTION_USE_DA_VINCI_CLIENT, "true");
      extraProps.setProperty(INGESTION_MEMORY_LIMIT, "1MB");
      extraProps.setProperty(ROCKSDB_MAX_MEMTABLE_COUNT, "2");
      extraProps.setProperty(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, "128KB");
      extraProps.setProperty(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "512KB");
      extraProps.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");

      VeniceProperties veniceServerProperties =
          AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
      RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

      int partitionId = 0;
      StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);

      RocksDBMemoryStats mockMemoryStats = mock(RocksDBMemoryStats.class);
      VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
      VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
      RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(
          serverConfig,
          mockMemoryStats,
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
          AvroProtocolDefinition.PARTITION_STATE.getSerializer());
      Mockito.verify(mockMemoryStats).setMemoryLimit(anyLong());
      Mockito.verify(mockMemoryStats).setSstFileManager(factory.getSstFileManagerForMemoryLimiter());
      storagePartition = new RocksDBStoragePartition(
          partitionConfig,
          factory,
          DATA_BASE_DIR,
          null,
          ROCKSDB_THROTTLER,
          rocksDBServerConfig,
          storeConfig);
      RocksDBStoragePartition finalStoragePartition = storagePartition;
      Assert.expectThrows(MemoryLimitExhaustedException.class, () -> {
        String keyPrefix = "key_prefix_";
        String valuePrefix = "value_prefix________________________________________";
        for (int i = 0; i < 100000; ++i) {
          finalStoragePartition.put((keyPrefix + i).getBytes(), (valuePrefix + i).getBytes());
        }
        ;
      });

      Assert.expectThrows(MemoryLimitExhaustedException.class, () -> {
        String keyPrefix = "key_prefix1_";
        for (int i = 0; i < 100000; ++i) {
          finalStoragePartition.delete((keyPrefix + i).getBytes());
        }
        ;
      });

      Assert.expectThrows(MemoryLimitExhaustedException.class, () -> finalStoragePartition.sync());
      storagePartition.close();

      extraProps.setProperty(INGESTION_MEMORY_LIMIT, "800KB");
      // With a tighter memory limiter, the database open should fail
      veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
      RocksDBServerConfig finalRocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

      serverConfig = new VeniceServerConfig(veniceServerProperties);
      RocksDBStorageEngineFactory finalFactory = new RocksDBStorageEngineFactory(
          serverConfig,
          mockMemoryStats,
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
          AvroProtocolDefinition.PARTITION_STATE.getSerializer());
      Assert.expectThrows(
          MemoryLimitExhaustedException.class,
          () -> new RocksDBStoragePartition(
              partitionConfig,
              finalFactory,
              DATA_BASE_DIR,
              null,
              ROCKSDB_THROTTLER,
              finalRocksDBServerConfig,
              storeConfig));
    } finally {
      if (storagePartition != null) {
        storagePartition.close();
        storagePartition.drop();
      }
      removeDir(storeDir);
    }
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testCreateSnapshot(boolean blobTransferEnabled) {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(false);
    Properties extraProps = new Properties();
    extraProps.setProperty(BLOB_TRANSFER_MANAGER_ENABLED, "true");
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, extraProps);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);

    // Set the blob transfer enabled flag
    storeConfig.setBlobTransferEnabled(blobTransferEnabled);

    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    try (MockedStatic<RocksDBStoragePartition> rocksDBStoragePartition =
        Mockito.mockStatic(RocksDBStoragePartition.class)) {
      rocksDBStoragePartition.when(() -> RocksDBStoragePartition.createSnapshot(Mockito.any(), Mockito.any()))
          .thenAnswer(invocation -> {
            return null;
          });
      storagePartition.createSnapshot();
      if (blobTransferEnabled) {
        rocksDBStoragePartition
            .verify(() -> RocksDBStoragePartition.createSnapshot(Mockito.any(), Mockito.any()), Mockito.times(1));
      } else {
        rocksDBStoragePartition
            .verify(() -> RocksDBStoragePartition.createSnapshot(Mockito.any(), Mockito.any()), Mockito.never());
      }
    }

    if (storagePartition != null) {
      storagePartition.close();
      storagePartition.drop();
    }
    removeDir(storeDir);
  }

  @Test
  public void testCreateSnapshotForBatch() throws RocksDBException {
    String basePath = Utils.getUniqueTempPath("sstTest");
    String storeName = "test-store";
    int version = 1;
    int partition = 0;
    String dir = basePath + "/" + storeName + "_v" + version + "/"
        + RocksDBUtils.getPartitionDbName(storeName + "_v" + version, partition);

    try (MockedStatic<Checkpoint> checkpointMockedStatic = Mockito.mockStatic(Checkpoint.class)) {
      try (MockedStatic<FileUtils> fileUtilsMockedStatic = Mockito.mockStatic(FileUtils.class)) {
        // test prepare
        RocksDB mockRocksDB = mock(RocksDB.class);
        Checkpoint mockCheckpoint = mock(Checkpoint.class);
        checkpointMockedStatic.when(() -> Checkpoint.create(mockRocksDB)).thenReturn(mockCheckpoint);
        String fullSnapshotPath = dir + "/.snapshot_files";
        File file = Mockito.spy(new File(fullSnapshotPath));
        Mockito.doNothing().when(mockCheckpoint).createCheckpoint(fullSnapshotPath);

        // case 1: snapshot file not exists
        // test execute
        RocksDBStoragePartition.createSnapshot(mockRocksDB, fullSnapshotPath);
        // test verify
        Mockito.verify(mockCheckpoint, Mockito.times(1)).createCheckpoint(fullSnapshotPath);
        fileUtilsMockedStatic
            .verify(() -> FileUtils.deleteDirectory(Mockito.eq(file.getAbsoluteFile())), Mockito.times(0));

        // case 2: snapshot file exists
        // test prepare
        File fullSnapshotDir = new File(fullSnapshotPath);
        if (!fullSnapshotDir.exists()) {
          fullSnapshotDir.mkdirs();
        }
        // test execute
        RocksDBStoragePartition.createSnapshot(mockRocksDB, fullSnapshotPath);
        // test verify
        Mockito.verify(mockCheckpoint, Mockito.times(2)).createCheckpoint(fullSnapshotPath);
        fileUtilsMockedStatic
            .verify(() -> FileUtils.deleteDirectory(Mockito.eq(file.getAbsoluteFile())), Mockito.times(1));

        // case 3: delete snapshot file fail
        // test prepare
        fileUtilsMockedStatic.when(() -> FileUtils.deleteDirectory(Mockito.any(File.class)))
            .thenThrow(new IOException("Delete snapshot file failed."));
        // test execute
        try {
          RocksDBStoragePartition.createSnapshot(mockRocksDB, fullSnapshotPath);
          Assert.fail("Should throw exception");
        } catch (VeniceException e) {
          // test verify
          Mockito.verify(mockCheckpoint, Mockito.times(2)).createCheckpoint(fullSnapshotPath);
          fileUtilsMockedStatic
              .verify(() -> FileUtils.deleteDirectory(Mockito.eq(file.getAbsoluteFile())), Mockito.times(2));
          Assert.assertEquals(e.getMessage(), "Failed to delete the existing snapshot directory: " + fullSnapshotPath);
        }

        // case 4: create createCheckpoint failed
        // test prepare
        fullSnapshotDir.delete();
        fileUtilsMockedStatic.reset();
        Mockito.doThrow(new RocksDBException("Create checkpoint failed."))
            .when(mockCheckpoint)
            .createCheckpoint(fullSnapshotPath);
        // test execute
        try {
          RocksDBStoragePartition.createSnapshot(mockRocksDB, fullSnapshotPath);
          Assert.fail("Should throw exception");
        } catch (VeniceException e) {
          // test verify
          Mockito.verify(mockCheckpoint, Mockito.times(3)).createCheckpoint(fullSnapshotPath);
          fileUtilsMockedStatic
              .verify(() -> FileUtils.deleteDirectory(Mockito.eq(file.getAbsoluteFile())), Mockito.times(0));
          Assert.assertEquals(
              e.getMessage(),
              "Received exception during RocksDB's snapshot creation in directory " + fullSnapshotPath);
        }
      }
    }
  }
}
