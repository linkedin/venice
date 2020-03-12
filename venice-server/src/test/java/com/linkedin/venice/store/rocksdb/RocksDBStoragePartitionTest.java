package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.Slice;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RocksDBStoragePartitionTest {
  private static final String DATA_BASE_DIR = TestUtils.getUniqueTempPath();
  private static final String keyPrefix = "key_";
  private static final String valuePrefix = "value_";
  private static final RocksDBThrottler rocksDbThrottler = new RocksDBThrottler(3);

  private Map<String, String> generateInput(int recordCnt, boolean sorted) {
    Map<String, String> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        Slice s1 = new Slice(o1.getBytes());
        Slice s2 = new Slice(o2.getBytes());
        return comparator.compare(s1, s2);
      });
    } else {
      records = new HashMap<>();
    }
    for (int i = 0; i < recordCnt; ++i) {
      records.put(keyPrefix + i, valuePrefix + i);
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

  @DataProvider (name="testIngestionDataProvider")
  private Object[][] testIngestionDataProvider() {
    return new Object[][] {
        {true, false, false}, // Sorted input without interruption
        {true, true, true},   // Sorted input with interruption
        {true, true, false},  // Sorted input with storage node re-boot
        {false, false, false},// Unsorted input without interruption
        {false, true, false}, // Unsorted input with interruption
        {false, true, true}   // Unsorted input with storage node re-boot
    };
  }

  @Test (dataProvider = "testIngestionDataProvider")
  public void testIngestion(boolean sorted, boolean interrupted, boolean reopenDatabaseDuringInterruption) {
    String storeName = TestUtils.getUniqueString("test_store");
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(sorted);
    Options options = new Options();
    options.setCreateIfMissing(true);
    Map<String, String> inputRecords = generateInput(1000, sorted);
    RocksDBStoragePartition storagePartition = new RocksDBStoragePartition(partitionConfig, options, DATA_BASE_DIR, null, rocksDbThrottler);
    final int syncPerRecords = 100;
    final int interruptedRecord = 345;
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>());
    }
    int currentRecordNum = 0;
    int currentFileNo = 0;
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, String> entry : inputRecords.entrySet()) {
      storagePartition.put(entry.getKey().getBytes(), entry.getValue().getBytes());
      if (++currentRecordNum % syncPerRecords == 0) {
        checkpointingInfo = storagePartition.sync();
        if (sorted) {
          Assert.assertEquals(checkpointingInfo.get(RocksDBStoragePartition.ROCKSDB_LAST_FINISHED_SST_FILE_NO),
              new Integer(currentFileNo++).toString());
        } else {
          Assert.assertTrue(checkpointingInfo.isEmpty(), "For non-deferred-write database, sync() should return empty map");
        }
      }
      if (interrupted) {
        if (currentRecordNum == interruptedRecord) {
          if (reopenDatabaseDuringInterruption) {
            storagePartition.close();
            storagePartition = new RocksDBStoragePartition(partitionConfig, options, DATA_BASE_DIR, null, rocksDbThrottler);
          }
          if (sorted) {
            storagePartition.beginBatchWrite(checkpointingInfo);
          }

          // Pass last checkpointed info.
          // Need to re-consume from the offset when last checkpoint happens
          // inclusive [replayStart, replayEnd]
          int replayStart = (interruptedRecord / syncPerRecords) * syncPerRecords + 1;
          int replayEnd = interruptedRecord;
          int replayCnt = 0;
          for (Map.Entry<String, String> innerEntry : inputRecords.entrySet()) {
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
    }
    if (sorted) {
      storagePartition.endBatchWrite();
    }

    // Verify all the key/value pairs
    for (Map.Entry<String, String> entry : inputRecords.entrySet()) {
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), entry.getValue().getBytes());
    }

    // Verify current ingestion mode is in deferred-write mode
    Assert.assertTrue(storagePartition.verifyConfig(partitionConfig));

    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    storagePartition = new RocksDBStoragePartition(partitionConfig, options, DATA_BASE_DIR, null, rocksDbThrottler);
    // Test deletion
    String toBeDeletedKey = keyPrefix + 10;
    Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes()));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes()));

    storagePartition.drop();
    options.close();
    removeDir(storeDir);
  }
}
