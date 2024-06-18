package com.linkedin.davinci.blob;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobSnapshotManagerTest {
  private static final long SNAPSHOT_RETENTION_TIME = 60000;
  private static final String STORE_NAME = Utils.getUniqueString("sstTest");
  private static final int PARTITION_ID = 0;
  private static final String BASE_PATH = Utils.getUniqueTempPath("sstTest");

  @Test
  public void testHybridSnapshot() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    BlobSnapshotManager blobSnapshotManager = spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));

    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    verify(mockCheckpoint, times(1)).createCheckpoint(
        BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID)
            + "/.snapshot_files");
  }

  @Test
  public void testSnapshotUpdatesWhenStale() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    BlobSnapshotManager blobSnapshotManager = spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));
    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    HashMap<String, HashMap<Integer, Long>> snapShotTimestamps = new HashMap<>();
    snapShotTimestamps.put(STORE_NAME, new HashMap<>());
    snapShotTimestamps.get(STORE_NAME).put(PARTITION_ID, System.currentTimeMillis() - SNAPSHOT_RETENTION_TIME - 1);
    blobSnapshotManager.snapShotTimestamps = snapShotTimestamps;

    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    verify(mockCheckpoint, times(1)).createCheckpoint(
        BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID)
            + "/.snapshot_files");
  }

  @Test
  public void testSameSnapshotWhenConcurrentUsers() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    BlobSnapshotManager blobSnapshotManager = spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));
    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    blobSnapshotManager.getHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    Assert.assertEquals(blobSnapshotManager.getConcurrentUsers(mockRocksDB, STORE_NAME, PARTITION_ID), 3);
  }
}
