package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
  private static final ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final String DB_DIR =
      BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID);

  @Test
  public void testHybridSnapshot() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    Store mockStore = mock(Store.class);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.isHybrid()).thenReturn(true);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME, readOnlyStoreRepository));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));

    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    verify(mockCheckpoint, times(1)).createCheckpoint(DB_DIR + "/.snapshot_files");
  }

  @Test
  public void testSnapshotUpdatesWhenStale() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    Store mockStore = mock(Store.class);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.isHybrid()).thenReturn(true);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME, readOnlyStoreRepository));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint).createCheckpoint(DB_DIR);
    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    Map<String, Map<Integer, Long>> snapShotTimestamps = new VeniceConcurrentHashMap<>();
    snapShotTimestamps.put(STORE_NAME, new VeniceConcurrentHashMap<>());
    snapShotTimestamps.get(STORE_NAME).put(PARTITION_ID, System.currentTimeMillis() - SNAPSHOT_RETENTION_TIME - 1);
    blobSnapshotManager.setSnapShotTimestamps(snapShotTimestamps);

    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    verify(mockCheckpoint, times(1)).createCheckpoint(DB_DIR + "/.snapshot_files");
  }

  @Test
  public void testSameSnapshotWhenConcurrentUsers() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    Store mockStore = mock(Store.class);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.isHybrid()).thenReturn(true);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME, readOnlyStoreRepository));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));
    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);

    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(mockRocksDB, STORE_NAME, PARTITION_ID), 3);
  }

  @Test
  public void testMultipleThreads() throws RocksDBException {
    final int numberOfThreads = 2;
    final ExecutorService asyncExecutor = Executors.newFixedThreadPool(numberOfThreads);
    final CountDownLatch latch = new CountDownLatch(numberOfThreads);
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    Store mockStore = mock(Store.class);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.isHybrid()).thenReturn(true);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME, readOnlyStoreRepository));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));

    for (int i = 0; i < numberOfThreads; i++) {
      asyncExecutor.submit(() -> {
        blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
        blobSnapshotManager.decreaseConcurrentUserCount(STORE_NAME, PARTITION_ID);
        latch.countDown();
      });
    }

    try {
      // Wait for all threads to finish
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Test interrupted", e);
    }

    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(mockRocksDB, STORE_NAME, PARTITION_ID), 0);
  }

  @Test
  public void testSnapshotNotUpdatedWhenNotHybrid() throws RocksDBException {
    RocksDB mockRocksDB = mock(RocksDB.class);
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    Store mockStore = mock(Store.class);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.isHybrid()).thenReturn(false);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(BASE_PATH, SNAPSHOT_RETENTION_TIME, readOnlyStoreRepository));
    doReturn(mockCheckpoint).when(blobSnapshotManager).createCheckpoint(mockRocksDB);
    doNothing().when(mockCheckpoint)
        .createCheckpoint(
            BASE_PATH + "/" + STORE_NAME + "/" + RocksDBUtils.getPartitionDbName(STORE_NAME, PARTITION_ID));
    blobSnapshotManager.maybeUpdateHybridSnapshot(mockRocksDB, STORE_NAME, PARTITION_ID);
    verify(mockCheckpoint, times(0)).createCheckpoint(DB_DIR + "/.snapshot_files");
  }

  @Test
  public void testCreateSnapshotForBatch() throws RocksDBException {
    try (MockedStatic<Checkpoint> checkpointMockedStatic = Mockito.mockStatic(Checkpoint.class)) {
      try (MockedStatic<FileUtils> fileUtilsMockedStatic = Mockito.mockStatic(FileUtils.class)) {
        // test prepare
        RocksDB mockRocksDB = mock(RocksDB.class);
        Checkpoint mockCheckpoint = mock(Checkpoint.class);
        checkpointMockedStatic.when(() -> Checkpoint.create(mockRocksDB)).thenReturn(mockCheckpoint);
        String fullSnapshotPath = DB_DIR + "/.snapshot_files";
        File file = spy(new File(fullSnapshotPath));
        doNothing().when(mockCheckpoint).createCheckpoint(fullSnapshotPath);

        // case 1: snapshot file not exists
        // test execute
        BlobSnapshotManager.createSnapshotForBatch(mockRocksDB, fullSnapshotPath);
        // test verify
        verify(mockCheckpoint, times(1)).createCheckpoint(fullSnapshotPath);
        fileUtilsMockedStatic.verify(() -> FileUtils.deleteDirectory(eq(file.getAbsoluteFile())), times(0));

        // case 2: snapshot file exists
        // test prepare
        File fullSnapshotDir = new File(fullSnapshotPath);
        if (!fullSnapshotDir.exists()) {
          fullSnapshotDir.mkdirs();
        }
        // test execute
        BlobSnapshotManager.createSnapshotForBatch(mockRocksDB, fullSnapshotPath);
        // test verify
        verify(mockCheckpoint, times(2)).createCheckpoint(fullSnapshotPath);
        fileUtilsMockedStatic.verify(() -> FileUtils.deleteDirectory(eq(file.getAbsoluteFile())), times(1));

        // case 3: delete snapshot file fail
        // test prepare
        fileUtilsMockedStatic.when(() -> FileUtils.deleteDirectory(any(File.class)))
            .thenThrow(new IOException("Delete snapshot file failed."));
        // test execute
        try {
          BlobSnapshotManager.createSnapshotForBatch(mockRocksDB, fullSnapshotPath);
          Assert.fail("Should throw exception");
        } catch (VeniceException e) {
          // test verify
          verify(mockCheckpoint, times(2)).createCheckpoint(fullSnapshotPath);
          fileUtilsMockedStatic.verify(() -> FileUtils.deleteDirectory(eq(file.getAbsoluteFile())), times(2));
          Assert.assertEquals(e.getMessage(), "Failed to delete the existing snapshot directory: " + fullSnapshotPath);
        }

        // case 4: create createCheckpoint failed
        // test prepare
        fullSnapshotDir.delete();
        fileUtilsMockedStatic.reset();
        doThrow(new RocksDBException("Create checkpoint failed.")).when(mockCheckpoint)
            .createCheckpoint(fullSnapshotPath);
        // test execute
        try {
          BlobSnapshotManager.createSnapshotForBatch(mockRocksDB, fullSnapshotPath);
          Assert.fail("Should throw exception");
        } catch (VeniceException e) {
          // test verify
          verify(mockCheckpoint, times(3)).createCheckpoint(fullSnapshotPath);
          fileUtilsMockedStatic.verify(() -> FileUtils.deleteDirectory(eq(file.getAbsoluteFile())), times(0));
          Assert.assertEquals(
              e.getMessage(),
              "Received exception during RocksDB's snapshot creation in directory " + fullSnapshotPath);
        }
      }
    }
  }
}
