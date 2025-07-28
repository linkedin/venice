package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBThrottler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobSnapshotManagerTest {
  private static final int TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final String STORE_NAME = "test-store";
  private static final int VERSION_ID = 1;
  private static final String TOPIC_NAME = STORE_NAME + "_v" + VERSION_ID;
  private static final int PARTITION_ID = 0;
  private static final String BASE_PATH = Utils.getUniqueTempPath("sstTest");
  private static final ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
  private static final StorageMetadataService storageMetadataService = mock(StorageMetadataService.class);
  private static final BlobTransferPartitionMetadata blobTransferPartitionMetadata =
      new BlobTransferPartitionMetadata();

  private static final BlobTransferPayload blobTransferPayload = new BlobTransferPayload(
      BASE_PATH,
      STORE_NAME,
      VERSION_ID,
      PARTITION_ID,
      BlobTransferTableFormat.BLOCK_BASED_TABLE);

  @Test(timeOut = TIMEOUT)
  public void testHybridSnapshot() {
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));

    // Due to the store is hybrid, it will re-create a new snapshot.
    verify(storagePartition, times(1)).createSnapshot();
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);
  }

  @Test(timeOut = TIMEOUT)
  public void testFastFailoverForSEAndPartitionCheck() {
    // prepare:
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));

    // case 1: SE does not exist
    Mockito.doReturn(null).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    // action
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      Assert.fail("Should throw exception when SE does not exist");
    } catch (VeniceException e) {
      String errorMessage =
          String.format("No storage engine found for replica %s", Utils.getReplicaId(TOPIC_NAME, PARTITION_ID));
      Assert.assertEquals(e.getMessage(), errorMessage);
    }

    // case 2: have SE, but SE does not contain partition
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(false).when(storageEngine).containsPartition(PARTITION_ID);
    // action
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      Assert.fail("Should throw exception when partition does not exist");
    } catch (VeniceException e) {
      String errorMessage =
          String.format("No storage engine found for replica %s", Utils.getReplicaId(TOPIC_NAME, PARTITION_ID));
      Assert.assertEquals(e.getMessage(), errorMessage);
    }

    // case 3: SE contains this partition, but this partition is in blob transfer.
    // prepare the rocksDBStoragePartition
    // Set the blobTransferInProgress flag to true in StoragePartitionConfig
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(TOPIC_NAME, PARTITION_ID, true);
    Properties properties = new Properties();
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    RocksDBServerConfig rocksDBServerConfig =
        new RocksDBServerConfig(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties));
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    RocksDBStoragePartition storagePartition =
        new RocksDBStoragePartition(partitionConfig, factory, "", null, new RocksDBThrottler(3), rocksDBServerConfig);

    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    // action
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      Assert.fail("Should throw exception when partition is in blob transfer");
    } catch (VeniceException e) {
      String errorMessage = String.format(
          "RocksDB instance is null, rocksDBPartitionBlobTransferInProgress flag is true for replica %s",
          Utils.getReplicaId(TOPIC_NAME, PARTITION_ID));
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  @Test(timeOut = TIMEOUT)
  public void testTwoRequestUsingSameOffset() {
    // Prepare
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // first request for same payload but use offset 1
    BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);

    // second request for same payload but use offset 2
    BlobTransferPartitionMetadata blobTransferPartitionMetadata2 = Mockito.mock(BlobTransferPartitionMetadata.class);
    doReturn(blobTransferPartitionMetadata2).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);
    actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);

    // verify that the second offset record is not tracked, and the first offset record is still tracked
    Assert.assertEquals(
        blobSnapshotManager.getTransferredSnapshotMetadata(TOPIC_NAME, PARTITION_ID),
        blobTransferPartitionMetadata);
  }

  @Test(timeOut = TIMEOUT)
  public void testMultipleThreads() throws InterruptedException {
    final int numberOfThreads = 2;
    final ExecutorService asyncExecutor = Executors.newFixedThreadPool(numberOfThreads);
    final CountDownLatch latch = new CountDownLatch(numberOfThreads);

    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    try {
      for (int i = 0; i < numberOfThreads; i++) {
        asyncExecutor.submit(() -> {
          BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
              blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
          blobSnapshotManager.decreaseConcurrentUserCount(blobTransferPayload);
          Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);
          latch.countDown();
        });
      }
    } catch (VeniceException e) {
      String errorMessage = String.format(
          "Snapshot is being used by some hosts, cannot update for topic %s partition %d",
          TOPIC_NAME,
          PARTITION_ID);
      Assert.assertEquals(e.getMessage(), errorMessage);
    }

    assertTrue(latch.await(TIMEOUT / 2, TimeUnit.MILLISECONDS));

    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(TOPIC_NAME, PARTITION_ID), 0);
  }

  @Test
  public void testNotAllowRecreateSnapshotWhenHavingConcurrentUsers() {
    // Prepare
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // Mock there is one existing snapshot user
    blobSnapshotManager.increaseConcurrentUserCount(TOPIC_NAME, PARTITION_ID);

    // New request but the snapshot info is not recorded, and it will try to generate a new snapshot
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      Assert.fail("Should throw exception");
    } catch (VeniceException e) {
      String errorMessage = String.format(
          "Snapshot for topic %s partition %d is still in use by others, can not recreate snapshot for new transfer request.",
          TOPIC_NAME,
          PARTITION_ID);
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  /**
   * test not cleanup snapshot while the snapshot is still in use
   */
  @Test
  public void testNotCleanupSnapshotWhileServingBlobTransferRequest() {
    // Prepare
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    // set the snapshot retention time to 0
    BlobSnapshotManager blobSnapshotManager = spy(
        new BlobSnapshotManager(
            storageEngineRepository,
            storageMetadataService,
            0,
            BlobTransferTableFormat.BLOCK_BASED_TABLE,
            2));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();
    Mockito.doNothing().when(blobSnapshotManager).cleanupSnapshot(TOPIC_NAME, PARTITION_ID);

    // Thread 1: Get transfer metadata and try to generate snapshot.
    Thread transferThread = new Thread(() -> {
      try {
        blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      } catch (Exception e) {
        Assert.fail("Exception in transfer thread: " + e.getMessage());
      }
    });

    // Thread 2: Try to clean up, but should not succeed due to ongoing request.
    Thread cleanupThread = new Thread(() -> {
      try {
        Thread.sleep(100); // Small delay to ensure first thread has started
        blobSnapshotManager.cleanupOutOfRetentionSnapshot(TOPIC_NAME, PARTITION_ID);
      } catch (Exception e) {
        Assert.fail("Exception in cleanup thread: " + e.getMessage());
      }
    });

    // Start
    transferThread.start();
    cleanupThread.start();
    try {
      transferThread.join(3000);
      cleanupThread.join(3000);
    } catch (InterruptedException e) {
      Assert.fail("Test testNotCleanupSnapshotWhileServingBlobTransferRequest interrupted");
    }

    // Verify cleanup was not executed because snapshot was in use
    verify(blobSnapshotManager, times(0)).cleanupSnapshot(TOPIC_NAME, PARTITION_ID);
  }

  /**
   * test while deleting snapshot, a new request arrived, it should recreate the snapshot after the cleanup.
   */
  @Test
  public void testServeBlobTransferRequestWhileDeletingSnapshot() {
    // Prepare
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);
    Mockito.doNothing().when(blobSnapshotManager).cleanupSnapshot(TOPIC_NAME, PARTITION_ID);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(true).when(storageEngine).containsPartition(PARTITION_ID);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // Thread 1: cleanup snapshot
    Thread cleanupThread = new Thread(() -> {
      try {
        blobSnapshotManager.cleanupOutOfRetentionSnapshot(TOPIC_NAME, PARTITION_ID);
      } catch (Exception e) {
        Assert.fail("Exception in cleanup thread: " + e.getMessage());
      }
    });

    // Thread 2: Get transfer metadata and try to generate snapshot.
    Thread transferThread = new Thread(() -> {
      try {
        Thread.sleep(100); // Small delay to ensure cleanup thread has started first
        blobSnapshotManager.getTransferMetadata(blobTransferPayload, new AtomicBoolean(false));
      } catch (Exception e) {
        Assert.fail("Exception in transfer thread: " + e.getMessage());
      }
    });

    // Start
    cleanupThread.start();
    transferThread.start();

    try {
      cleanupThread.join(3000);
      transferThread.join(3000);
    } catch (InterruptedException e) {
      Assert.fail("Test testServeBlobTransferRequestWhileDeletingSnapshot interrupted");
    }

    // Verify cleanup was executed and snapshot was created
    verify(blobSnapshotManager, times(1)).cleanupSnapshot(TOPIC_NAME, PARTITION_ID);
    verify(blobSnapshotManager, times(1)).createSnapshot(TOPIC_NAME, PARTITION_ID);
  }
}
