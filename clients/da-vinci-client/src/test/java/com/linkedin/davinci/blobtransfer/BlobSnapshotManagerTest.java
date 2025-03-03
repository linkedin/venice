package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);

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
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload);

    // Due to the store is hybrid, it will re-create a new snapshot.
    verify(storagePartition, times(1)).createSnapshot();
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);
  }

  @Test(timeOut = TIMEOUT)
  public void testSameSnapshotWhenConcurrentUsersNotExceedMaxAllowedUsers() {
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // Create snapshot for the first time
    BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(TOPIC_NAME, PARTITION_ID), 1);
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);

    // Try to create snapshot again with concurrent users
    actualBlobTransferPartitionMetadata = blobSnapshotManager.getTransferMetadata(blobTransferPayload);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(TOPIC_NAME, PARTITION_ID), 2);
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);
  }

  @Test(timeOut = TIMEOUT)
  public void testSameSnapshotWhenConcurrentUsersExceedsMaxAllowedUsers() {
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    when(mockStore.getVersion(VERSION_ID)).thenReturn(mockVersion);
    when(readOnlyStoreRepository.getStore(STORE_NAME)).thenReturn(mockStore);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);

    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // Create snapshot
    for (int tryCount = 0; tryCount < BlobSnapshotManager.DEFAULT_MAX_CONCURRENT_USERS; tryCount++) {
      BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
          blobSnapshotManager.getTransferMetadata(blobTransferPayload);
      Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);
    }

    // The last snapshot creation should fail
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload);
    } catch (VeniceException e) {
      String errorMessage = String.format(
          "Exceeded the maximum number of concurrent users %d for topic %s partition %d",
          BlobSnapshotManager.DEFAULT_MAX_CONCURRENT_USERS,
          TOPIC_NAME,
          PARTITION_ID);
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
    Assert.assertEquals(
        blobSnapshotManager.getConcurrentSnapshotUsers(TOPIC_NAME, PARTITION_ID),
        BlobSnapshotManager.DEFAULT_MAX_CONCURRENT_USERS);
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
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // first request for same payload but use offset 1
    BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
        blobSnapshotManager.getTransferMetadata(blobTransferPayload);
    Assert.assertEquals(actualBlobTransferPartitionMetadata, blobTransferPartitionMetadata);

    // second request for same payload but use offset 2
    BlobTransferPartitionMetadata blobTransferPartitionMetadata2 = Mockito.mock(BlobTransferPartitionMetadata.class);
    doReturn(blobTransferPartitionMetadata2).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);
    actualBlobTransferPartitionMetadata = blobSnapshotManager.getTransferMetadata(blobTransferPayload);
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
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    try {
      for (int i = 0; i < numberOfThreads; i++) {
        asyncExecutor.submit(() -> {
          BlobTransferPartitionMetadata actualBlobTransferPartitionMetadata =
              blobSnapshotManager.getTransferMetadata(blobTransferPayload);
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
        spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));
    doReturn(blobTransferPartitionMetadata).when(blobSnapshotManager).prepareMetadata(blobTransferPayload);

    AbstractStoragePartition storagePartition = Mockito.mock(AbstractStoragePartition.class);
    AbstractStorageEngine storageEngine = Mockito.mock(AbstractStorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(TOPIC_NAME);
    Mockito.doReturn(storagePartition).when(storageEngine).getPartitionOrThrow(PARTITION_ID);
    Mockito.doNothing().when(storagePartition).createSnapshot();

    // Mock there is one existing snapshot user
    blobSnapshotManager.increaseConcurrentUserCount(TOPIC_NAME, PARTITION_ID);

    // New request but the snapshot info is not recorded, and it will try to generate a new snapshot
    try {
      blobSnapshotManager.getTransferMetadata(blobTransferPayload);
      Assert.fail("Should throw exception");
    } catch (VeniceException e) {
      String errorMessage = String.format(
          "Snapshot for topic %s partition %d is still in use by others, can not recreate snapshot for new transfer request.",
          TOPIC_NAME,
          PARTITION_ID);
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }
}
