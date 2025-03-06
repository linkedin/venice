package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBlobTransferManagerBuilder {
  @Test
  public void testInitBlobTransferManager() throws IOException {
    int port = TestUtils.getFreePort();
    Path tmpPartitionDir = Files.createTempDirectory("tmpPartitionDir");
    StorageMetadataService storageMetadataService = mock(StorageMetadataService.class);
    AggVersionedBlobTransferStats blobTransferStats = mock(AggVersionedBlobTransferStats.class);
    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    ClientConfig clientConfig = mock(ClientConfig.class);

    P2PBlobTransferConfig blobTransferConfig = new P2PBlobTransferConfig(
        port,
        port,
        tmpPartitionDir.toAbsolutePath().toString(),
        5,
        30,
        30,
        BlobTransferUtils.BlobTransferTableFormat.PLAIN_TABLE,
        2000000,
        2000000,
        2000000);

    BlobTransferManager blobTransferManager = new BlobTransferManagerBuilder().setBlobTransferConfig(blobTransferConfig)
        .setClientConfig(clientConfig)
        .setCustomizedViewFuture(null)
        .setStorageMetadataService(storageMetadataService)
        .setReadOnlyStoreRepository(readOnlyStoreRepository)
        .setStorageEngineRepository(storageEngineRepository)
        .setAggVersionedBlobTransferStats(blobTransferStats)
        .build();

    Assert.assertNotNull(blobTransferManager);
  }

  @Test
  public void testFailToCreateBlobTransferManager() throws IOException {
    int port = TestUtils.getFreePort();
    Path tmpPartitionDir = Files.createTempDirectory("tmpPartitionDir");
    StorageMetadataService storageMetadataService = mock(StorageMetadataService.class);
    AggVersionedBlobTransferStats blobTransferStats = mock(AggVersionedBlobTransferStats.class);
    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    ClientConfig clientConfig = mock(ClientConfig.class);
    CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture = mock(CompletableFuture.class);

    P2PBlobTransferConfig blobTransferConfig = new P2PBlobTransferConfig(
        port,
        port,
        tmpPartitionDir.toAbsolutePath().toString(),
        5,
        30,
        30,
        BlobTransferUtils.BlobTransferTableFormat.PLAIN_TABLE,
        2000000,
        2000000,
        2000000);

    // Case 1: expect exception is thrown due to both clientConfig and customizedViewFuture are not null
    try {
      BlobTransferManager blobTransferManager =
          new BlobTransferManagerBuilder().setBlobTransferConfig(blobTransferConfig)
              .setClientConfig(clientConfig)
              .setCustomizedViewFuture(customizedViewFuture)
              .setStorageMetadataService(storageMetadataService)
              .setReadOnlyStoreRepository(readOnlyStoreRepository)
              .setStorageEngineRepository(storageEngineRepository)
              .setAggVersionedBlobTransferStats(blobTransferStats)
              .build();
      Assert.assertNull(blobTransferManager);
    } catch (IllegalArgumentException e) {
      Assert
          .assertTrue(e.getMessage().contains("The client config and customized view future must one of them be null"));
    }

    // Case 2: expect exception is thrown due to both clientConfig and customizedViewFuture are null
    try {
      BlobTransferManager blobTransferManager1 =
          new BlobTransferManagerBuilder().setBlobTransferConfig(blobTransferConfig)
              .setClientConfig(null)
              .setCustomizedViewFuture(null)
              .setStorageMetadataService(storageMetadataService)
              .setReadOnlyStoreRepository(readOnlyStoreRepository)
              .setStorageEngineRepository(storageEngineRepository)
              .setAggVersionedBlobTransferStats(blobTransferStats)
              .build();
      Assert.assertNull(blobTransferManager1);
    } catch (IllegalArgumentException e) {
      Assert
          .assertTrue(e.getMessage().contains("The client config and customized view future must one of them be null"));
    }

    // Case 3: expect exception is thrown due to null blobTransferConfig
    try {
      BlobTransferManager blobTransferManager2 = new BlobTransferManagerBuilder().setBlobTransferConfig(null)
          .setClientConfig(clientConfig)
          .setCustomizedViewFuture(null)
          .setStorageMetadataService(storageMetadataService)
          .setReadOnlyStoreRepository(readOnlyStoreRepository)
          .setStorageEngineRepository(storageEngineRepository)
          .setAggVersionedBlobTransferStats(blobTransferStats)
          .build();
      Assert.assertNull(blobTransferManager2);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "The blob transfer config, storage metadata service, read only store repository, storage engine repository, and agg versioned blob transfer stats must not be null"));
    }
  }
}
