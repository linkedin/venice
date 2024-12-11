package com.linkedin.davinci.blobtransfer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestNettyP2PBlobTransferManager {
  BlobFinder finder;
  P2PBlobTransferService server;
  NettyFileTransferClient client;
  NettyP2PBlobTransferManager manager;
  StorageMetadataService storageMetadataService;
  BlobSnapshotManager blobSnapshotManager;
  AggVersionedBlobTransferStats blobTransferStats;
  Path tmpSnapshotDir;
  Path tmpPartitionDir;
  String TEST_STORE = "test_store";
  String TMP_SNAPSHOT_DIR = "tmpSnapshotDir";
  String TMP_PARTITION_DIR = "tmpPartitionDir";
  int TEST_VERSION = 1;
  int TEST_PARTITION = 0;
  Path file1;
  Path file2;
  Path file3;
  Path destFile1;
  Path destFile2;
  Path destFile3;

  @BeforeMethod
  public void setUp() throws Exception {
    int port = TestUtils.getFreePort();
    int blobTransferMaxTimeoutInMin = 30;
    tmpSnapshotDir = Files.createTempDirectory(TMP_SNAPSHOT_DIR);
    tmpPartitionDir = Files.createTempDirectory(TMP_PARTITION_DIR);
    // intentionally use different directories for snapshot and partition so that we can verify the file transfer
    storageMetadataService = mock(StorageMetadataService.class);

    blobTransferStats = mock(AggVersionedBlobTransferStats.class);
    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    blobSnapshotManager =
        Mockito.spy(new BlobSnapshotManager(readOnlyStoreRepository, storageEngineRepository, storageMetadataService));

    server =
        new P2PBlobTransferService(port, tmpSnapshotDir.toString(), blobTransferMaxTimeoutInMin, blobSnapshotManager);
    client = Mockito.spy(new NettyFileTransferClient(port, tmpPartitionDir.toString(), storageMetadataService));
    finder = mock(BlobFinder.class);

    manager = new NettyP2PBlobTransferManager(server, client, finder, tmpPartitionDir.toString(), blobTransferStats);
    manager.start();
  }

  @AfterMethod
  public void teardown() throws Exception {
    manager.close();
    // if the partition directory is not empty, delete all files
    if (Files.exists(tmpSnapshotDir)) {
      Files.walk(tmpSnapshotDir).sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.delete(path);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
    if (Files.exists(tmpPartitionDir)) {
      Files.walk(tmpPartitionDir).sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.delete(path);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  @Test
  public void testFailedConnectPeer() {
    CompletionStage<InputStream> future = null;
    try {
      future = client.get("remotehost123", "test_store", 1, 1);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VenicePeersConnectionException);
      Assert.assertEquals(e.getMessage(), "Failed to connect to the host: remotehost123");
      Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
    }
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VenicePeersConnectionException);
    });
  }

  @Test
  public void testFailedRequestFromFinder() {
    doReturn(null).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VenicePeersNotFoundException);
    });
  }

  @Test
  public void testNoResultFromFinder() {
    // Preparation:
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord = new OffsetRecord(partitionStateSerializer);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    // Execution:
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VenicePeersNotFoundException);
    });
    // Verification:
    verifyFileTransferFailed(expectOffsetRecord);
  }

  @Test
  public void testLocalFileTransferInBatchStore()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation:
    Mockito.doReturn(false).when(blobSnapshotManager).isStoreHybrid(anyString(), anyInt());

    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord = new OffsetRecord(partitionStateSerializer);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    snapshotPreparation();

    // Execution:
    // Manager should be able to fetch the file and download it to another directory
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // Verification:
    verifyFileTransferSuccess(expectOffsetRecord);
  }

  /**
   * Host order: badhost1, badhost2, localhost (good host).
   * Test the case where the host is bad and cannot connect, the nettyP2PBlobTransferManager should throw an exception,
   * and it will move on and use the good host to transfer the file
   */
  @Test
  public void testSkipBadHostAndUseCorrectHost()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation:
    List<String> hostlist = Arrays.asList("badhost1", "badhost2", "localhost");
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(hostlist);

    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord = new OffsetRecord(partitionStateSerializer);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    snapshotPreparation();

    // Execution:
    // Manager should be able to fetch the file and download it to another directory, and future is done normally
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);
    future.whenComplete((result, throwable) -> {
      Assert.assertNull(throwable);
    });

    // Verification:
    // Verify that even has bad hosts in the list, it still finally uses good host to transfer the file
    Mockito.verify(client, Mockito.times(1)).get("localhost", TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Mockito.verify(client, Mockito.times(1)).get("badhost1", TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Mockito.verify(client, Mockito.times(1)).get("badhost2", TEST_STORE, TEST_VERSION, TEST_PARTITION);

    verifyFileTransferSuccess(expectOffsetRecord);
  }

  /**
   * Host order: localhost, badhost1, badhost2
   * Test when the localhost (good host) is the first host in the list, the nettyP2PBlobTransferManager should use the localhost,
   * and all bad hosts should not call the client.get for the file transfer, all remaining hosts should be skipped.
   */
  @Test
  public void testUseCorrectHostAndSkipRemainingHosts()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation:
    List<String> hostlist = Arrays.asList("localhost", "badhost1", "badhost2");
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(hostlist);
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord = new OffsetRecord(partitionStateSerializer);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    snapshotPreparation();

    // Execution:
    // Manager should be able to fetch the file and download it to another directory, and future is done normally
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);
    future.whenComplete((result, throwable) -> {
      Assert.assertNull(throwable);
    });

    // Verification:
    // Verify that it has the localhost in the first place, it should use localhost to transfer the file
    // All the remaining bad hosts should not be called to fetch the file.
    Mockito.verify(client, Mockito.times(1)).get("localhost", TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Mockito.verify(client, Mockito.never()).get("badhost1", TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Mockito.verify(client, Mockito.never()).get("badhost2", TEST_STORE, TEST_VERSION, TEST_PARTITION);

    verifyFileTransferSuccess(expectOffsetRecord);
  }

  @Test
  public void testLocalFileTransferInHybridStore()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation:
    Mockito.doReturn(true).when(blobSnapshotManager).isStoreHybrid(anyString(), anyInt());
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord = new OffsetRecord(partitionStateSerializer);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    snapshotPreparation();

    // Execution:
    // Manager should be able to fetch the file and download it to another directory
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // Verification:
    verifyFileTransferSuccess(expectOffsetRecord);

    // Verify the createSnapshot is called
    Mockito.verify(blobSnapshotManager, Mockito.times(1)).prepareMetadata(Mockito.any());
    Mockito.verify(blobSnapshotManager, Mockito.times(1)).createSnapshot(TEST_STORE + "_v" + TEST_VERSION, 0);

    // Verify the concurrent user of this partition is 0 as it should firstly be 1 and after the file is sent,
    // it should decrease to 0
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers(TEST_STORE + "_v" + TEST_VERSION, 0), 0);
  }

  /**
   * Prepare files in the snapshot directory
   * @throws IOException
   */
  private void snapshotPreparation() throws IOException {
    // Prepare files in the snapshot directory
    Path snapshotDir = Paths.get(
        RocksDBUtils.composeSnapshotDir(tmpSnapshotDir.toString(), TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION));
    Path partitionDir = Paths.get(
        RocksDBUtils
            .composePartitionDbDir(tmpPartitionDir.toString(), TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION));
    Files.createDirectories(snapshotDir);
    file1 = snapshotDir.resolve("file1.txt");
    file2 = snapshotDir.resolve("file2.txt");
    file3 = snapshotDir.resolve("file3.txt");
    destFile1 = partitionDir.resolve("file1.txt");
    destFile2 = partitionDir.resolve("file2.txt");
    destFile3 = partitionDir.resolve("file3.txt");
    // small file
    Files.write(file1.toAbsolutePath(), "helloworld".getBytes());
    Files.write(file3.toAbsolutePath(), "helloworldtwice".getBytes());
    // large file
    long size = 10 * 1024 * 1024;
    // Create an array of dummy bytes
    byte[] dummyData = new byte[1024]; // 1KB of dummy data
    Arrays.fill(dummyData, (byte) 0); // Fill with zeros or any dummy value

    // Write data to the file in chunks
    for (long written = 0; written < size; written += dummyData.length) {
      Files.write(file2.toAbsolutePath(), dummyData, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // both files don't exist in the partition directory
    Assert.assertTrue(Files.notExists(destFile1));
    Assert.assertTrue(Files.notExists(destFile2));
    Assert.assertTrue(Files.notExists(destFile3));
  }

  /**
   * Verify the file transfer is successful and the metadata is updated
   * @param expectOffsetRecord
   * @throws IOException
   */
  private void verifyFileTransferSuccess(OffsetRecord expectOffsetRecord) throws IOException {
    // Verify files are all written to the partition directory
    Assert.assertTrue(Files.exists(destFile1));
    Assert.assertTrue(Files.exists(destFile2));
    Assert.assertTrue(Files.exists(destFile3));
    // same content
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file1), Files.readAllBytes(destFile1)));
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file2), Files.readAllBytes(destFile2)));
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file3), Files.readAllBytes(destFile3)));

    // Verify the metadata is retrieved
    Mockito.verify(storageMetadataService, Mockito.times(1))
        .getLastOffset(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION);
    Mockito.verify(storageMetadataService, Mockito.times(1)).getStoreVersionState(TEST_STORE + "_v" + TEST_VERSION);

    // Verify the record is updated
    Mockito.verify(storageMetadataService, Mockito.times(1))
        .put(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, expectOffsetRecord);

    // Verify the store version state is updated
    Mockito.verify(storageMetadataService, Mockito.times(1))
        .computeStoreVersionState(Mockito.anyString(), Mockito.any());
  }

  /**
   * Verify the file transfer is failed and the metadata is not updated
   * @param expectOffsetRecord
   */
  private void verifyFileTransferFailed(OffsetRecord expectOffsetRecord) {
    // Verify client never get called
    Mockito.verify(client, Mockito.never()).get(anyString(), anyString(), anyInt(), anyInt());

    // Verify files are not written to the partition directory
    Assert.assertFalse(Files.exists(destFile1));
    Assert.assertFalse(Files.exists(destFile2));
    Assert.assertFalse(Files.exists(destFile3));

    // Verify the metadata/record/svs is never retrieved/updated
    Mockito.verify(storageMetadataService, Mockito.never())
        .getLastOffset(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION);
    Mockito.verify(storageMetadataService, Mockito.never()).getStoreVersionState(TEST_STORE + "_v" + TEST_VERSION);
    Mockito.verify(storageMetadataService, Mockito.never())
        .put(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, expectOffsetRecord);
    Mockito.verify(storageMetadataService, Mockito.never())
        .computeStoreVersionState(Mockito.anyString(), Mockito.any());
  }
}
