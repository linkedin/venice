package com.linkedin.davinci.blobtransfer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestNettyP2PBlobTransferManager {
  BlobFinder finder;
  P2PBlobTransferService server;
  NettyFileTransferClient client;
  NettyP2PBlobTransferManager manager;
  Path tmpSnapshotDir;
  Path tmpPartitionDir;
  String TEST_STORE = "test_store";
  String TMP_SNAPSHOT_DIR = "tmpSnapshotDir";
  String TMP_PARTITION_DIR = "tmpPartitionDir";
  int TEST_VERSION = 1;
  int TEST_PARTITION = 0;

  @BeforeClass
  public void setUp() throws Exception {
    int port = TestUtils.getFreePort();
    tmpSnapshotDir = Files.createTempDirectory(TMP_SNAPSHOT_DIR);
    tmpPartitionDir = Files.createTempDirectory(TMP_PARTITION_DIR);
    // intentionally use different directories for snapshot and partition so that we can verify the file transfer
    server = new P2PBlobTransferService(port, tmpSnapshotDir.toString());
    client = new NettyFileTransferClient(port, tmpPartitionDir.toString());
    finder = mock(BlobFinder.class);
    manager = new NettyP2PBlobTransferManager(server, client, finder);
    manager.start();
  }

  @AfterClass
  public void teardown() throws Exception {
    manager.close();
    Files.walk(tmpSnapshotDir).sorted(Comparator.reverseOrder()).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    Files.walk(tmpPartitionDir).sorted(Comparator.reverseOrder()).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  @Test
  public void testFailedConnectPeer() {
    CompletionStage<InputStream> future = client.get("remotehost123", "test_store", 1, 1);
    Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
  }

  @Test
  public void testFailedRequestFromFinder() {
    doReturn(null).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());
    try {
      manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
      Assert.fail("Should have thrown exception");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VenicePeersNotFoundException);
      Assert.assertEquals(e.getMessage(), "Failed to obtain the peers for the requested blob");
    }
  }

  @Test
  public void testNoResultFromFinder() {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());
    try {
      manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
      Assert.fail("Should have thrown exception");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VenicePeersNotFoundException);
      Assert.assertEquals(e.getMessage(), "No peers found for the requested blob");
    }
  }

  @Test
  public void testLocalFileTransfer() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    // Prepare files in the snapshot directory
    Path snapshotDir = Paths.get(
        RocksDBUtils.composeSnapshotDir(tmpSnapshotDir.toString(), TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION));
    Path partitionDir = Paths.get(
        RocksDBUtils
            .composePartitionDbDir(tmpPartitionDir.toString(), TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1.txt");
    Path file2 = snapshotDir.resolve("file2.txt");
    Path file3 = snapshotDir.resolve("file3.txt");
    Path destFile1 = partitionDir.resolve("file1.txt");
    Path destFile2 = partitionDir.resolve("file2.txt");
    Path destFile3 = partitionDir.resolve("file3.txt");
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

    // Manager should be able to fetch the file and download it to another directory
    CompletionStage<InputStream> future = manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // Verify files are all written to the partition directory
    Assert.assertTrue(Files.exists(destFile1));
    Assert.assertTrue(Files.exists(destFile2));
    Assert.assertTrue(Files.exists(destFile3));
    // same content
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file1), Files.readAllBytes(destFile1)));
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file2), Files.readAllBytes(destFile2)));
    Assert.assertTrue(Arrays.equals(Files.readAllBytes(file3), Files.readAllBytes(destFile3)));
  }
}
