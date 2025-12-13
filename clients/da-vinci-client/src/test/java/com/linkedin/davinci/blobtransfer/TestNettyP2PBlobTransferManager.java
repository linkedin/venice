package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferGlobalTrafficShapingHandlerHolder.getGlobalChannelTrafficShapingHandlerInstance;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.createAclHandler;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VenicePeersAllFailedException;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.IncrementalPushReplicaStatus;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
  AggBlobTransferStats blobTransferStats;
  AggVersionedBlobTransferStats versionedBlobTransferStats;
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

  Optional<SSLFactory> sslFactory;
  Optional<BlobTransferAclHandler> aclHandler;
  VeniceNotifier notifier;

  @BeforeMethod
  public void setUp() throws Exception {
    int port = TestUtils.getFreePort();
    int blobTransferMaxTimeoutInMin = 30;
    tmpSnapshotDir = Files.createTempDirectory(TMP_SNAPSHOT_DIR);
    tmpPartitionDir = Files.createTempDirectory(TMP_PARTITION_DIR);
    // intentionally use different directories for snapshot and partition so that we can verify the file transfer
    storageMetadataService = mock(StorageMetadataService.class);

    blobTransferStats = mock(AggBlobTransferStats.class);
    versionedBlobTransferStats = mock(AggVersionedBlobTransferStats.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler =
        getGlobalChannelTrafficShapingHandlerInstance(2000000, 2000000);

    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(Mockito.anyString());
    Mockito.doReturn(true).when(storageEngine).containsPartition(Mockito.anyInt());

    sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());

    Properties sslProperties = SslUtils.getVeniceLocalSslProperties();
    sslProperties.setProperty(ConfigKeys.BLOB_TRANSFER_SSL_ENABLED, "true");
    sslProperties.setProperty(ConfigKeys.BLOB_TRANSFER_ACL_ENABLED, "true");

    VeniceProperties veniceProperties = new VeniceProperties(sslProperties);
    VeniceConfigLoader configLoader = Mockito.mock(VeniceConfigLoader.class);

    Mockito.when(configLoader.getCombinedProperties()).thenReturn(veniceProperties);
    aclHandler = createAclHandler(configLoader);

    blobSnapshotManager = Mockito.spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    notifier = Mockito.mock(VeniceNotifier.class);
    Mockito.doNothing().when(notifier).startOfIncrementalPushReceived(anyString(), anyInt(), any(), anyString());
    Mockito.doNothing().when(notifier).endOfIncrementalPushReceived(anyString(), anyInt(), any(), anyString());

    server = new P2PBlobTransferService(
        port,
        tmpSnapshotDir.toString(),
        blobTransferMaxTimeoutInMin,
        blobSnapshotManager,
        globalChannelTrafficShapingHandler,
        blobTransferStats,
        sslFactory,
        aclHandler,
        20);
    client = Mockito.spy(
        new NettyFileTransferClient(
            port,
            tmpPartitionDir.toString(),
            storageMetadataService,
            30,
            60,
            blobTransferMaxTimeoutInMin,
            globalChannelTrafficShapingHandler,
            blobTransferStats,
            sslFactory,
            () -> notifier));
    finder = mock(BlobFinder.class);
    manager = new NettyP2PBlobTransferManager(server, client, finder, tmpPartitionDir.toString(), versionedBlobTransferStats, 5);
    manager.start();
  }

  @AfterMethod
  public void tearDown() throws Exception {
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
      future = client.get("remotehost123", "test_store", 1, 1, BlobTransferTableFormat.BLOCK_BASED_TABLE);
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
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
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
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    // Execution:
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VenicePeersNotFoundException);
    });
    // Verification:
    verifyFileTransferFailed(expectOffsetRecord);
  }

  /**
   * Testing the request blob transfer table format is not match to the existing snapshot format
   * @throws IOException
   */
  @Test
  public void testSnapshotFormatNotMatch() throws IOException {
    // Preparation:
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    // Execution:
    // Bootstrap try to get plain table snapshot,
    // but the existing snapshot is block based table, as the snapshot manager is initialized with block based table
    // config
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.PLAIN_TABLE);
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VeniceBlobTransferFileNotFoundException);
    });
  }

  @Test
  public void testLocalFileTransferInBatchStore()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    // Execution:
    // Manager should be able to fetch the file and download it to another directory
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
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
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    // Execution:
    // Manager should be able to fetch the file and download it to another directory, and future is done normally
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);
    future.whenComplete((result, throwable) -> {
      Assert.assertNull(throwable);
    });

    // Verification:
    Mockito.verify(client, Mockito.times(1))
        .get("localhost", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    // Verify that bad hosts are not called to fetch the file as they are no longer connectable
    Mockito.verify(client, Mockito.never())
        .get("badhost1", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Mockito.verify(client, Mockito.never())
        .get("badhost2", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Mockito.verify(client, Mockito.times(2)).purgeStaleConnectivityRecords(Mockito.any());

    verifyFileTransferSuccess(expectOffsetRecord);
  }

  /**
   * The client is initialized with host freshness 30 sec, so when purgeStaleConnectivityRecords is called,
   * All hosts connectivity records older than 30s should be purged.
   */
  @Test
  public void testPurgeStaleConnectivityRecords() {
    VeniceConcurrentHashMap<String, Long> connectivityMap = new VeniceConcurrentHashMap<>();
    connectivityMap.put("oldestHost", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(120));
    connectivityMap.put("newestHost", System.currentTimeMillis());
    client.purgeStaleConnectivityRecords(connectivityMap);

    Assert.assertEquals(connectivityMap.size(), 1);
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
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    // Execution:
    // Manager should be able to fetch the file and download it to another directory, and future is done normally
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);
    future.whenComplete((result, throwable) -> {
      Assert.assertNull(throwable);
    });

    // Verification:
    // Verify that it has the localhost in the first place, it should use localhost to transfer the file
    // All the remaining bad hosts should not be called to fetch the file.
    Mockito.verify(client, Mockito.times(1))
        .get("localhost", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Mockito.verify(client, Mockito.never())
        .get("badhost1", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Mockito.verify(client, Mockito.never())
        .get("badhost2", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    verifyFileTransferSuccess(expectOffsetRecord);
  }

  @Test
  public void testLocalFileTransferInHybridStore()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // Preparation:
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    Map<String, IncrementalPushReplicaStatus> incrementalPushInfo = new HashMap<>();
    incrementalPushInfo.put(
        "pushJobVersion1",
        new IncrementalPushReplicaStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 1000L));
    incrementalPushInfo.put(
        "pushJobVersion2",
        new IncrementalPushReplicaStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 2000L));
    expectOffsetRecord.setTrackingIncrementalPushStatus(incrementalPushInfo);
    expectOffsetRecord.setOffsetLag(1000L);

    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();

    // Execution:
    // Manager should be able to fetch the file and download it to another directory
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
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

  @Test
  public void testGetCompletesWithTimeoutExceptionAndClosesChannel() throws Exception {
    // Preparation:
    GlobalChannelTrafficShapingHandler newGlobalChannelTrafficShapingHandler =
        getGlobalChannelTrafficShapingHandlerInstance(2000000, 2000000);
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    snapshotPreparation();
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    int port = TestUtils.getFreePort();
    NettyFileTransferClient newClient = Mockito.spy(
        new NettyFileTransferClient(
            port,
            tmpPartitionDir.toString(),
            storageMetadataService,
            30,
            0, // general transfer timeout immediately
            10,
            newGlobalChannelTrafficShapingHandler,
            blobTransferStats,
            sslFactory,
            null));

    P2PBlobTransferService newServer = new P2PBlobTransferService(
        port,
        tmpSnapshotDir.toString(),
        20,
        blobSnapshotManager,
        newGlobalChannelTrafficShapingHandler,
        blobTransferStats,
        sslFactory,
        aclHandler,
        20);

    NettyP2PBlobTransferManager newManager =
        new NettyP2PBlobTransferManager(newServer, newClient, finder, tmpPartitionDir.toString(), versionedBlobTransferStats, 5);
    newManager.start();

    // Action
    CompletionStage<InputStream> future =
        newManager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    // Verify that the future completes exceptionally with a TimeoutException
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof TimeoutException);
      Assert.assertTrue(throwable.getMessage().contains("Timeout while waiting for blob transfer"));
    });
  }

  @Test
  public void testAllPeersFailException() {
    // Preparation:
    List<String> hostlist = Arrays.asList("badhost1", "badhost2", "badhost3");
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setDiscoveryResult(hostlist);

    doReturn(response).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord expectOffsetRecord =
        new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    expectOffsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(expectOffsetRecord)
        .when(storageMetadataService)
        .getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    // Execution:
    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    // Verification:
    Assert.assertTrue(future.toCompletableFuture().isCompletedExceptionally());
    future.whenComplete((result, throwable) -> {
      Assert.assertNotNull(throwable);
      Assert.assertTrue(throwable instanceof VenicePeersAllFailedException);
      Assert.assertTrue(throwable.getMessage().contains("failed to connect to any peer"));
    });
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
    Mockito.verify(storageMetadataService, Mockito.times(2))
        .getLastOffset(eq(TEST_STORE + "_v" + TEST_VERSION), eq(TEST_PARTITION), any());
    Mockito.verify(storageMetadataService, Mockito.times(2)).getStoreVersionState(TEST_STORE + "_v" + TEST_VERSION);

    // Verify the record is updated
    Mockito.verify(storageMetadataService, Mockito.times(1))
        .put(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, expectOffsetRecord);

    // Verify the store version state is updated
    Mockito.verify(storageMetadataService, Mockito.times(1))
        .computeStoreVersionState(Mockito.anyString(), Mockito.any());

    // Verify notifier is called for incremental push status updates
    if (!expectOffsetRecord.getTrackingIncrementalPushStatus().isEmpty()) {
      Mockito.verify(notifier, Mockito.times(1))
          .startOfIncrementalPushReceived(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, null, "pushJobVersion2");
      Mockito.verify(notifier, Mockito.times(1))
          .endOfIncrementalPushReceived(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, null, "pushJobVersion1");
    }
  }

  /**
   * Verify the file transfer is failed and the metadata is not updated
   * @param expectOffsetRecord
   */
  private void verifyFileTransferFailed(OffsetRecord expectOffsetRecord) {
    // Verify client never get called
    Mockito.verify(client, Mockito.never()).get(anyString(), anyString(), anyInt(), anyInt(), Mockito.any());

    // Verify files are not written to the partition directory
    Assert.assertFalse(Files.exists(destFile1));
    Assert.assertFalse(Files.exists(destFile2));
    Assert.assertFalse(Files.exists(destFile3));

    // Verify the metadata/record/svs is never retrieved/updated
    Mockito.verify(storageMetadataService, Mockito.never())
        .getLastOffset(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    Mockito.verify(storageMetadataService, Mockito.never()).getStoreVersionState(TEST_STORE + "_v" + TEST_VERSION);
    Mockito.verify(storageMetadataService, Mockito.never())
        .put(TEST_STORE + "_v" + TEST_VERSION, TEST_PARTITION, expectOffsetRecord);
    Mockito.verify(storageMetadataService, Mockito.never())
        .computeStoreVersionState(Mockito.anyString(), Mockito.any());
  }
}
