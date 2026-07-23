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
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceBlobTransferCancelledException;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VeniceBlobTransferIncompatibleSchemaException;
import com.linkedin.venice.exceptions.VenicePeersAllFailedException;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.IncrementalPushReplicaStatus;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.InOrder;
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
    aclHandler = createAclHandler(configLoader, Optional.empty(), false);

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
        20,
        25,
        true);
    client = Mockito.spy(
        new NettyFileTransferClient(
            port,
            tmpPartitionDir.toString(),
            storageMetadataService,
            30,
            60,
            blobTransferMaxTimeoutInMin,
            Math.max(4, Runtime.getRuntime().availableProcessors() / 5),
            globalChannelTrafficShapingHandler,
            blobTransferStats,
            sslFactory,
            () -> notifier,
            LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name())));
    finder = mock(BlobFinder.class);
    manager = new NettyP2PBlobTransferManager(
        server,
        client,
        finder,
        tmpPartitionDir.toString(),
        versionedBlobTransferStats,
        5,
        LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name()));
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

  @Test
  public void testFallsBackFromDaVinciPeerToServerAfterTransferFailure() throws Exception {
    BlobPeersDiscoveryResponse primaryResponse = new BlobPeersDiscoveryResponse();
    primaryResponse.setDiscoveryResult(Collections.singletonList("dvc-host"));
    doReturn(primaryResponse).when(finder).discoverBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    BlobPeersDiscoveryResponse fallbackResponse = new BlobPeersDiscoveryResponse();
    fallbackResponse.setDiscoveryResult(Collections.singletonList("server-host"));
    doReturn(true).when(finder).supportsFallback();
    doReturn(fallbackResponse).when(finder).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    doReturn(new HashSet<>(Arrays.asList("dvc-host", "server-host"))).when(client)
        .getConnectableHosts(any(), eq(TEST_STORE), eq(TEST_VERSION), eq(TEST_PARTITION));

    CompletableFuture<InputStream> daVinciFailure = new CompletableFuture<>();
    daVinciFailure.completeExceptionally(new VeniceBlobTransferFileNotFoundException("DVC snapshot unavailable"));
    InputStream serverResponse = mock(InputStream.class);
    doReturn(daVinciFailure).when(client)
        .get("dvc-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    doReturn(CompletableFuture.completedFuture(serverResponse)).when(client)
        .get("server-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    InputStream result =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE)
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    Assert.assertSame(result, serverResponse);
    InOrder transferOrder = Mockito.inOrder(finder, client);
    transferOrder.verify(finder).discoverBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    transferOrder.verify(client)
        .get("dvc-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    transferOrder.verify(finder).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    transferOrder.verify(client)
        .get("server-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
  }

  @Test
  public void testDoesNotDiscoverFallbackAfterPrimaryTransferSucceeds() throws Exception {
    BlobPeersDiscoveryResponse primaryResponse = new BlobPeersDiscoveryResponse();
    primaryResponse.setDiscoveryResult(Collections.singletonList("dvc-host"));
    doReturn(primaryResponse).when(finder).discoverBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    doReturn(true).when(finder).supportsFallback();
    doReturn(Collections.singleton("dvc-host")).when(client)
        .getConnectableHosts(any(), eq(TEST_STORE), eq(TEST_VERSION), eq(TEST_PARTITION));

    InputStream daVinciResponse = mock(InputStream.class);
    doReturn(CompletableFuture.completedFuture(daVinciResponse)).when(client)
        .get("dvc-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    InputStream result =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE)
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    Assert.assertSame(result, daVinciResponse);
    Mockito.verify(finder, Mockito.never()).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
  }

  @Test
  public void testDiscoversFallbackWhenNoPrimaryPeersExist() throws Exception {
    BlobPeersDiscoveryResponse primaryResponse = new BlobPeersDiscoveryResponse();
    primaryResponse.setDiscoveryResult(Collections.emptyList());
    doReturn(primaryResponse).when(finder).discoverBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    BlobPeersDiscoveryResponse fallbackResponse = new BlobPeersDiscoveryResponse();
    fallbackResponse.setDiscoveryResult(Collections.singletonList("server-host"));
    doReturn(true).when(finder).supportsFallback();
    doReturn(fallbackResponse).when(finder).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    doReturn(Collections.singleton("server-host")).when(client)
        .getConnectableHosts(any(), eq(TEST_STORE), eq(TEST_VERSION), eq(TEST_PARTITION));

    InputStream serverResponse = mock(InputStream.class);
    doReturn(CompletableFuture.completedFuture(serverResponse)).when(client)
        .get("server-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    InputStream result =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE)
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    Assert.assertSame(result, serverResponse);
    Mockito.verify(finder).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
  }

  @Test
  public void testCancellationDoesNotDiscoverFallback() throws Exception {
    BlobPeersDiscoveryResponse primaryResponse = new BlobPeersDiscoveryResponse();
    primaryResponse.setDiscoveryResult(Collections.singletonList("dvc-host"));
    doReturn(primaryResponse).when(finder).discoverBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
    doReturn(true).when(finder).supportsFallback();
    doReturn(Collections.singleton("dvc-host")).when(client)
        .getConnectableHosts(any(), eq(TEST_STORE), eq(TEST_VERSION), eq(TEST_PARTITION));

    CompletableFuture<InputStream> daVinciTransfer = new CompletableFuture<>();
    doReturn(daVinciTransfer).when(client)
        .get("dvc-host", TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);

    CompletableFuture<InputStream> result =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE)
            .toCompletableFuture();
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(TEST_STORE, TEST_VERSION), TEST_PARTITION);
    manager.getTransferStatusTrackingManager().cancelTransfer(replicaId);
    daVinciTransfer.completeExceptionally(new VeniceBlobTransferFileNotFoundException("DVC snapshot unavailable"));

    try {
      result.get(10, TimeUnit.SECONDS);
      Assert.fail("Expected the partition transfer to be cancelled");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceBlobTransferCancelledException);
    }
    Mockito.verify(finder, Mockito.never()).discoverFallbackBlobPeers(TEST_STORE, TEST_VERSION, TEST_PARTITION);
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
            Math.max(4, Runtime.getRuntime().availableProcessors() / 5),
            newGlobalChannelTrafficShapingHandler,
            blobTransferStats,
            sslFactory,
            null,
            LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name())));

    P2PBlobTransferService newServer = new P2PBlobTransferService(
        port,
        tmpSnapshotDir.toString(),
        20,
        blobSnapshotManager,
        newGlobalChannelTrafficShapingHandler,
        blobTransferStats,
        sslFactory,
        aclHandler,
        20,
        25,
        true);

    NettyP2PBlobTransferManager newManager = new NettyP2PBlobTransferManager(
        newServer,
        newClient,
        finder,
        tmpPartitionDir.toString(),
        versionedBlobTransferStats,
        5,
        LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name()));
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

  /**
   * On schema-version mismatch, the server rejects with 412 before any file work, so the partition
   * directory must NOT be wiped — that would clobber whatever the caller already had on disk for no
   * reason. Contrast with a generic peer failure, which still triggers the cleanup.
   */
  @Test
  public void testSchemaMismatchFromPeerSkipsPartitionDirCleanup() throws IOException {
    String kafkaTopic = Version.composeKafkaTopic(TEST_STORE, TEST_VERSION);
    Path partitionDir =
        Paths.get(RocksDBUtils.composePartitionDbDir(tmpPartitionDir.toString(), kafkaTopic, TEST_PARTITION));
    Files.createDirectories(partitionDir);
    Path canary = partitionDir.resolve("canary");
    Files.write(canary, "untouched".getBytes());

    BlobPeersDiscoveryResponse discovery = new BlobPeersDiscoveryResponse();
    discovery.setDiscoveryResult(Collections.singletonList("peer-mismatch:1234"));
    doReturn(discovery).when(finder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    CompletableFuture<InputStream> mismatchFuture = new CompletableFuture<>();
    mismatchFuture.completeExceptionally(
        new VeniceBlobTransferIncompatibleSchemaException("peer-mismatch:1234", "synthetic schema mismatch"));
    Mockito.doReturn(mismatchFuture)
        .when(client)
        .get(eq("peer-mismatch:1234"), eq(TEST_STORE), eq(TEST_VERSION), eq(TEST_PARTITION), any());

    CompletionStage<InputStream> future =
        manager.get(TEST_STORE, TEST_VERSION, TEST_PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    try {
      future.toCompletableFuture().get(10, TimeUnit.SECONDS);
      Assert.fail(
          "Expected fall-through to VenicePeersAllFailedException after the only peer rejected with schema mismatch");
    } catch (ExecutionException e) {
      Assert.assertTrue(
          e.getCause() instanceof VenicePeersAllFailedException,
          "Expected VenicePeersAllFailedException after all peers failed, got: " + e.getCause());
    } catch (InterruptedException | TimeoutException e) {
      Assert.fail("Future did not complete within timeout: " + e);
    }

    Assert.assertTrue(Files.exists(canary), "partition dir must be left intact on schema-mismatch rejection");
  }
}
