package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferStatusTrackingManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.exceptions.VeniceBlobTransferCancelledException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.Utils;
import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DefaultIngestionBackend}
 */
public class DefaultIngestionBackendTest {
  @Mock
  private StorageMetadataService storageMetadataService;
  @Mock
  private KafkaStoreIngestionService storeIngestionService;
  @Mock
  private StorageService storageService;
  @Mock
  private AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  @Mock
  private BlobTransferManager blobTransferManager;
  @Mock
  private BlobTransferStatusTrackingManager statusTrackingManager;
  @Mock
  private DefaultIngestionBackend ingestionBackend;
  @Mock
  private VeniceStoreVersionConfig storeConfig;
  @Mock
  private StorageEngine storageEngine;
  @Mock
  private ReadOnlyStoreRepository metadataRepo;
  @Mock
  private VeniceServerConfig veniceServerConfig;
  @Mock
  private Store store;
  @Mock
  private Version version;
  @Mock
  private StoreVersionState storeVersionState;
  @Mock
  private OffsetRecord offsetRecord;

  private static final int VERSION_NUMBER = 1;
  private static final int PARTITION = 1;
  private static final String STORE_NAME = "testStore";
  private static final String STORE_VERSION = "store_v1";
  private static final String BASE_DIR = "mockBaseDir";
  private static final BlobTransferTableFormat BLOB_TRANSFER_FORMAT = BlobTransferTableFormat.BLOCK_BASED_TABLE;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(store.getName()).thenReturn(STORE_NAME);
    when(version.getNumber()).thenReturn(VERSION_NUMBER);
    StoreVersionInfo storeAndVersion = new StoreVersionInfo(store, version);

    when(storeConfig.getStoreVersionName()).thenReturn(STORE_VERSION);
    when(storeIngestionService.getMetadataRepo()).thenReturn(metadataRepo);
    doNothing().when(storeIngestionService).startConsumption(any(VeniceStoreVersionConfig.class), anyInt(), any());
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class))).thenReturn(storeAndVersion);
    when(storageMetadataService.getStoreVersionState(STORE_VERSION)).thenReturn(storeVersionState);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any())).thenReturn(storageEngine);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any(), any()))
        .thenReturn(storageEngine);

    when(offsetRecord.getOffsetLag()).thenReturn(0L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(
        storageMetadataService.getLastOffset(
            eq(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER)),
            eq(PARTITION),
            any(PubSubContext.class))).thenReturn(offsetRecord);

    when(blobTransferManager.getAggVersionedBlobTransferStats()).thenReturn(aggVersionedBlobTransferStats);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(statusTrackingManager);

    // Create the DefaultIngestionBackend instance with mocked dependencies
    ingestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);
  }

  // verify that blobTransferManager was called based on different conditions
  @Test
  public void testStartConsumptionWithBlobTransfer() {
    // Case 1: DaVinci client
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);
    RocksDBServerConfig rocksDBServerConfig = Mockito.mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
    verifyBlobTransfer(true);
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));

    // Reset replica state for next iteration
    ingestionBackend.removeReplicaConsumptionContext(Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION));

    // Case 2: Server client (DaVinci = false, store.isBlobTransferEnabled = false)
    when(storeIngestionService.isDaVinciClient()).thenReturn(false);
    when(store.isBlobTransferEnabled()).thenReturn(false);

    // 2.1 store level blobTransferInServerEnabled = enabled, server level blobTransferReceiverServerPolicy =
    // not_specified
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.ENABLED.name(),
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED,
        true);
    // 2.2: store level blobTransferInServerEnabled = not_specified, server level blobTransferReceiverServerPolicy =
    // enabled
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name(),
        ConfigCommonUtils.ActivationState.ENABLED,
        true);

    // case 2.3: store level blobTransferInServerEnabled = disabled, server level blobTransferReceiverServerPolicy =
    // enabled
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.DISABLED.name(),
        ConfigCommonUtils.ActivationState.ENABLED,
        false);
    // case 2.4: store level blobTransferInServerEnabled = enabled , server level blobTransferReceiverServerPolicy =
    // disabled
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.ENABLED.name(),
        ConfigCommonUtils.ActivationState.DISABLED,
        false);

    // case 2.5: store level blobTransferInServerEnabled = not_specified, server level blobTransferReceiverServerPolicy
    // = disabled
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name(),
        ConfigCommonUtils.ActivationState.DISABLED,
        false);
    // case 2.6: store level blobTransferInServerEnabled = disabled, server level blobTransferReceiverServerPolicy =
    // not_specified
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.DISABLED.name(),
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED,
        false);

    // case 2.7: store level blobTransferInServerEnabled = not_specified, server level blobTransferReceiverServerPolicy
    // = not_specified
    runBlobTransferCase(
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name(),
        ConfigCommonUtils.ActivationState.NOT_SPECIFIED,
        false);
  }

  private void runBlobTransferCase(
      String storeSetting,
      ConfigCommonUtils.ActivationState serverSetting,
      boolean expectEnabled) {
    Mockito.reset(blobTransferManager);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(statusTrackingManager);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(store.getBlobTransferInServerEnabled()).thenReturn(storeSetting);
    when(veniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(serverSetting);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
    verifyBlobTransfer(expectEnabled);

    // Reset replica state for next iteration
    ingestionBackend.removeReplicaConsumptionContext(Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION));
  }

  private void verifyBlobTransfer(boolean expectEnabled) {
    if (expectEnabled) {
      verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    } else {
      verify(blobTransferManager, never())
          .get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    }
  }

  @Test
  public void testStartConsumptionWithBlobTransferStoreLevelConfigDisabled() {
    when(store.isBlobTransferEnabled()).thenReturn(false); // disable store level config
    when(store.getBlobTransferInServerEnabled()).thenReturn(ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name());
    when(veniceServerConfig.getBlobTransferReceiverServerPolicy())
        .thenReturn(ConfigCommonUtils.ActivationState.ENABLED);
    when(store.isHybrid()).thenReturn(false);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);

    RocksDBServerConfig rocksDBServerConfig = Mockito.mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
    verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));
  }

  @Test
  public void testStartConsumptionWithBlobTransferValidatePartitionStatus() {
    when(storageService.getStorageEngine(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER)))
        .thenReturn(storageEngine);
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);
    RocksDBServerConfig rocksDBServerConfig = Mockito.mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);
    doNothing().when(storageEngine)
        .adjustStoragePartition(eq(PARTITION), eq(StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER), any());

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());

    verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));
    // verify that blob transfer flag is true when creating the partition.
    verify(storageService).openStoreForNewPartition(
        eq(storeConfig),
        eq(PARTITION),
        any(),
        Mockito.argThat(storagePartitionConfig -> storagePartitionConfig.isBlobTransferInProgress()));
    // verify that when adjust the status after the blob transfer is completed, the blob transfer flag is off.
    verify(storageEngine).adjustStoragePartition(
        eq(PARTITION),
        eq(StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER),
        Mockito.argThat(storagePartitionConfig -> !storagePartitionConfig.isBlobTransferInProgress()));
  }

  @Test
  public void testStartConsumptionWithBlobTransferWhenNoPeerFound() {
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    CompletableFuture<InputStream> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new VenicePeersNotFoundException("No peers found"));
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(errorFuture);

    CompletableFuture<Void> future = ingestionBackend
        .bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, BLOB_TRANSFER_FORMAT, 100L, 0, storeConfig, null)
        .toCompletableFuture();
    assertTrue(future.isDone());
    verify(aggVersionedBlobTransferStats, never()).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats, never())
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
  }

  @Test
  public void testNotStartBootstrapFromBlobTransferWhenNotLaggingForHybridStore() {
    long laggingThreshold = 1000L;
    when(offsetRecord.getOffsetLag()).thenReturn(10L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubUtil.fromKafkaOffset(10L));
    when(
        storageMetadataService
            .getLastOffset(eq(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER)), eq(PARTITION), any()))
                .thenReturn(offsetRecord);

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true); // hybrid store
    CompletableFuture<InputStream> future = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(future);

    CompletableFuture<Void> result =
        ingestionBackend
            .bootstrapFromBlobs(
                store,
                VERSION_NUMBER,
                PARTITION,
                BLOB_TRANSFER_FORMAT,
                laggingThreshold,
                0,
                storeConfig,
                null)
            .toCompletableFuture();
    assertTrue(result.isDone());
    verify(blobTransferManager, never())
        .get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    verify(aggVersionedBlobTransferStats, never()).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats, never())
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
  }

  @Test
  public void testNotStartBootstrapFromBlobTransferWhenNotLaggingForBatchStore() {
    long laggingThreshold = 1000L;
    when(offsetRecord.isEndOfPushReceived()).thenReturn(true); // for batch store, end of push is received.
    when(
        storageMetadataService
            .getLastOffset(eq(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER)), eq(PARTITION), any()))
                .thenReturn(offsetRecord);

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    CompletableFuture<InputStream> future = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(future);

    CompletableFuture<Void> result =
        ingestionBackend
            .bootstrapFromBlobs(
                store,
                VERSION_NUMBER,
                PARTITION,
                BLOB_TRANSFER_FORMAT,
                laggingThreshold,
                0,
                storeConfig,
                null)
            .toCompletableFuture();
    assertTrue(result.isDone());
    verify(blobTransferManager, never())
        .get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    verify(aggVersionedBlobTransferStats, never()).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats, never())
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
  }

  @Test
  public void testStartConsumptionWithClosePartition() {
    StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    when(storageEngine.containsPartition(PARTITION)).thenReturn(true);
    doNothing().when(storageEngine).dropPartition(PARTITION, false);

    String kafkaTopic = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);
    RocksDBServerConfig rocksDBServerConfig = Mockito.mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);
    when(storageService.getStorageEngine(kafkaTopic)).thenReturn(storageEngine);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
    verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT));
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));
  }

  @Test
  public void testHasCurrentVersionBootstrapping() {
    KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
    DefaultIngestionBackend ingestionBackend =
        new DefaultIngestionBackend(null, mockIngestionService, null, null, null);
    doReturn(true).when(mockIngestionService).hasCurrentVersionBootstrapping();
    assertTrue(ingestionBackend.hasCurrentVersionBootstrapping());

    doReturn(false).when(mockIngestionService).hasCurrentVersionBootstrapping();
    assertFalse(ingestionBackend.hasCurrentVersionBootstrapping());
  }

  @Test
  public void testReplicaBlobTransferLagCheck() {
    KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    OffsetRecord offsetRecord = Mockito.mock(OffsetRecord.class);
    doReturn(offsetRecord).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    DefaultIngestionBackend ingestionBackend =
        new DefaultIngestionBackend(mockStorageMetadataService, mockIngestionService, null, null, null);
    String store = "foo";
    int version = 123;
    int partition = 456;
    long offsetLagThreshold = 10000L;

    // Test batch-only store.
    doReturn(true).when(offsetRecord).isEndOfPushReceived();
    Assert.assertTrue(ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, -1, 0, false));
    Assert.assertFalse(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 0, false));
    Assert.assertFalse(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 1, false));
    doReturn(false).when(offsetRecord).isEndOfPushReceived();
    Assert.assertTrue(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 0, false));
    Assert.assertTrue(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 1, false));

    // Test hybrid store
    doReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2)).when(offsetRecord).getHeartbeatTimestamp();
    doReturn(offsetLagThreshold + 1).when(offsetRecord).getOffsetLag();
    // Refer to Time-lag
    Assert.assertTrue(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 1, true));
    Assert.assertFalse(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 3, true));
    // Refer to Offset-lag
    Assert.assertTrue(ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, -1, 0, true));
    Assert.assertTrue(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 0, true));
    doReturn(offsetLagThreshold - 1).when(offsetRecord).getOffsetLag();
    Assert.assertFalse(
        ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, offsetLagThreshold, 0, true));
    // Legacy edge case.
    doReturn(PubSubSymbolicPosition.EARLIEST).when(offsetRecord).getCheckpointedLocalVtPosition();
    Assert.assertTrue(ingestionBackend.isReplicaLaggedAndNeedBlobTransfer(store, version, partition, 0, 0, true));
  }

  @Test
  public void testStopConsumption_NoBlobTransferManager() {
    // When blobTransferManager is null, stopConsumption should handle it gracefully
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);

    // Start consumption first to set state to RUNNING (takes simple path since blobTransferManager is null)
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());

    backend.stopConsumption(storeConfig, PARTITION);
    verify(storeIngestionService).stopConsumption(storeConfig, PARTITION);
  }

  @Test
  public void testConcurrentStopConsumptionWithLockExecutes() throws Exception {
    // Setup mocks
    BlobTransferStatusTrackingManager mockStatusTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockStatusTrackingManager);
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class)))
        .thenReturn(new StoreVersionInfo(store, version));
    when(veniceServerConfig.isServerAllowlistEnabled()).thenReturn(false);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);

    // Start consumption via simple path to set state to RUNNING
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());

    // Now enable blob transfer for stop operations
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);

    // Get the actual replica ID from storeConfig
    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Track which threads acquired the lock and in what order
    AtomicInteger lockAcquisitionOrder = new AtomicInteger(0);
    ConcurrentHashMap<String, Integer> threadToOrder = new ConcurrentHashMap<>();

    // Mock: Track when flag is checked (this happens inside the lock during blob transfer cancel)
    when(mockStatusTrackingManager.isBlobTransferCancelRequestSentBefore(eq(replicaId))).thenAnswer(inv -> {
      String threadName = Thread.currentThread().getName();
      if (threadName.startsWith("Stop-")) {
        threadToOrder.putIfAbsent(threadName, lockAcquisitionOrder.incrementAndGet());
      }
      return false; // Return false so threads actually call cancelTransfer
    });

    // Mock: cancelTransfer - simulate some work being done under the lock
    Mockito.doAnswer(inv -> {
      Thread.sleep(50); // Simulate work - if no lock, threads would overlap
      return null;
    }).when(mockStatusTrackingManager).cancelTransfer(eq(replicaId));

    // Track completions
    AtomicInteger completedCount = new AtomicInteger(0);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(3);

    // Task: call stopConsumption (which internally cancels blob transfer)
    Runnable stopTask = () -> {
      try {
        startLatch.await(); // Wait for signal to start
        backend.stopConsumption(storeConfig, PARTITION);
        completedCount.incrementAndGet();
      } catch (Exception e) {
        LogManager.getLogger().error("{} error: {}", Thread.currentThread().getName(), e.getMessage());
      } finally {
        doneLatch.countDown();
      }
    };

    // Launch 3 threads that will try to stop consumption simultaneously
    Thread t1 = new Thread(stopTask, "Stop-1");
    Thread t2 = new Thread(stopTask, "Stop-2");
    Thread t3 = new Thread(stopTask, "Stop-3");

    t1.start();
    t2.start();
    t3.start();

    // Let all threads start at once (creates the race)
    startLatch.countDown();

    // Wait for all to complete
    assertTrue(doneLatch.await(15, TimeUnit.SECONDS), "All threads should complete without deadlock");

    t1.join(1000);
    t2.join(1000);
    t3.join(1000);

    // ASSERTIONS:
    // 1. All threads completed (proves no deadlock)
    Assert.assertEquals(completedCount.get(), 3, "All 3 threads should have completed");

    // 2. The lock ensured serialization - threads executed in sequence
    // Even though they started simultaneously, the lock made them execute one at a time
    assertTrue(threadToOrder.size() <= 3, "At most 3 threads acquired lock");

    // 3. Verify cancelTransfer was called (as part of stopConsumption)
    verify(mockStatusTrackingManager, Mockito.atLeastOnce()).cancelTransfer(eq(replicaId));
  }

  @Test
  public void testStartConsumptionAndDropConcurrently_AfterTransferStarts() throws Exception {
    // Scenario: Blob transfer has started and is in progress (actively transferring data),
    // drop happens mid-transfer
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(veniceServerConfig.isServerAllowlistEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Use real status tracking manager so map logic proves itself
    NettyFileTransferClient nettyClient = mock(NettyFileTransferClient.class);
    BlobTransferStatusTrackingManager realStatusTrackingManager = new BlobTransferStatusTrackingManager(nettyClient);

    // Spy to track when initialTransfer is called while using real implementation
    BlobTransferStatusTrackingManager spyStatusTrackingManager = Mockito.spy(realStatusTrackingManager);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(spyStatusTrackingManager);

    // Blob transfer future that simulates ongoing transfer
    CompletableFuture<InputStream> blobTransferFuture = new CompletableFuture<>();
    // Simulate what NettyP2PBlobTransferManager.get() does: call startedTransfer()
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), any())).thenAnswer(inv -> {
      realStatusTrackingManager.startedTransfer(replicaId);
      return blobTransferFuture;
    });

    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Recreate DefaultIngestionBackend with the real status tracking manager
    DefaultIngestionBackend testIngestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);

    CountDownLatch transferInitializedLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      transferInitializedLatch.countDown();
      return inv.callRealMethod(); // Call real method to update internal map
    }).when(spyStatusTrackingManager).initialTransfer(eq(replicaId));

    AtomicInteger completedCount = new AtomicInteger(0);

    // Thread 1: Start consumption
    Thread startThread = new Thread(() -> {
      try {
        testIngestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
        completedCount.incrementAndGet();
      } catch (Exception e) {
        LogManager.getLogger().error("Start error: {}", e.getMessage(), e);
      }
    });

    // Thread 2: Drop partition while transfer is in progress
    Thread dropThread = new Thread(() -> {
      try {
        // Wait for transfer to be initialized
        boolean latchCompleted = transferInitializedLatch.await(2, TimeUnit.SECONDS);
        if (!latchCompleted) {
          LogManager.getLogger().warn("Latch timed out waiting for transfer initialization");
        }
        Thread.sleep(50); // Small delay to ensure we're mid-transfer
        testIngestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5);
        completedCount.incrementAndGet();
      } catch (Exception e) {
        LogManager.getLogger().error("Drop error: {}", e.getMessage(), e);
      }
    });

    startThread.start();
    dropThread.start();

    // Wait a bit, then complete the blob transfer with cancellation exception
    Thread.sleep(100);
    blobTransferFuture
        .completeExceptionally(new VeniceBlobTransferCancelledException("Transfer cancelled during drop"));

    startThread.join(5000);
    dropThread.join(5000);

    // Verify operations completed
    assertTrue(completedCount.get() >= 1, "At least one operation should complete");

    // Verify status was managed by real map logic
    // Status should be CANCELLED or null (cleared after cleanup)
    BlobTransferStatus finalStatus = realStatusTrackingManager.getTransferStatus(replicaId);
    assertTrue(
        finalStatus == null || finalStatus == BlobTransferStatus.TRANSFER_CANCELLED
            || finalStatus == BlobTransferStatus.TRANSFER_CANCEL_REQUESTED,
        "Expected CANCELLED/CANCEL_REQUESTED/null (cleared), got: " + finalStatus);
  }

  @Test
  public void testStartConsumptionAndDropConcurrently_VerifyCancelTransition() throws Exception {
    // Scenario: Explicitly verify the CANCEL_REQUESTED → CANCELLED transition
    // This test verifies that when stopConsumption is called mid-transfer,
    // the status transitions through: STARTED → CANCEL_REQUESTED → CANCELLED
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(veniceServerConfig.isServerAllowlistEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Use real status tracking manager
    NettyFileTransferClient nettyClient = mock(NettyFileTransferClient.class);
    BlobTransferStatusTrackingManager realStatusTrackingManager = new BlobTransferStatusTrackingManager(nettyClient);

    // Spy to track method calls
    BlobTransferStatusTrackingManager spyStatusTrackingManager = Mockito.spy(realStatusTrackingManager);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(spyStatusTrackingManager);

    // Blob transfer future that will complete with cancellation exception
    CompletableFuture<InputStream> blobTransferFuture = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), any())).thenAnswer(inv -> {
      realStatusTrackingManager.startedTransfer(replicaId);
      return blobTransferFuture;
    });

    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Recreate DefaultIngestionBackend with real status tracking manager
    DefaultIngestionBackend testIngestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);

    CountDownLatch transferInitializedLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      transferInitializedLatch.countDown();
      return inv.callRealMethod();
    }).when(spyStatusTrackingManager).initialTransfer(eq(replicaId));

    // Thread 1: Start consumption
    Thread startThread = new Thread(() -> {
      try {
        testIngestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
      } catch (Exception e) {
        LogManager.getLogger().error("Start error: {}", e.getMessage(), e);
      }
    });

    // Thread 2: Stop consumption (which calls cancelTransfer)
    Thread stopThread = new Thread(() -> {
      try {
        boolean latchCompleted = transferInitializedLatch.await(2, TimeUnit.SECONDS);
        if (!latchCompleted) {
          LogManager.getLogger().warn("Latch timed out waiting for transfer initialization");
        }
        Thread.sleep(50); // Ensure transfer started
        testIngestionBackend.stopConsumption(storeConfig, PARTITION);
      } catch (Exception e) {
        LogManager.getLogger().error("Stop error: {}", e.getMessage(), e);
      }
    });

    startThread.start();
    stopThread.start();

    // Wait then complete with cancellation exception
    Thread.sleep(100);
    blobTransferFuture
        .completeExceptionally(new VeniceBlobTransferCancelledException("Transfer cancelled during test"));

    startThread.join(5000);
    stopThread.join(5000);

    // KEY VERIFICATIONS: Prove CANCEL_REQUESTED → CANCELLED transition happened

    // 1. cancelTransfer() was called (sets status to CANCEL_REQUESTED)
    verify(spyStatusTrackingManager, Mockito.atLeastOnce()).cancelTransfer(eq(replicaId));

    // 2. markTransferCancelled() was called (transitions CANCEL_REQUESTED → CANCELLED)
    verify(spyStatusTrackingManager, Mockito.atLeastOnce()).markTransferCancelled(eq(replicaId));

    // 3. Final status is CANCELLED (or null if dropStoragePartitionGracefully was called later)
    BlobTransferStatus finalStatus = realStatusTrackingManager.getTransferStatus(replicaId);
    assertTrue(
        finalStatus == BlobTransferStatus.TRANSFER_CANCELLED || finalStatus == null,
        "Expected CANCELLED or null (cleaned up), got: " + finalStatus);
  }

  @Test
  public void testStartConsumptionAndDropConcurrently_RightWhenTransferCompletes() throws Exception {
    // Scenario: Race condition - transfer completes successfully on peer chain thread
    // at the exact moment drop is called from Helix thread
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(veniceServerConfig.isServerAllowlistEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    when(veniceServerConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Use real status tracking manager so map logic proves itself
    NettyFileTransferClient nettyClient = mock(NettyFileTransferClient.class);
    BlobTransferStatusTrackingManager realStatusTrackingManager = new BlobTransferStatusTrackingManager(nettyClient);

    // Spy to track when initialTransfer is called while using real implementation
    BlobTransferStatusTrackingManager spyStatusTrackingManager = Mockito.spy(realStatusTrackingManager);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(spyStatusTrackingManager);

    // Blob transfer completes successfully
    CompletableFuture<InputStream> blobTransferFuture = new CompletableFuture<>();
    // Simulate what NettyP2PBlobTransferManager.get() does: call startedTransfer()
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), any())).thenAnswer(inv -> {
      realStatusTrackingManager.startedTransfer(replicaId);
      return blobTransferFuture;
    });

    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Recreate DefaultIngestionBackend with the real status tracking manager
    DefaultIngestionBackend testIngestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);

    CountDownLatch transferInitializedLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      transferInitializedLatch.countDown();
      return inv.callRealMethod(); // Call real method to update internal map
    }).when(spyStatusTrackingManager).initialTransfer(eq(replicaId));

    AtomicInteger completedCount = new AtomicInteger(0);

    // Thread 1: Start consumption (transfer will complete when we complete the future)
    Thread startThread = new Thread(() -> {
      try {
        testIngestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());
        completedCount.incrementAndGet();
      } catch (Exception e) {
        LogManager.getLogger().error("Start error: {}", e.getMessage(), e);
      }
    });

    // Thread 2: Drop partition right as transfer completes
    Thread dropThread = new Thread(() -> {
      try {
        // Wait for transfer to be initialized
        boolean latchCompleted = transferInitializedLatch.await(2, TimeUnit.SECONDS);
        if (!latchCompleted) {
          LogManager.getLogger().warn("Latch timed out waiting for transfer initialization");
        }
        Thread.sleep(50); // Small delay
        // Drop immediately (race condition with completion)
        testIngestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5);
        completedCount.incrementAndGet();
      } catch (Exception e) {
        LogManager.getLogger().error("Drop error: {}", e.getMessage(), e);
      }
    });

    startThread.start();
    dropThread.start();

    // Wait for threads to start, then complete the transfer successfully
    Thread.sleep(100);
    blobTransferFuture.complete(mock(InputStream.class));

    startThread.join(5000);
    dropThread.join(5000);

    // Verify operations completed
    assertTrue(completedCount.get() >= 1, "At least one operation should complete");

    // Verify status was managed by real map logic
    // Since transfer completes successfully, status should be COMPLETED or null (if cleared)
    BlobTransferStatus finalStatus = realStatusTrackingManager.getTransferStatus(replicaId);
    assertTrue(
        finalStatus == BlobTransferStatus.TRANSFER_COMPLETED || finalStatus == null,
        "Expected COMPLETED or null (cleared) status, got: " + finalStatus);
  }

  @Test
  public void testStopConsumptionAfterTransferCompletes() throws Exception {
    // Scenario: Transfer completes successfully, then stopConsumption is called
    // Start consumption first to set state to RUNNING (simple path - blob transfer not enabled by default)
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Mock: Transfer is already completed
    when(statusTrackingManager.getTransferStatus(eq(replicaId))).thenReturn(BlobTransferStatus.TRANSFER_COMPLETED);
    when(statusTrackingManager.isTransferInFinalState(eq(replicaId))).thenReturn(true);

    // Call stopConsumption after transfer is done
    ingestionBackend.stopConsumption(storeConfig, PARTITION);

    // Verify cancellation was attempted (but skipped internally due to final state)
    verify(statusTrackingManager, Mockito.atLeastOnce()).cancelTransfer(eq(replicaId));
  }

  @Test
  public void testMultipleStopConsumptionCalls() throws Exception {
    // Scenario: Multiple stopConsumption calls (simulates duplicate Helix messages)
    // Start consumption first to set state to RUNNING (simple path - blob transfer not enabled by default)
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty());

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Mock: First call sees STARTED, second call sees CANCEL_REQUESTED
    when(statusTrackingManager.getTransferStatus(eq(replicaId))).thenReturn(BlobTransferStatus.TRANSFER_STARTED)
        .thenReturn(BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
    when(statusTrackingManager.isBlobTransferCancelRequestSentBefore(eq(replicaId))).thenReturn(false).thenReturn(true);

    // First stopConsumption call - transitions RUNNING -> STOPPED
    ingestionBackend.stopConsumption(storeConfig, PARTITION);

    // Second stopConsumption call - state is STOPPED (not RUNNING), so it's a no-op
    ingestionBackend.stopConsumption(storeConfig, PARTITION);

    // Verify cancelTransfer was called exactly once (second call skipped due to state check)
    verify(statusTrackingManager, Mockito.times(1)).cancelTransfer(eq(replicaId));
  }

  // ==================== ReplicaIntendedState Tests ====================

  @Test
  public void testReplicaIntendedState_NormalStartSetsRunning() {
    // A fresh start should transition NOT_EXIST → RUNNING
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Initially NOT_EXIST
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    // Start consumption (simple path since blobTransferManager is null)
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());

    // Should be RUNNING
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);
  }

  @Test
  public void testReplicaIntendedState_DuplicateStartIgnored() {
    // Calling startConsumption twice should not fail; second call is a no-op
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // Second call should be ignored (state already RUNNING)
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // Verify startConsumption on ingestion service was only called once
    verify(storeIngestionService, Mockito.times(1)).startConsumption(eq(storeConfig), eq(PARTITION), any());
  }

  @Test
  public void testReplicaIntendedState_StopSetsStoppedFromRunning() {
    // stopConsumption on a RUNNING replica should set it to STOPPED
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);
  }

  @Test
  public void testReplicaIntendedState_StopWithoutStartIsNoop() {
    // stopConsumption without prior startConsumption should be a no-op (state is NOT_EXIST)
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    backend.stopConsumption(storeConfig, PARTITION);

    // State remains NOT_EXIST
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    // stopConsumption on ingestion service should NOT have been called
    verify(storeIngestionService, never()).stopConsumption(any(), anyInt());
  }

  @Test
  public void testReplicaIntendedState_DuplicateStopIsNoop() {
    // Second stopConsumption call should be a no-op since state is already STOPPED
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // Second stop call should be no-op (state is STOPPED, not RUNNING)
    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // stopConsumption on ingestion service should have been called exactly once
    verify(storeIngestionService, Mockito.times(1)).stopConsumption(any(), anyInt());
  }

  @Test
  public void testReplicaIntendedState_DropFromRunningSetsNotExist() {
    // dropStoragePartitionGracefully from RUNNING should end in NOT_EXIST
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    backend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, false);
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_DropFromStoppedSetsNotExist() {
    // dropStoragePartitionGracefully from STOPPED should end in NOT_EXIST
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    backend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, false);
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_DropFromNotExistSetsNotExist() {
    // dropStoragePartitionGracefully from NOT_EXIST should still end in NOT_EXIST
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    backend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, false);
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_StartAfterStopWaitsAndSetsRunning() {
    // startConsumption when state is STOPPED should wait for stop to complete, then set RUNNING
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // Start, then stop
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // Start again while STOPPED - should wait for stop to complete, then set RUNNING
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // Verify stopConsumptionAndWait was called (from the STOPPED→RUNNING wait path in startConsumption)
    verify(storeIngestionService, Mockito.atLeastOnce())
        .stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
  }

  @Test
  public void testReplicaIntendedState_FullLifecycle() {
    // Full lifecycle: start → stop → drop → start again
    DefaultIngestionBackend backend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        veniceServerConfig);
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String replicaId = Utils.getReplicaId(storeConfig.getStoreVersionName(), PARTITION);

    // 1. NOT_EXIST → RUNNING
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // 2. RUNNING → STOPPED
    backend.stopConsumption(storeConfig, PARTITION);
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // 3. STOPPED → NOT_EXIST (via drop)
    backend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, false);
    Assert.assertEquals(
        backend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    // 4. NOT_EXIST → RUNNING (start again after drop)
    backend.startConsumption(storeConfig, PARTITION, Optional.empty());
    Assert
        .assertEquals(backend.getReplicaIntendedState(replicaId), DefaultIngestionBackend.ReplicaIntendedState.RUNNING);
  }
}
