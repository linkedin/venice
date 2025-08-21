package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
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
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.ConfigCommonUtils;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


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
    doNothing().when(storeIngestionService).startConsumption(any(VeniceStoreVersionConfig.class), anyInt());
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class))).thenReturn(storeAndVersion);
    when(storageMetadataService.getStoreVersionState(STORE_VERSION)).thenReturn(storeVersionState);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any())).thenReturn(storageEngine);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any(), any()))
        .thenReturn(storageEngine);

    when(offsetRecord.getOffsetLag()).thenReturn(0L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(storageMetadataService.getLastOffset(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER), PARTITION))
        .thenReturn(offsetRecord);

    when(blobTransferManager.getAggVersionedBlobTransferStats()).thenReturn(aggVersionedBlobTransferStats);

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

    ingestionBackend.startConsumption(storeConfig, PARTITION);
    verifyBlobTransfer(true);
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));

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
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(store.getBlobTransferInServerEnabled()).thenReturn(storeSetting);
    when(veniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(serverSetting);

    ingestionBackend.startConsumption(storeConfig, PARTITION);
    verifyBlobTransfer(expectEnabled);
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

    ingestionBackend.startConsumption(storeConfig, PARTITION);
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

    ingestionBackend.startConsumption(storeConfig, PARTITION);

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
        .bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, BLOB_TRANSFER_FORMAT, 100L, storeConfig, null)
        .toCompletableFuture();
    assertTrue(future.isDone());
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
  }

  @Test
  public void testNotStartBootstrapFromBlobTransferWhenNotLaggingForHybridStore() {
    long laggingThreshold = 1000L;
    when(offsetRecord.getOffsetLag()).thenReturn(10L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubUtil.fromKafkaOffset(10L));
    when(storageMetadataService.getLastOffset(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER), PARTITION))
        .thenReturn(offsetRecord);

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true); // hybrid store
    CompletableFuture<InputStream> future = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(future);

    CompletableFuture<Void> result = ingestionBackend
        .bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, BLOB_TRANSFER_FORMAT, laggingThreshold, storeConfig, null)
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
    when(storageMetadataService.getLastOffset(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER), PARTITION))
        .thenReturn(offsetRecord);

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(storeIngestionService.isDaVinciClient()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    CompletableFuture<InputStream> future = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), eq(BLOB_TRANSFER_FORMAT)))
        .thenReturn(future);

    CompletableFuture<Void> result = ingestionBackend
        .bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, BLOB_TRANSFER_FORMAT, laggingThreshold, storeConfig, null)
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

    ingestionBackend.startConsumption(storeConfig, PARTITION);
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
}
