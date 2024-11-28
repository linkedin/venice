package com.linkedin.davinci.ingestion;

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
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.Pair;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
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
  private AbstractStorageEngine storageEngine;
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

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(store.getName()).thenReturn(STORE_NAME);
    when(version.getNumber()).thenReturn(VERSION_NUMBER);
    Pair<Store, Version> storeAndVersion = Pair.create(store, version);

    when(storeConfig.getStoreVersionName()).thenReturn(STORE_VERSION);
    when(storeIngestionService.getMetadataRepo()).thenReturn(metadataRepo);
    doNothing().when(storeIngestionService).startConsumption(any(VeniceStoreVersionConfig.class), anyInt());
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class))).thenReturn(storeAndVersion);
    when(storageMetadataService.getStoreVersionState(STORE_VERSION)).thenReturn(storeVersionState);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any())).thenReturn(storageEngine);

    when(offsetRecord.getOffsetLag()).thenReturn(0L);
    when(offsetRecord.getLocalVersionTopicOffset()).thenReturn(-1L);
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

  // verify that blobTransferManager was called given it is blob enabled
  @Test
  public void testStartConsumptionWithBlobTransfer() {
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(store.isHybrid()).thenReturn(true);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceServerConfig.getRocksDBPath()).thenReturn(BASE_DIR);

    ingestionBackend.startConsumption(storeConfig, PARTITION);
    verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION));
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(true));
  }

  @Test
  public void testStartConsumptionWithBlobTransferWhenNoPeerFound() {
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    CompletableFuture<InputStream> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new VenicePeersNotFoundException("No peers found"));
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION))).thenReturn(errorFuture);

    CompletableFuture<Void> future =
        ingestionBackend.bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, 100L).toCompletableFuture();
    assertTrue(future.isDone());
    verify(aggVersionedBlobTransferStats).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats)
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
  }

  @Test
  public void testNotStartBootstrapFromBlobTransferWhenNotLagging() {
    long laggingThreshold = 1000L;
    when(offsetRecord.getOffsetLag()).thenReturn(-10L);
    when(offsetRecord.getLocalVersionTopicOffset()).thenReturn(10L);
    when(storageMetadataService.getLastOffset(Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER), PARTITION))
        .thenReturn(offsetRecord);

    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    CompletableFuture<InputStream> future = new CompletableFuture<>();
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION))).thenReturn(future);

    CompletableFuture<Void> result =
        ingestionBackend.bootstrapFromBlobs(store, VERSION_NUMBER, PARTITION, laggingThreshold).toCompletableFuture();
    assertTrue(result.isDone());
    verify(blobTransferManager, never()).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION));
    verify(aggVersionedBlobTransferStats, never()).recordBlobTransferResponsesCount(eq(STORE_NAME), eq(VERSION_NUMBER));
    verify(aggVersionedBlobTransferStats, never())
        .recordBlobTransferResponsesBasedOnBoostrapStatus(eq(STORE_NAME), eq(VERSION_NUMBER), eq(false));
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
