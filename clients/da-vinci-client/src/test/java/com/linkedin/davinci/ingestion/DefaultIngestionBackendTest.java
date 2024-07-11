package com.linkedin.davinci.ingestion;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DefaultIngestionBackendTest {
  private StorageMetadataService storageMetadataService;
  private KafkaStoreIngestionService storeIngestionService;
  private StorageService storageService;
  private BlobTransferManager blobTransferManager;
  private VeniceConfigLoader configLoader;
  private DefaultIngestionBackend ingestionBackend;
  private VeniceStoreVersionConfig storeConfig;
  private AbstractStorageEngine storageEngine;
  private ReadOnlyStoreRepository metadataRepo;
  private VeniceServerConfig veniceServerConfig;

  @BeforeMethod
  public void setUp() {
    storageMetadataService = mock(StorageMetadataService.class);
    storeIngestionService = mock(KafkaStoreIngestionService.class);
    storageService = mock(StorageService.class);
    blobTransferManager = mock(BlobTransferManager.class);
    configLoader = mock(VeniceConfigLoader.class);
    storeConfig = mock(VeniceStoreVersionConfig.class);
    storageEngine = mock(AbstractStorageEngine.class);
    metadataRepo = mock(ReadOnlyStoreRepository.class);
    veniceServerConfig = mock(VeniceServerConfig.class);

    // Create the DefaultIngestionBackend instance with mocked dependencies
    ingestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        blobTransferManager,
        veniceServerConfig);
  }

  @Test
  public void testStartConsumption() {
    String storeVersion = "store_v1";
    int partition = 1;
    String storeName = "testStore";
    int versionNumber = 1;
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    Pair<Store, Version> storeAndVersion = Pair.create(store, version);
    StoreVersionState storeVersionState = mock(StoreVersionState.class);
    CompletableFuture<InputStream> bootstrapFuture = CompletableFuture.completedFuture(null);
    String baseDir = "mockBaseDir";

    when(storeConfig.getStoreVersionName()).thenReturn(storeVersion);
    when(storeIngestionService.getMetadataRepo()).thenReturn(metadataRepo);
    doNothing().when(storeIngestionService).startConsumption(any(VeniceStoreVersionConfig.class), anyInt());
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class))).thenReturn(storeAndVersion);
    when(storageMetadataService.getStoreVersionState(storeVersion)).thenReturn(storeVersionState);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(partition), any())).thenReturn(storageEngine);
    when(store.getName()).thenReturn(storeName);
    when(version.getNumber()).thenReturn(versionNumber);
    when(blobTransferManager.get(eq(storeName), eq(versionNumber), eq(partition))).thenReturn(bootstrapFuture);
    when(store.isBlobTransferEnabled()).thenReturn(true);
    when(store.isHybrid()).thenReturn(false);
    when(veniceServerConfig.getRocksDBPath()).thenReturn(baseDir);

    ingestionBackend.startConsumption(storeConfig, partition);

    // verify that blobTransferManager was called given it is a hybrid & blob enabled
    verify(blobTransferManager).get(eq(storeName), eq(versionNumber), eq(partition));
  }
}
