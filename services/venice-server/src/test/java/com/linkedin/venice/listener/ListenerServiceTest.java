package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.MetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ListenerServiceTest {
  StorageEngineRepository storageEngineRepository;
  ReadOnlyStoreRepository storeMetadataRepository;
  ReadOnlySchemaRepository schemaRepository;
  CompletableFuture<HelixCustomizedViewOfflinePushRepository> cvRepository;
  MetadataRetriever metadataRetriever;
  VeniceServerConfig serverConfig;
  MetricsRepository metricsRepository;
  Optional<SSLFactory> sslFactory;
  Optional<StaticAccessController> routerAccessController;
  Optional<DynamicAccessController> storeAccessController;
  DiskHealthCheckService diskHealthService;
  StorageEngineBackedCompressorFactory compressorFactory;
  Optional<ResourceReadUsageTracker> resourceReadUsageTracker;

  @BeforeMethod
  public void setUp() {
    storageEngineRepository = mock(StorageEngineRepository.class);
    storeMetadataRepository = mock(ReadOnlyStoreRepository.class);
    schemaRepository = mock(ReadOnlySchemaRepository.class);
    cvRepository = mock(CompletableFuture.class);
    metadataRetriever = mock(MetadataRetriever.class);
    serverConfig = mock(VeniceServerConfig.class);
    metricsRepository = new MetricsRepository();
    sslFactory = Optional.of(mock(SSLFactory.class));
    routerAccessController = Optional.of(mock(StaticAccessController.class));
    storeAccessController = Optional.of(mock(DynamicAccessController.class));
    diskHealthService = mock(DiskHealthCheckService.class);
    compressorFactory = mock(StorageEngineBackedCompressorFactory.class);
    resourceReadUsageTracker = Optional.of(mock(ResourceReadUsageTracker.class));
    doReturn(1234).when(serverConfig).getListenerPort();
    doReturn(BlockingQueueType.LINKED_BLOCKING_QUEUE).when(serverConfig).getBlockingQueueType();
    doReturn(10).when(serverConfig).getRestServiceStorageThreadNum();
    doReturn(10).when(serverConfig).getDatabaseLookupQueueCapacity();
    doReturn(10).when(serverConfig).getServerComputeThreadNum();
    doReturn(10).when(serverConfig).getComputeQueueCapacity();
    doReturn(10).when(serverConfig).getSslHandshakeThreadPoolSize();
    doReturn(10).when(serverConfig).getSslHandshakeQueueCapacity();
    doReturn(false).when(serverConfig).isRestServiceEpollEnabled();
    doReturn(10).when(serverConfig).getNettyWorkerThreadCount();
  }

  @Test
  public void testConstructor() {
    ListenerService listenerService = new ListenerService(
        storageEngineRepository,
        storeMetadataRepository,
        schemaRepository,
        cvRepository,
        metadataRetriever,
        serverConfig,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        diskHealthService,
        compressorFactory,
        resourceReadUsageTracker);
    // dummy method call
    listenerService.getName();
  }
}
