package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.IngestionMetadataRetriever;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ListenerServiceTest {
  StorageEngineRepository storageEngineRepository;
  ReadOnlyStoreRepository storeMetadataRepository;
  ReadOnlySchemaRepository schemaRepository;
  CompletableFuture<HelixCustomizedViewOfflinePushRepository> cvRepository;
  IngestionMetadataRetriever ingestionMetadataRetriever;
  ReadMetadataRetriever readMetadataRetriever;
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
    ingestionMetadataRetriever = mock(IngestionMetadataRetriever.class);
    readMetadataRetriever = mock(ReadMetadataRetriever.class);
    serverConfig = mock(VeniceServerConfig.class);
    metricsRepository = new MetricsRepository();
    SSLFactory mockSslFactory = mock(SSLFactory.class);
    doReturn(new SSLConfig()).when(mockSslFactory).getSSLConfig();
    sslFactory = Optional.of(mockSslFactory);
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
    doReturn(DefaultIdentityParser.class.getName()).when(serverConfig).getIdentityParserClassName();
    doReturn(1024 * 1024).when(serverConfig).getChannelOptionWriteBufferHighBytes();
  }

  @Test
  public void testConstructor() {
    ListenerService listenerService = createListener(new ArrayList<>());
    // dummy method call
    listenerService.getName();
  }

  @AfterMethod
  public void tearDown() {
    metricsRepository.close();
  }

  @DataProvider
  public Object[][] startupOutcomes() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "startupOutcomes")
  public void testSamplerLifecycleAndStartupRollback(boolean grpcFails) throws Exception {
    metricsRepository.close();
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(
                ModuleMetricEntityInterface
                    .getUniqueMetricEntities(Collections.singletonList(ThreadPoolOtelMetricEntity.class)))
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    doReturn(0).when(serverConfig).getListenerPort();
    doReturn(grpcFails).when(serverConfig).isGrpcEnabled();
    doReturn(1).when(serverConfig).getGrpcWorkerThreadCount();
    sslFactory = Optional.empty();
    List<ThreadPoolExecutor> pools = new ArrayList<>();
    try (ServerSocket occupiedGrpcPort = new ServerSocket(0)) {
      doReturn(occupiedGrpcPort.getLocalPort()).when(serverConfig).getGrpcPort();
      ListenerService listener = createListener(pools);
      EventLoopGroup boss = listener.getBootstrap().config().group();
      EventLoopGroup workers = listener.getBootstrap().config().childGroup();
      try {
        if (grpcFails) {
          assertThrows(VeniceException.class, listener::startInner);
          assertTrue(listener.getServerFuture().isSuccess(), "HTTP must bind before gRPC startup fails");
          for (ThreadPoolExecutor pool: pools) {
            assertTrue(pool.isShutdown(), "Startup failure must release all listener executors");
          }
        } else {
          assertTrue(listener.startInner());
          TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> assertTrue(sampleCount(reader) > 0));
          ScheduledFuture<?> samplingTask = listener.getThreadPoolSampler().getSamplingTask();
          listener.stopInner();
          assertTrue(samplingTask.isCancelled(), "Listener shutdown must cancel sampling");
        }
        assertTrue(boss.terminationFuture().await(10, TimeUnit.SECONDS));
        assertTrue(workers.terminationFuture().await(10, TimeUnit.SECONDS));
        assertFalse(listener.getServerFuture().channel().isOpen());
        long stoppedCount = sampleCount(reader);
        listener.getThreadPoolSampler().sample();
        assertEquals(sampleCount(reader), stoppedCount, "Closed sampler must not record");
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        listener.getThreadPoolSampler().start(scheduler);
        verifyNoInteractions(scheduler);
      } finally {
        listener.getThreadPoolSampler().close();
        if (listener.getServerFuture() != null) {
          listener.getServerFuture().channel().close();
        }
        workers.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        boss.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
      }
    } finally {
      pools.forEach(ThreadPoolExecutor::shutdownNow);
    }
  }

  private long sampleCount(InMemoryMetricReader reader) {
    return reader.collectAllMetrics()
        .stream()
        .filter(metric -> metric.getName().equals("venice.server.thread_pool.thread.active_time_distribution"))
        .flatMap(metric -> metric.getHistogramData().getPoints().stream())
        .mapToLong(point -> point.getCount())
        .sum();
  }

  private ListenerService createListener(List<ThreadPoolExecutor> pools) {
    return new ListenerService(
        storageEngineRepository,
        storeMetadataRepository,
        schemaRepository,
        cvRepository,
        ingestionMetadataRetriever,
        readMetadataRetriever,
        serverConfig,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        diskHealthService,
        compressorFactory,
        resourceReadUsageTracker,
        Optional.empty()) {
      @Override
      protected ThreadPoolExecutor createThreadPool(int count, String name, int capacity) {
        ThreadPoolExecutor pool = super.createThreadPool(count, name, capacity);
        pools.add(pool);
        return pool;
      }

      @Override
      protected ThreadPoolExecutor createThreadPool(int count, String name, int capacity, int priority) {
        ThreadPoolExecutor pool = super.createThreadPool(count, name, capacity, priority);
        pools.add(pool);
        return pool;
      }
    };
  }
}
