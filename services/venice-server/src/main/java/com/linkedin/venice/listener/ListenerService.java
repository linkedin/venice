package com.linkedin.venice.listener;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.ThreadPoolStats;
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
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Service that listens on configured port to accept incoming GET requests
 */
public class ListenerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(ListenerService.class);

  private final ServerBootstrap bootstrap;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  private final int port;
  private final VeniceServerConfig serverConfig;
  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor computeExecutor;

  private ThreadPoolExecutor sslHandshakeExecutor;

  // TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public ListenerService(
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository storeMetadataRepository,
      ReadOnlySchemaRepository schemaRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      MetadataRetriever metadataRetriever,
      VeniceServerConfig serverConfig,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      DiskHealthCheckService diskHealthService,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {

    this.serverConfig = serverConfig;
    this.port = serverConfig.getListenerPort();

    executor = createThreadPool(
        serverConfig.getRestServiceStorageThreadNum(),
        "StorageExecutionThread",
        serverConfig.getDatabaseLookupQueueCapacity());
    new ThreadPoolStats(metricsRepository, executor, "storage_execution_thread_pool");

    computeExecutor = createThreadPool(
        serverConfig.getServerComputeThreadNum(),
        "StorageComputeThread",
        serverConfig.getComputeQueueCapacity());
    new ThreadPoolStats(metricsRepository, computeExecutor, "storage_compute_thread_pool");

    if (sslFactory.isPresent() && serverConfig.getSslHandshakeThreadPoolSize() > 0) {
      this.sslHandshakeExecutor = createThreadPool(
          serverConfig.getSslHandshakeThreadPoolSize(),
          "SSLHandShakeThread",
          serverConfig.getSslHandshakeQueueCapacity());
      new ThreadPoolStats(metricsRepository, this.sslHandshakeExecutor, "ssl_handshake_thread_pool");
    }

    StorageReadRequestsHandler requestHandler = createRequestHandler(
        executor,
        computeExecutor,
        storageEngineRepository,
        storeMetadataRepository,
        schemaRepository,
        metadataRetriever,
        diskHealthService,
        serverConfig.isComputeFastAvroEnabled(),
        serverConfig.isEnableParallelBatchGet(),
        serverConfig.getParallelBatchGetChunkSize(),
        compressorFactory,
        resourceReadUsageTracker);

    HttpChannelInitializer channelInitializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactory,
        sslHandshakeExecutor,
        serverConfig,
        routerAccessController,
        storeAccessController,
        requestHandler);

    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    boolean epollEnabled = serverConfig.isRestServiceEpollEnabled();
    if (epollEnabled) {
      try {
        bossGroup = new EpollEventLoopGroup(1);
        workerGroup = new EpollEventLoopGroup(serverConfig.getNettyWorkerThreadCount()); // if 0, defaults to 2*cpu
                                                                                         // count
        serverSocketChannelClass = EpollServerSocketChannel.class;
        LOGGER.info("Epoll is enabled in Server Rest Service");
      } catch (LinkageError error) {
        LOGGER.info("Epoll is only supported on Linux; switching to NIO for Server Rest Service", error);
        epollEnabled = false;
      }
    }
    if (!epollEnabled) {
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup(serverConfig.getNettyWorkerThreadCount()); // if 0, defaults to 2*cpu count
      serverSocketChannelClass = NioServerSocketChannel.class;
    }
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(serverSocketChannelClass)
        .childHandler(channelInitializer)
        .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }

  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    LOGGER.info("Listener service started on port: {}", port);

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    /**
     * Netty shutdown gracefully is NOT working well for us since it will close all the connections right away.
     * By sleeping the configured period, Storage Node could serve all the requests, which are already received.
     *
     * Since Storage Node will stop {@link HelixParticipationService}
     * (disconnect from Zookeeper, which makes it unavailable in Venice Router) before Netty,
     * there shouldn't be a lot of requests coming during this grace period, otherwise, we need to tune the config.
     */
    Thread.sleep(TimeUnit.SECONDS.toMillis(serverConfig.getNettyGracefulShutdownPeriodSeconds()));
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }

  protected ThreadPoolExecutor createThreadPool(int threadCount, String threadNamePrefix, int capacity) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threadCount,
        threadCount,
        0,
        TimeUnit.MILLISECONDS,
        serverConfig.getExecutionQueue(capacity),
        new DaemonThreadFactory(threadNamePrefix));
    /**
     * When the capacity is fully saturated, the scheduled task will be executed in the caller thread.
     * We will leverage this policy to propagate the back pressure to the caller, so that no more tasks will be
     * scheduled.
     */
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

    return executor;
  }

  protected StorageReadRequestsHandler createRequestHandler(
      ThreadPoolExecutor executor,
      ThreadPoolExecutor computeExecutor,
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository metadataRepository,
      ReadOnlySchemaRepository schemaRepository,
      MetadataRetriever metadataRetriever,
      DiskHealthCheckService diskHealthService,
      boolean fastAvroEnabled,
      boolean parallelBatchGetEnabled,
      int parallelBatchGetChunkSize,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {
    return new StorageReadRequestsHandler(
        executor,
        computeExecutor,
        storageEngineRepository,
        metadataRepository,
        schemaRepository,
        metadataRetriever,
        diskHealthService,
        fastAvroEnabled,
        parallelBatchGetEnabled,
        parallelBatchGetChunkSize,
        serverConfig,
        compressorFactory,
        resourceReadUsageTracker);
  }
}
