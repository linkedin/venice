package com.linkedin.venice.router;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
import com.linkedin.ddsstorage.base.concurrency.impl.SuccessAsyncFuture;
import com.linkedin.ddsstorage.base.registry.ResourceRegistry;
import com.linkedin.ddsstorage.base.registry.ShutdownableExecutors;
import com.linkedin.ddsstorage.router.api.LongTailRetrySupplier;
import com.linkedin.ddsstorage.router.api.ScatterGatherHelper;
import com.linkedin.ddsstorage.router.impl.Router;
import com.linkedin.ddsstorage.router.impl.netty4.Router4Impl;
import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.acl.RouterAclHandler;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterHeartbeat;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceDelegateModeConfig;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VeniceMetricsProvider;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.router.httpclient.ApacheHttpAsyncStorageNodeClient;
import com.linkedin.venice.router.httpclient.NettyStorageNodeClient;
import com.linkedin.venice.router.httpclient.StorageNodeClient;

import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.HealthCheckStats;
import com.linkedin.venice.router.stats.LongTailRetryStatsProvider;
import com.linkedin.venice.router.stats.RouterCacheStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.router.stats.SecurityStats;

import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.router.throttle.NoopRouterThrottler;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class RouterServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(RouterServer.class);

  // Immutable state
  private final List<D2Server> d2ServerList;
  private final MetricsRepository metricsRepository;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Optional<DynamicAccessController> accessController;

  private final VeniceRouterConfig config;

  // Mutable state
  // TODO: Make these final once the test constructors are cleaned up.
  private ZkClient zkClient;
  private SafeHelixManager manager;
  private HelixReadOnlySchemaRepository schemaRepository;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixReadOnlyStoreRepository metadataRepository;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;

  private HelixLiveInstanceMonitor liveInstanceMonitor;

  // These are initialized in startInner()... TODO: Consider refactoring this to be immutable as well.
  private AsyncFuture<SocketAddress> serverFuture = null;
  private AsyncFuture<SocketAddress> secureServerFuture = null;
  private ResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;
  private RouterHeartbeat heartbeat;
  private VeniceDelegateMode scatterGatherMode;
  private HelixAdapterSerializer adapter;
  private ZkRoutersClusterManager routersClusterManager;
  private Router router;
  private Router secureRouter;
  private Optional<RouterCache> routerCache;

  private MultithreadEventLoopGroup workerEventLoopGroup;
  private MultithreadEventLoopGroup serverEventLoopGroup;

  private ExecutorService workerExecutor;

  private final static String ROUTER_SERVICE_NAME = "venice-router";

  /**
   * Thread number used to monitor the listening port;
   */
  private final static int ROUTER_BOSS_THREAD_NUM = 1;
  /**
   * How many threads should be used by router for directly handling requests
   */
  private final static int ROUTER_IO_THREAD_NUM = Runtime.getRuntime().availableProcessors();;
  /**
   * How big should the thread pool used by the router be.  This is the number of threads used for handling
   * requests plus the threads used by the boss thread pool per bound socket (ie 1 for SSL and 1 for non-SSL)
   */
  private final static int ROUTER_THREAD_POOL_SIZE = 2 * (ROUTER_IO_THREAD_NUM + ROUTER_BOSS_THREAD_NUM);;

  public static void main(String args[]) throws Exception {

    VeniceProperties props;
    try {
      String routerConfigFilePath = args[0];
      props = Utils.parseProperties(routerConfigFilePath);
    } catch (Exception e){
      throw new VeniceException("No config file parameter found", e);
    }


    logger.info("Zookeeper: " + props.getString(ConfigKeys.ZOOKEEPER_ADDRESS));
    logger.info("Cluster: " + props.getString(ConfigKeys.CLUSTER_NAME));
    logger.info("Port: " + props.getInt(ConfigKeys.LISTENER_PORT));
    logger.info("SSL Port: " + props.getInt(ConfigKeys.LISTENER_SSL_PORT));
    logger.info("Thread count: " + ROUTER_THREAD_POOL_SIZE);

    Optional<SSLEngineComponentFactory> sslFactory = Optional.of(SslUtils.getLocalSslFactory());
    RouterServer server = new RouterServer(props, new ArrayList<>(), Optional.empty(), sslFactory);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server.isStarted()) {
          try {
            server.stop();
          } catch (Exception e) {
            logger.error("Error shutting the server. ", e);
          }
        }
      }
    });

    while(true) {
      Thread.sleep(TimeUnit.HOURS.toMillis(1));
    }
  }

  public RouterServer(VeniceProperties properties, List<D2Server> d2Servers, Optional<DynamicAccessController> accessController,
      Optional<SSLEngineComponentFactory> sslEngineComponentFactory) {
    this(properties, d2Servers, accessController, sslEngineComponentFactory, TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME));
  }

  // for test purpose
  public MetricsRepository getMetricsRepository() {
    return this.metricsRepository;
  }

  public RouterServer(
      VeniceProperties properties,
      List<D2Server> d2ServerList,
      Optional<DynamicAccessController> accessController,
      Optional<SSLEngineComponentFactory> sslFactory,
      MetricsRepository metricsRepository) {
    this(properties, d2ServerList, accessController, sslFactory, metricsRepository, true);
    this.metadataRepository = new HelixReadOnlyStoreRepository(zkClient, adapter, config.getClusterName(),
        config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.schemaRepository =
        new HelixReadOnlySchemaRepository(this.metadataRepository, this.zkClient, adapter, config.getClusterName(),
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.routingDataRepository = new HelixRoutingDataRepository(manager);
    this.storeConfigRepository =
        new HelixReadOnlyStoreConfigRepository(zkClient, adapter, config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs());
    this.liveInstanceMonitor = new HelixLiveInstanceMonitor(this.zkClient, config.getClusterName());
  }

  /**
   * CTOR maintain the common logic for both test and normal CTOR.
   */
  private RouterServer(
      VeniceProperties properties,
      List<D2Server> d2ServerList,
      Optional<DynamicAccessController> accessController,
      Optional<SSLEngineComponentFactory> sslFactory,
      MetricsRepository metricsRepository, boolean isCreateHelixManager) {
    config = new VeniceRouterConfig(properties);
    zkClient = new ZkClient(config.getZkConnection(), ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "router-zk-client"));

    this.adapter = new HelixAdapterSerializer();
    if(isCreateHelixManager) {
      this.manager = new SafeHelixManager(new ZKHelixManager(config.getClusterName(), null, InstanceType.SPECTATOR, config.getZkConnection()));
    }

    this.metricsRepository = metricsRepository;
    this.routerStats = new RouterStats<>( requestType -> new AggRouterHttpRequestStats(metricsRepository, requestType) );

    this.d2ServerList = d2ServerList;
    this.accessController= accessController;
    this.sslFactory = sslFactory;
    verifySslOk();
  }

  /**
   * Only use this constructor for testing when you want to pass mock repositories
   *
   * Having separate constructors just for tests is hard to maintain, especially since in this case,
   * the test constructor does not initialize manager...
   */
  public RouterServer(
      VeniceProperties properties,
      HelixRoutingDataRepository routingDataRepository,
      HelixReadOnlyStoreRepository metadataRepository,
      HelixReadOnlySchemaRepository schemaRepository,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      List<D2Server> d2ServerList,
      Optional<SSLEngineComponentFactory> sslFactory,
      HelixLiveInstanceMonitor liveInstanceMonitor){
    this(properties, d2ServerList, Optional.empty(), sslFactory, new MetricsRepository(), false);
    this.routingDataRepository = routingDataRepository;
    this.metadataRepository = metadataRepository;
    this.schemaRepository = schemaRepository;
    this.storeConfigRepository = storeConfigRepository;
    this.liveInstanceMonitor = liveInstanceMonitor;
  }

  @Override
  public boolean startInner() throws Exception {
    /**
     * {@link ResourceRegistry#globalShutdown()} will be invoked automatically since {@link ResourceRegistry} registers
     * runtime shutdown hook, so we need to make sure all the clean up work will be done before the actual shutdown.
     *
     * Global shutdown delay will be longer (2 times) than the grace shutdown logic in {@link #stopInner()} since
     * we would like to execute Router owned shutdown logic first to avoid race condition.
     */
    ResourceRegistry.setGlobalShutdownDelayMillis(TimeUnit.SECONDS.toMillis(config.getRouterNettyGracefulShutdownPeriodSeconds() * 2));

    metadataRepository.refresh();
    storeConfigRepository.refresh();
    // No need to call schemaRepository.refresh() since it will do nothing.
    registry = new ResourceRegistry();
    workerExecutor = registry
        .factory(ShutdownableExecutors.class)
        .newFixedThreadPool(config.getNettyClientEventLoopThreads(), new DefaultThreadFactory("RouterThread", true, Thread.MAX_PRIORITY));
    /**
     * Use TreeMap inside TimeoutProcessor; the other option ConcurrentSkipList has performance issue.
     *
     * Refer to more context on {@link VeniceRouterConfig#checkProperties(VeniceProperties)}
     */
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry, true, 1);
    Map<String, Object> serverSocketOptions = null;

    Optional<SSLEngineComponentFactory> sslFactoryForRequests = config.isSslToStorageNodes()? sslFactory : Optional.empty();
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository);
    VeniceHostHealth healthMonitor = new VeniceHostHealth(liveInstanceMonitor);
    scatterGatherMode = new VeniceDelegateMode(new VeniceDelegateModeConfig(config));
    routerCache = Optional.empty();
    if (config.isCacheEnabled()) {
      logger.info("Router cache type: " + config.getCacheType() + ", cache eviction: " + config.getCacheEviction() +
      ", cache size: " + config.getCacheSizeBytes() + ", cache concurrency: " + config.getCacheConcurrency() +
      ", cache hash table size: " + config.getCacheHashTableSize());
      routerCache = Optional.of(new RouterCache(config.getCacheType(), config.getCacheEviction(),
              config.getCacheSizeBytes(), config.getCacheConcurrency(), config.getCacheHashTableSize(),
              config.getCacheTTLmillis(), routingDataRepository));
      // Tracking cache metrics
      new RouterCacheStats(metricsRepository, "router_cache", routerCache.get());
    }

    Class<? extends Channel> channelClass;
    Class<? extends AbstractChannel> serverSocketChannelClass;
    try {
      workerEventLoopGroup = new EpollEventLoopGroup(config.getNettyClientEventLoopThreads(), workerExecutor);
      channelClass = EpollSocketChannel.class;
      serverEventLoopGroup = new EpollEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      serverSocketChannelClass = EpollServerSocketChannel.class;
    } catch (LinkageError error) {
      logger.info("Epoll is only supported on Linux; switching to NIO", error);
      workerEventLoopGroup = new NioEventLoopGroup(config.getNettyClientEventLoopThreads(), workerExecutor);
      channelClass = NioSocketChannel.class;
      serverEventLoopGroup = new NioEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      serverSocketChannelClass = NioServerSocketChannel.class;
    }

    StorageNodeClient storageNodeClient;
    switch (config.getStorageNodeClientType()) {
      case NETTY_4_CLIENT:
        logger.info("Router will use NETTY_4_CLIENT");
        storageNodeClient = new NettyStorageNodeClient(config, sslFactoryForRequests,
            routerStats, workerEventLoopGroup, channelClass);
        break;
      case APACHE_HTTP_ASYNC_CLIENT:
        logger.info("Router will use Apache_Http_Async_Client");
        storageNodeClient = new ApacheHttpAsyncStorageNodeClient(config, sslFactoryForRequests, metricsRepository);
        break;
      default:
        throw new VeniceException("Router client type " + config.getStorageNodeClientType().toString() + " is not supported!");
    }

    dispatcher = new VeniceDispatcher(config, healthMonitor, metadataRepository, routerCache,
        routerStats, metricsRepository, storageNodeClient);

    heartbeat = new RouterHeartbeat(liveInstanceMonitor, healthMonitor, config, sslFactoryForRequests);
    heartbeat.startInner();
    MetaDataHandler metaDataHandler =
        new MetaDataHandler(routingDataRepository, schemaRepository, config.getClusterName(), storeConfigRepository,
            config.getClusterToD2Map());

    /**
     * TODO: find a way to add read compute stats in host finder;
     *
     * Host finder uses http method to distinguish single-get from multi-get and it doesn't have any other information,
     * so there is no way to distinguish compute request from multi-get; all read compute metrics in host finder will
     * be recorded as multi-get metrics; affected metric is "find_unhealthy_host_request"
     */
    OnlineInstanceFinder onlineInstanceFinder =
        new OnlineInstanceFinderDelegator(metadataRepository, routingDataRepository, new PartitionStatusOnlineInstanceFinder(
        new HelixOfflinePushMonitorAccessor(config.getClusterName(), zkClient, adapter), routingDataRepository));

    VeniceHostFinder hostFinder = new VeniceHostFinder(onlineInstanceFinder,
        config.isStickyRoutingEnabledForSingleGet(),
        config.isStickyRoutingEnabledForMultiGet(),
        routerStats,
        healthMonitor);

    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        metadataRepository, onlineInstanceFinder,
        new StaleVersionStats(metricsRepository, "stale_version"));
    VenicePathParser pathParser = new VenicePathParser(versionFinder, partitionFinder,
        routerStats, metadataRepository, config);

    // Setup stat tracking for exceptional case
    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    // Fixed retry future
    AsyncFuture<LongSupplier> singleGetRetryFuture = new SuccessAsyncFuture<>(() -> config.getLongTailRetryForSingleGetThresholdMs());
    LongTailRetrySupplier retrySupplier = new LongTailRetrySupplier<VenicePath, RouterKey>() {
      private TreeMap<Integer, Integer> longTailRetryConfigForBatchGet = config.getLongTailRetryForBatchGetThresholdMs();

      @Nonnull
      @Override
      public AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(@Nonnull VenicePath path,
          @Nonnull String methodName) {
        if (VeniceRouterUtils.isHttpGet(methodName)) {
          // single-get
          path.setLongTailRetryThresholdMs(config.getLongTailRetryForSingleGetThresholdMs());
          return singleGetRetryFuture;
        } else {
          /**
           * Long tail retry threshold is based on key count for batch-get request.
           */
          int keyNum = path.getPartitionKeys().size();
          if (0 == keyNum) {
            // Should not happen
            throw new VeniceException("Met scatter-gather request without any keys");
          }
          /**
           * Refer to {@link ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS} to get more info.
           */
          int longTailRetryThresholdMs = longTailRetryConfigForBatchGet.floorEntry(keyNum).getValue();
          path.setLongTailRetryThresholdMs(longTailRetryThresholdMs);
          return new SuccessAsyncFuture<>(() -> longTailRetryThresholdMs);
        }
      }
    };
    // Log to indicate whether Streaming is enabled in Router or not
    if (config.isStreamingEnabled()) {
      logger.info("Streaming is enabled in Router");
    } else {
      logger.info("Streaming is disabled in Router");
    }

    /**
     * No need to setup {@link com.linkedin.ddsstorage.router.api.HostHealthMonitor} here since
     * {@link VeniceHostFinder} will always do health check.
     */
    ScatterGatherHelper scatterGather = ScatterGatherHelper.builder()
        .roleFinder(new VeniceRoleFinder())
        .pathParserExtended(pathParser)
        .partitionFinder(partitionFinder)
        .hostFinder(hostFinder)
        .dispatchHandler(dispatcher)
        .scatterMode(scatterGatherMode)
        .responseAggregatorFactory(
            new VeniceResponseAggregator(routerStats)
            .withSingleGetTardyThreshold(config.getSingleGetTardyLatencyThresholdMs(), TimeUnit.MILLISECONDS)
            .withMultiGetTardyThreshold(config.getMultiGetTardyLatencyThresholdMs(), TimeUnit.MILLISECONDS)
            .withComputeTardyThreshold(config.getComputeTardyLatencyThresholdMs(), TimeUnit.MILLISECONDS)
        )
        .metricsProvider(new VeniceMetricsProvider())
        .longTailRetrySupplier(retrySupplier)
        .scatterGatherStatsProvider(new LongTailRetryStatsProvider(routerStats))
        .enableStackTraceResponseForException(true)
        .enableRetryRequestAlwaysUseADifferentHost(true)
        .build();

    SecurityStats securityStats = new SecurityStats(this.metricsRepository, "security");
    VerifySslHandler unsecureVerifySslHandler = new VerifySslHandler(securityStats, config.isEnforcingSecureOnly());
    HealthCheckStats healthCheckStats = new HealthCheckStats(this.metricsRepository, "healthcheck_stats");
    router = Router.builder(scatterGather)
        .name("VeniceRouterHttp")
        .resourceRegistry(registry)
        .serverSocketChannel(serverSocketChannelClass)
        .bossPoolBuilder(EventLoopGroup.class, ignored -> serverEventLoopGroup)
        .ioWorkerPoolBuilder(EventLoopGroup.class, ignored -> workerEventLoopGroup)
        .connectionLimit(config.getConnectionLimit())
        .timeoutProcessor(timeoutProcessor)
        .serverSocketOptions(serverSocketOptions)
        .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
          pipeline.addLast("HealthCheckHandler", new HealthCheckHandler(healthCheckStats));
          pipeline.addLast("VerifySslHandler", unsecureVerifySslHandler);
          pipeline.addLast("MetadataHandler", metaDataHandler);
          addStreamingHandler(pipeline);
        })
        .idleTimeout(3, TimeUnit.HOURS)
        .build();

    VerifySslHandler verifySslHandler = new VerifySslHandler(securityStats);
    RouterAclHandler aclHandler = accessController.isPresent() ? new RouterAclHandler(accessController.get(), metadataRepository) : null;
    SSLInitializer sslInitializer = sslFactory.isPresent() ? new SSLInitializer(sslFactory.get()) : null;
    Consumer<ChannelPipeline> noop = pipeline -> {};
    Consumer<ChannelPipeline> addSslInitializer = pipeline -> {pipeline.addFirst("SSL Initializer", sslInitializer);};
    HealthCheckHandler secureRouterHealthCheckHander = new HealthCheckHandler(healthCheckStats);
    Consumer<ChannelPipeline> withoutAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", verifySslHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      addStreamingHandler(pipeline);
    };
    Consumer<ChannelPipeline> withAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", verifySslHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      pipeline.addLast("RouterAclHandler", aclHandler);
      addStreamingHandler(pipeline);
    };

    secureRouter = Router.builder(scatterGather)
        .name("SecureVeniceRouterHttps")
        .resourceRegistry(registry)
        .serverSocketChannel(serverSocketChannelClass)
        .bossPoolBuilder(EventLoopGroup.class, ignored -> serverEventLoopGroup)
        .ioWorkerPoolBuilder(EventLoopGroup.class, ignored -> workerEventLoopGroup)
        .connectionLimit(config.getConnectionLimit())
        .timeoutProcessor(timeoutProcessor)
        .serverSocketOptions(serverSocketOptions)
        .beforeHttpServerCodec(ChannelPipeline.class, sslFactory.isPresent() ? addSslInitializer : noop)  // Compare once per router. Previously compared once per request.
        .beforeHttpRequestHandler(ChannelPipeline.class, accessController.isPresent() ? withAcl : withoutAcl) // Compare once per router. Previously compared once per request.
        .idleTimeout(3, TimeUnit.HOURS)
        .build();

    asyncStart();

    // The start up process is not finished yet, because it is continuing asynchronously.
    return false;
  }

  private void addStreamingHandler(ChannelPipeline pipeline) {
    if (config.isStreamingEnabled()) {
      pipeline.addLast("VeniceChunkedWriteHandler", new VeniceChunkedWriteHandler());
    }
  }

  @Override
  public void stopInner() throws Exception {
    for(D2Server d2Server : d2ServerList)
    {
      logger.info("Stopping d2 announcer: " + d2Server);
      try {
        d2Server.notifyShutdown();
      } catch (RuntimeException e){
        logger.error("D2 announcer " + d2Server + " failed to shutdown properly", e);
      }
    }
    // Graceful shutdown
    Thread.sleep(TimeUnit.SECONDS.toMillis(config.getRouterNettyGracefulShutdownPeriodSeconds()));
    if (!serverFuture.cancel(false)){
      serverFuture.awaitUninterruptibly();
    }
    if (!secureServerFuture.cancel(false)){
      secureServerFuture.awaitUninterruptibly();
    }
    /**
     * The following change is trying to solve the router stop stuck issue.
     * From the logs, it seems {@link ResourceRegistry.State#performShutdown()} is not shutting down
     * resources synchronously when necessary, which could cause some race condition.
     * For example, "executor" initialized in {@link #startInner()} could be shutdown earlier than
     * {@link #router} and {@link #secureRouter}, then the shutdown of {@link #router} and {@link #secureRouter}
     * would be blocked because of the following exception:
     * 2018/08/21 17:55:40.855 ERROR [rejectedExecution] [shutdown-com.linkedin.ddsstorage.router.impl.netty4.Router4Impl$$Lambda$230/594142688@5a8fd55c]
     * [venice-router-war] [] Failed to submit a listener notification task. Event loop shut down?
     * java.util.concurrent.RejectedExecutionException: event executor terminated
     *
     * The following shutdown logic is trying to shutdown resources in order.
     *
     * {@link Router4Impl#shutdown()} will trigger the shutdown thread, so here will trigger shutdown thread of {@link #router}
     * and {@link #secureRouter} together, and wait for them to complete after.
     *
     * TODO: figure out the root cause why {@link ResourceRegistry.State#performShutdown()} is not executing shutdown logic
     * correctly.
     */

    dispatcher.close();
    workerEventLoopGroup.shutdownGracefully();
    serverEventLoopGroup.shutdownGracefully();

    router.shutdown();
    secureRouter.shutdown();
    router.waitForShutdown();
    logger.info("Non-secure router has been shutdown completely");
    secureRouter.waitForShutdown();
    logger.info("Secure router has been shutdown completely");
    registry.shutdown();
    registry.waitForShutdown();
    logger.info("Other resources managed by local ResourceRegistry have been shutdown completely");

    routersClusterManager.unregisterRouter(Utils.getHelixNodeIdentifier(config.getPort()));
    routersClusterManager.clear();
    routingDataRepository.clear();
    metadataRepository.clear();
    storeConfigRepository.clear();
    liveInstanceMonitor.clear();
    if (manager != null) {
      manager.disconnect();
    }
    if (zkClient != null) {
      zkClient.close();
    }
    heartbeat.stopInner();
    if (routerCache.isPresent()) {
      routerCache.get().close();
    }

    logger.info(this.toString() + " is stopped");
  }

  public HelixRoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  public HelixReadOnlyStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  /**
   * a few tasks will be done asynchronously during the service startup and are moved into this method.
   * We are doing this because there is no way to specify Venice component startup order in "mint deploy".
   * This method prevents "mint deploy" failure (When server or router starts earlier than controller,
   * helix manager throw unknown cluster name exception.) We terminate the process if helix connection
   * cannot be established.
   */
  private void asyncStart() {
    CompletableFuture.runAsync(() -> {
      try {
        if (null == this.manager) {
          // TODO: Remove this check once test constructor is removed or otherwise fixed.
          logger.info("Not connecting to Helix because the HelixManager is null (the test constructor was used)");
        } else {
          HelixUtils.connectHelixManager(manager, 30, 1);
          logger.info(this.toString() + " finished connectHelixManager()");
        }
      } catch (VeniceException ve) {
        logger.error(this.toString() + " got an exception while trying to connectHelixManager()", ve);
        logger.error(this.toString() + " is about to exit");

        System.exit(1); // TODO: Clean up all System.exit() calls... this is not proper.
      }
      // Should refresh after Helix cluster is setup
      liveInstanceMonitor.refresh();

      // Register current router into ZK.
      routersClusterManager = new ZkRoutersClusterManager(zkClient, adapter, config.getClusterName(),
          config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
      routersClusterManager.refresh();
      routersClusterManager.registerRouter(Utils.getHelixNodeIdentifier(config.getPort()));
      routingDataRepository.refresh();


      // Setup read requests throttler.
      RouterThrottler throttler;
      if (config.isReadThrottlingEnabled()) {
        throttler = new ReadRequestThrottler(routersClusterManager, metadataRepository, routingDataRepository,
            config.getMaxReadCapacityCu(), routerStats.getStatsByType(RequestType.SINGLE_GET), config.getPerStorageNodeReadQuotaBuffer());
      } else {
        throttler = new NoopRouterThrottler();
      }

      scatterGatherMode.initReadRequestThrottler(throttler);
      dispatcher.initReadRequestThrottler(throttler);


      /**
       * When the listen port is open, we would like to have current Router to be fully ready.
       */
      serverFuture = router.start(new InetSocketAddress(config.getPort()));
      secureServerFuture = secureRouter.start(new InetSocketAddress(config.getSslPort()));
      try {
        serverFuture.await();
        secureServerFuture.await();
      } catch (InterruptedException e) {
        logger.error("Received InterruptedException, will exit", e);
        System.exit(1);
      }

      for (D2Server d2Server : d2ServerList) {
        logger.info("Starting d2 announcer: " + d2Server);
        d2Server.forceStart();
      }

      try {
        int port = ((InetSocketAddress) serverFuture.get()).getPort();
        int sslPort = ((InetSocketAddress) secureServerFuture.get()).getPort();
        logger.info(this.toString() + " started on port: " + port
            + " and ssl port: " + sslPort);
      } catch (Exception e) {
        logger.error("Exception while waiting for " + this.toString() + " to start", e);
        serviceState.set(ServiceState.STOPPED);
        throw new VeniceException(e);
      }

      serviceState.set(ServiceState.STARTED);
    });
  }

  private void verifySslOk(){
    if (config.isSslToStorageNodes() && !sslFactory.isPresent()){
      throw new VeniceException("Must specify an SSLEngineComponentFactory in order to use SSL in requests to storage nodes");
    }
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }

  public VeniceRouterConfig getConfig() {
    return config;
  }
}
