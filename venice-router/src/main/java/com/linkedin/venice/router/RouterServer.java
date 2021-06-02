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
import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixOfflinePushRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.DictionaryRetrievalService;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterHeartbeat;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VeniceMetricsProvider;
import com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelector;
import com.linkedin.venice.router.httpclient.ApacheHttpAsyncStorageNodeClient;
import com.linkedin.venice.router.httpclient.NettyStorageNodeClient;
import com.linkedin.venice.router.httpclient.R2StorageNodeClient;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.httpclient.VeniceR2ClientFactory;
import com.linkedin.venice.router.stats.AdminOperationsStats;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.HealthCheckStats;
import com.linkedin.venice.router.stats.LongTailRetryStatsProvider;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.stats.RouterThrottleStats;
import com.linkedin.venice.router.stats.SecurityStats;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.router.throttle.NoopRouterThrottler;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceJVMStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.throttle.EventThrottler;
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
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.*;


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
  private ReadOnlySchemaRepository schemaRepository;
  private HelixBaseRoutingRepository routingDataRepository;
  private Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository;
  private ReadOnlyStoreRepository metadataRepository;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;
  private HelixLiveInstanceMonitor liveInstanceMonitor;
  private PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder;
  private HelixInstanceConfigRepository instanceConfigRepository;
  private OnlineInstanceFinderDelegator onlineInstanceFinder;
  private HelixGroupSelector helixGroupSelector;
  private VeniceResponseAggregator responseAggregator;
  private TimeoutProcessor timeoutProcessor;

  // These are initialized in startInner()... TODO: Consider refactoring this to be immutable as well.
  private AsyncFuture<SocketAddress> serverFuture = null;
  private AsyncFuture<SocketAddress> secureServerFuture = null;
  private ResourceRegistry registry = null;
  private StorageNodeClient storageNodeClient;
  private VeniceDispatcher dispatcher;
  private RouterHeartbeat heartbeat = null;
  private VeniceDelegateMode scatterGatherMode;
  private HelixAdapterSerializer adapter;
  private ZkRoutersClusterManager routersClusterManager;
  private Optional<Router> router = Optional.empty();
  private Router secureRouter;
  private DictionaryRetrievalService dictionaryRetrievalService;
  private RouterThrottler readRequestThrottler;
  private RouterThrottler noopRequestThrottler;

  private MultithreadEventLoopGroup workerEventLoopGroup;
  private MultithreadEventLoopGroup serverEventLoopGroup;
  private MultithreadEventLoopGroup sslResolverEventLoopGroup;

  private ExecutorService workerExecutor;
  private EventThrottler routerEarlyThrottler;

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
  private VeniceJVMStats jvmStats;

  private final AggHostHealthStats aggHostHealthStats;

  private final VeniceR2ClientFactory r2ClientFactory;

  public static void main(String args[]) throws Exception {
    VeniceProperties props;
    try {
      String routerConfigFilePath = args[0];
      props = Utils.parseProperties(routerConfigFilePath);
    } catch (Exception e) {
      throw new VeniceException("No config file parameter found", e);
    }

    logger.info("Zookeeper: " + props.getString(ConfigKeys.ZOOKEEPER_ADDRESS));
    logger.info("Cluster: " + props.getString(ConfigKeys.CLUSTER_NAME));
    logger.info("Port: " + props.getInt(ConfigKeys.LISTENER_PORT));
    logger.info("SSL Port: " + props.getInt(ConfigKeys.LISTENER_SSL_PORT));
    logger.info("Thread count: " + ROUTER_THREAD_POOL_SIZE);

    Optional<SSLEngineComponentFactory> sslFactory = Optional.of(SslUtils.getLocalSslFactory());
    RouterServer server = new RouterServer(props, new ArrayList<>(), Optional.empty(), sslFactory, null);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server.isRunning()) {
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
      Optional<SSLEngineComponentFactory> sslEngineComponentFactory, VeniceR2ClientFactory r2ClientFactory) {
    this(properties, d2Servers, accessController, sslEngineComponentFactory, TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME), r2ClientFactory);
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
      MetricsRepository metricsRepository, VeniceR2ClientFactory r2ClientFactory) {
    this(properties, d2ServerList, accessController, sslFactory, metricsRepository, true, r2ClientFactory);

    HelixReadOnlyZKSharedSystemStoreRepository readOnlyZKSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, config.getSystemSchemaClusterName());
    HelixReadOnlyStoreRepository readOnlyStoreRepository = new HelixReadOnlyStoreRepository(zkClient, adapter, config.getClusterName(),
        config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
    this.metadataRepository = new HelixReadOnlyStoreRepositoryAdapter(
        readOnlyZKSharedSystemStoreRepository,
        readOnlyStoreRepository
    );
    this.schemaRepository = new HelixReadOnlySchemaRepositoryAdapter(
        new HelixReadOnlyZKSharedSchemaRepository(readOnlyZKSharedSystemStoreRepository, zkClient, adapter, config.getSystemSchemaClusterName(),
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs()),
        new HelixReadOnlySchemaRepository(readOnlyStoreRepository, zkClient, adapter, config.getClusterName(),
            config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs())
    );
    this.routingDataRepository =
        config.isHelixOfflinePushEnabled() ? new HelixOfflinePushRepository(manager)
            : new HelixExternalViewRepository(manager);
    this.hybridStoreQuotaRepository =
        config.isHelixHybridStoreQuotaEnabled()? Optional.of(new HelixHybridStoreQuotaRepository(manager)) : Optional.empty();
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
      MetricsRepository metricsRepository, boolean isCreateHelixManager, VeniceR2ClientFactory r2ClientFactory) {
    config = new VeniceRouterConfig(properties);
    zkClient = new ZkClient(config.getZkConnection(), ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "router-zk-client"));

    this.adapter = new HelixAdapterSerializer();
    if(isCreateHelixManager) {
      this.manager = new SafeHelixManager(new ZKHelixManager(config.getClusterName(), null, InstanceType.SPECTATOR, config.getZkConnection()));
    }

    this.metricsRepository = metricsRepository;
    this.routerStats =
        new RouterStats<>( requestType -> new AggRouterHttpRequestStats(metricsRepository, requestType,
            config.isKeyValueProfilingEnabled()));

    this.aggHostHealthStats = new AggHostHealthStats(metricsRepository);

    this.d2ServerList = d2ServerList;
    this.accessController = accessController;
    this.sslFactory = sslFactory;
    this.r2ClientFactory = r2ClientFactory;
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
      HelixExternalViewRepository routingDataRepository,
      Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository,
      HelixReadOnlyStoreRepository metadataRepository,
      HelixReadOnlySchemaRepository schemaRepository,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      List<D2Server> d2ServerList,
      Optional<SSLEngineComponentFactory> sslFactory,
      HelixLiveInstanceMonitor liveInstanceMonitor,
      VeniceR2ClientFactory r2ClientFactory){
    this(properties, d2ServerList, Optional.empty(), sslFactory, new MetricsRepository(), false,  r2ClientFactory);
    this.routingDataRepository = routingDataRepository;
    this.hybridStoreQuotaRepository = hybridStoreQuotaRepository;
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

    jvmStats = new VeniceJVMStats(metricsRepository, "VeniceJVMStats");

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
    timeoutProcessor = new TimeoutProcessor(registry, true, 1);
    Map<String, Object> serverSocketOptions = null;

    Optional<SSLEngineComponentFactory> sslFactoryForRequests = config.isSslToStorageNodes()? sslFactory : Optional.empty();
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository, metadataRepository);
    Class<? extends Channel> channelClass;
    Class<? extends AbstractChannel> serverSocketChannelClass;
    boolean useEpoll = true;
    try {
      workerEventLoopGroup = new EpollEventLoopGroup(config.getNettyClientEventLoopThreads(), workerExecutor);
      channelClass = EpollSocketChannel.class;
      serverEventLoopGroup = new EpollEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      serverSocketChannelClass = EpollServerSocketChannel.class;
    } catch (LinkageError error) {
      useEpoll = false;
      logger.info("Epoll is only supported on Linux; switching to NIO", error);
      workerEventLoopGroup = new NioEventLoopGroup(config.getNettyClientEventLoopThreads(), workerExecutor);
      channelClass = NioSocketChannel.class;
      serverEventLoopGroup = new NioEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      serverSocketChannelClass = NioServerSocketChannel.class;
    }

    switch (config.getStorageNodeClientType()) {
      case NETTY_4_CLIENT:
        logger.info("Router will use NETTY_4_CLIENT");
        storageNodeClient = new NettyStorageNodeClient(config, sslFactoryForRequests,
            routerStats, workerEventLoopGroup, channelClass);
        break;
      case APACHE_HTTP_ASYNC_CLIENT:
        logger.info("Router will use Apache_Http_Async_Client");
        storageNodeClient = new ApacheHttpAsyncStorageNodeClient(config, sslFactoryForRequests, metricsRepository, liveInstanceMonitor);
        break;
      case R2_CLIENT:
        logger.info("Router will use R2 client in per node client mode");
        storageNodeClient = new R2StorageNodeClient(r2ClientFactory, sslFactoryForRequests);
        break;
      default:
        throw new VeniceException("Router client type " + config.getStorageNodeClientType().toString() + " is not supported!");
    }

    RouteHttpRequestStats routeHttpRequestStats = new RouteHttpRequestStats(metricsRepository,storageNodeClient);

    VeniceHostHealth healthMonitor = new VeniceHostHealth(liveInstanceMonitor, storageNodeClient, config,
        routeHttpRequestStats, aggHostHealthStats);
    dispatcher = new VeniceDispatcher(config, metadataRepository,
        routerStats, metricsRepository, storageNodeClient, routeHttpRequestStats, aggHostHealthStats, routerStats);
    scatterGatherMode = new VeniceDelegateMode(config, routerStats, routeHttpRequestStats);

    if (config.isRouterHeartBeatEnabled()) {
      heartbeat = new RouterHeartbeat(liveInstanceMonitor, healthMonitor, config, sslFactoryForRequests, storageNodeClient);
      heartbeat.startInner();
    }

    /**
     * TODO: find a way to add read compute stats in host finder;
     *
     * Host finder uses http method to distinguish single-get from multi-get and it doesn't have any other information,
     * so there is no way to distinguish compute request from multi-get; all read compute metrics in host finder will
     * be recorded as multi-get metrics; affected metric is "find_unhealthy_host_request"
     */
    if (!config.isHelixOfflinePushEnabled()) {
      partitionStatusOnlineInstanceFinder = new PartitionStatusOnlineInstanceFinder(
              metadataRepository,
              new VeniceOfflinePushMonitorAccessor(config.getClusterName(), zkClient, adapter),
              routingDataRepository);
    }

    onlineInstanceFinder =
        new OnlineInstanceFinderDelegator(metadataRepository, routingDataRepository,
            partitionStatusOnlineInstanceFinder, config.isHelixOfflinePushEnabled());

    CompressorFactory compressorFactory = new CompressorFactory();

    dictionaryRetrievalService = new DictionaryRetrievalService(onlineInstanceFinder, config, sslFactoryForRequests,
        metadataRepository, storageNodeClient, compressorFactory);

    MetaDataHandler metaDataHandler =
        new MetaDataHandler(routingDataRepository, schemaRepository, storeConfigRepository,
            config.getClusterToD2Map(), onlineInstanceFinder, metadataRepository,
            hybridStoreQuotaRepository, config.getClusterName(), config.getZkConnection(),
            config.getKafkaZkAddress(), config.getKafkaBootstrapServers());

    VeniceHostFinder hostFinder = new VeniceHostFinder(onlineInstanceFinder,
        config.isStickyRoutingEnabledForSingleGet(),
        config.getMultiKeyRoutingStrategy().equals(KEY_BASED_STICKY_ROUTING),
        routerStats,
        healthMonitor);

    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        metadataRepository,
        onlineInstanceFinder,
        new StaleVersionStats(metricsRepository, "stale_version"),
        storeConfigRepository,
        config.getClusterToD2Map(),
        config.getClusterName(),
        compressorFactory);
    VenicePathParser pathParser = new VenicePathParser(versionFinder, partitionFinder,
        routerStats, metadataRepository, config, compressorFactory);

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

    responseAggregator = new VeniceResponseAggregator(routerStats);
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
            responseAggregator
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

    SecurityStats securityStats = new SecurityStats(this.metricsRepository, "security",
        () -> secureRouter != null ? secureRouter.getConnectedCount() : 0);
    RouterThrottleStats routerThrottleStats = new RouterThrottleStats(this.metricsRepository, "router_throttler_stats");
    routerEarlyThrottler = new EventThrottler(config.getMaxRouterReadCapacityCu(), config.getRouterQuotaCheckWindow(), "router-early-throttler", true, EventThrottler.REJECT_STRATEGY);

    VerifySslHandler unsecureVerifySslHandler = new VerifySslHandler(securityStats, config.isEnforcingSecureOnly());
    HealthCheckStats healthCheckStats = new HealthCheckStats(this.metricsRepository, "healthcheck_stats");
    AdminOperationsStats adminOperationsStats = new AdminOperationsStats(this.metricsRepository, "admin_stats", config);
    AdminOperationsHandler adminOperationsHandler = new AdminOperationsHandler(accessController.orElse(null), this, adminOperationsStats);

    if (!config.isEnforcingSecureOnly()) {
      router = Optional.of(Router.builder(scatterGather)
          .name("VeniceRouterHttp")
          .resourceRegistry(registry)
          .serverSocketChannel(serverSocketChannelClass)
          .bossPoolBuilder(EventLoopGroup.class, ignored -> serverEventLoopGroup)
          .ioWorkerPoolBuilder(EventLoopGroup.class, ignored -> workerEventLoopGroup)
          .connectionLimit(config.getConnectionLimit())
          .timeoutProcessor(timeoutProcessor)
          .serverSocketOptions(serverSocketOptions)
          .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
            pipeline.addLast("RouterThrottleHandler", new RouterThrottleHandler(routerThrottleStats, routerEarlyThrottler, config));
            pipeline.addLast("HealthCheckHandler", new HealthCheckHandler(healthCheckStats));
            pipeline.addLast("VerifySslHandler", unsecureVerifySslHandler);
            pipeline.addLast("MetadataHandler", metaDataHandler);
            pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
            addStreamingHandler(pipeline);
          })
          .idleTimeout(3, TimeUnit.HOURS)
          .build());
    }

    VerifySslHandler verifySslHandler = new VerifySslHandler(securityStats);
    StoreAclHandler aclHandler = accessController.isPresent() ? new StoreAclHandler(accessController.get(), metadataRepository) : null;
    final SSLInitializer sslInitializer;
    if (sslFactory.isPresent()) {
      sslInitializer = new SSLInitializer(sslFactory.get());
      if (config.isThrottleClientSslHandshakesEnabled()) {
        ExecutorService sslHandshakeExecutor = registry
            .factory(ShutdownableExecutors.class)
            .newFixedThreadPool(config.getClientSslHandshakeThreads(), new DefaultThreadFactory("RouterSSLHandshakeThread", true, Thread.NORM_PRIORITY));
        int clientSslHandshakeThreads = config.getClientSslHandshakeThreads();
        int maxConcurrentClientSslHandshakes = config.getMaxConcurrentClientSslHandshakes();
        int clientSslHandshakeAttempts = config.getClientSslHandshakeAttempts();
        long clientSslHandshakeBackoffMs = config.getClientSslHandshakeBackoffMs();
        if (useEpoll) {
          sslResolverEventLoopGroup = new EpollEventLoopGroup(clientSslHandshakeThreads, sslHandshakeExecutor);
        } else {
          sslResolverEventLoopGroup = new NioEventLoopGroup(clientSslHandshakeThreads, sslHandshakeExecutor);
        }
        sslInitializer.enableResolveBeforeSSL(sslResolverEventLoopGroup, clientSslHandshakeAttempts, clientSslHandshakeBackoffMs, maxConcurrentClientSslHandshakes);
      }
    } else {
      sslInitializer = null;
    }

    Consumer<ChannelPipeline> noop = pipeline -> {};
    Consumer<ChannelPipeline> addSslInitializer = pipeline -> {pipeline.addFirst("SSL Initializer", sslInitializer);};
    HealthCheckHandler secureRouterHealthCheckHander = new HealthCheckHandler(healthCheckStats);
    RouterThrottleHandler routerThrottleHandler = new RouterThrottleHandler(routerThrottleStats, routerEarlyThrottler, config);
    Consumer<ChannelPipeline> withoutAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", verifySslHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
      pipeline.addLast("RouterThrottleHandler", routerThrottleHandler);
      addStreamingHandler(pipeline);
    };
    Consumer<ChannelPipeline> withAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", verifySslHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
      pipeline.addLast("StoreAclHandler", aclHandler);
      pipeline.addLast("RouterThrottleHandler", routerThrottleHandler);
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

    boolean asyncStart = config.isAsyncStartEnabled();
    CompletableFuture<Void> startFuture = startServices(asyncStart);
    if (asyncStart) {
      startFuture.whenComplete((Object v, Throwable e) -> {
        if (e != null) {
          logger.error("Router has failed to start", e);
          close();
        }
      });
    } else {
      startFuture.get();
      logger.info("All the required services have been started");
    }
    // The start up process is not finished yet if async start is enabled, because it is continuing asynchronously.
    return !asyncStart;
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
    if (serverFuture != null && !serverFuture.cancel(false)){
      serverFuture.awaitUninterruptibly();
    }
    if (secureServerFuture != null && !secureServerFuture.cancel(false)){
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

    storageNodeClient.close();
    workerEventLoopGroup.shutdownGracefully();
    serverEventLoopGroup.shutdownGracefully();
    if (sslResolverEventLoopGroup != null) {
      sslResolverEventLoopGroup.shutdownGracefully();
    }

    dispatcher.stop();

    if (router.isPresent()) {
      router.get().shutdown();
    }
    secureRouter.shutdown();
    if (router.isPresent()) {
      router.get().waitForShutdown();
    }
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
    schemaRepository.clear();
    storeConfigRepository.clear();
    hybridStoreQuotaRepository.ifPresent(repo -> repo.clear());
    liveInstanceMonitor.clear();
    if (partitionStatusOnlineInstanceFinder != null) {
      partitionStatusOnlineInstanceFinder.clear();
    }
    timeoutProcessor.shutdownNow();
    dictionaryRetrievalService.stop();
    if (instanceConfigRepository != null) {
      instanceConfigRepository.clear();
    }
    if (manager != null) {
      manager.disconnect();
    }
    if (zkClient != null) {
      zkClient.close();
    }
    if (heartbeat != null) {
      heartbeat.stopInner();
    }
  }

  public HelixBaseRoutingRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  public ReadOnlyStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  public OnlineInstanceFinder getOnlineInstanceFinder()  {
    return onlineInstanceFinder;
  }

  public ReadOnlySchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  private void handleExceptionInStartServices(VeniceException e, boolean async) throws VeniceException {
    if (async) {
      Utils.exit("Failed to start router services due to " + e);
    } else {
      throw e;
    }
  }
  /**
   * a few tasks will be done asynchronously during the service startup and are moved into this method.
   * We are doing this because there is no way to specify Venice component startup order in "mint deploy".
   * This method prevents "mint deploy" failure (When server or router starts earlier than controller,
   * helix manager throw unknown cluster name exception.) We terminate the process if helix connection
   * cannot be established.
   */
  private CompletableFuture startServices(boolean async) {
    return CompletableFuture.runAsync(() -> {
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
        handleExceptionInStartServices(ve, async);
      }
      // Should refresh after Helix cluster is setup
      liveInstanceMonitor.refresh();
      /**
       * {@link StorageNodeClient#start()} should only be called after {@link liveInstanceMonitor.refresh()} since it only
       * contains the live instance set after refresh.
       */
      try {
        storageNodeClient.start();
      } catch (VeniceException e) {
        logger.error("Encountered issue when starting storage node client", e);
        handleExceptionInStartServices(e, async);
      }

      // Register current router into ZK.
      routersClusterManager = new ZkRoutersClusterManager(zkClient, adapter, config.getClusterName(),
          config.getRefreshAttemptsForZkReconnect(), config.getRefreshIntervalForZkReconnectInMs());
      routersClusterManager.refresh();
      routersClusterManager.registerRouter(Utils.getHelixNodeIdentifier(config.getPort()));
      routingDataRepository.refresh();
      if (hybridStoreQuotaRepository.isPresent()) {
        hybridStoreQuotaRepository.get().refresh();
      }

      readRequestThrottler = new ReadRequestThrottler(routersClusterManager, metadataRepository, routingDataRepository,
          config.getMaxReadCapacityCu(), routerStats.getStatsByType(RequestType.SINGLE_GET), config.getPerStorageNodeReadQuotaBuffer());

      noopRequestThrottler = new NoopRouterThrottler(routersClusterManager, metadataRepository, routerStats.getStatsByType(RequestType.SINGLE_GET));

      // Setup read requests throttler.
      setReadRequestThrottling(config.isReadThrottlingEnabled());

      if (config.getMultiKeyRoutingStrategy().equals(VeniceMultiKeyRoutingStrategy.HELIX_ASSISTED_ROUTING)) {
        /**
         * This statement should be invoked after {@link #manager} is connected.
         */
        instanceConfigRepository =
            new HelixInstanceConfigRepository(manager, config.isUseGroupFieldInHelixDomain());
        instanceConfigRepository.refresh();
        helixGroupSelector = new HelixGroupSelector(metricsRepository, instanceConfigRepository, config.getHelixGroupSelectionStrategy(),
            timeoutProcessor);
        scatterGatherMode.initHelixGroupSelector(helixGroupSelector);
        responseAggregator.initHelixGroupSelector(helixGroupSelector);
      }

      // Dictionary retrieval service should start only after "metadataRepository.refresh()" otherwise it won't be able
      // to preload dictionaries from SN.
      try {
        dictionaryRetrievalService.startInner();
      } catch (VeniceException e) {
        logger.error("Encountered issue when starting dictionary retriever", e);
        handleExceptionInStartServices(e, async);
      }

      /**
       * When the listen port is open, we would like to have current Router to be fully ready.
       */
      if (router.isPresent()) {
        serverFuture = router.get().start(new InetSocketAddress(config.getPort()));
      }
      secureServerFuture = secureRouter.start(new InetSocketAddress(config.getSslPort()));
      try {
        if (router.isPresent()) {
          serverFuture.await();
        }
        secureServerFuture.await();
      } catch (InterruptedException e) {
        handleExceptionInStartServices(new VeniceException(e), async);
      }

      for (D2Server d2Server : d2ServerList) {
        logger.info("Starting d2 announcer: " + d2Server);
        d2Server.forceStart();
      }

      try {
        if (router.isPresent()) {
          int port = ((InetSocketAddress) serverFuture.get()).getPort();
          logger.info(this.toString() + " started on non-ssl port: " + port);
        }
        int sslPort = ((InetSocketAddress) secureServerFuture.get()).getPort();
        logger.info(this.toString() + " started on ssl port: " + sslPort);
      } catch (Exception e) {
        logger.error("Exception while waiting for " + this.toString() + " to start", e);
        serviceState.set(ServiceState.STOPPED);
        handleExceptionInStartServices(new VeniceException(e), async);
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

  public void setReadRequestThrottling(boolean throttle) {
    RouterThrottler throttler = throttle ? readRequestThrottler : noopRequestThrottler;
    scatterGatherMode.initReadRequestThrottler(throttler);
  }

  /* test-only */
  public void refresh() {
    liveInstanceMonitor.refresh();
    storeConfigRepository.refresh();
    metadataRepository.refresh();
    schemaRepository.refresh();
    routingDataRepository.refresh();
    if (hybridStoreQuotaRepository.isPresent()) {
      hybridStoreQuotaRepository.get().refresh();
    }
    if (partitionStatusOnlineInstanceFinder != null) {
      partitionStatusOnlineInstanceFinder.refresh();
    }
  }
}
