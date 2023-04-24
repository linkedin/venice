package com.linkedin.venice.router;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.concurrency.impl.SuccessAsyncFuture;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableExecutors;
import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.alpini.router.api.LongTailRetrySupplier;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.impl.Router;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.DictionaryRetrievalService;
import com.linkedin.venice.router.api.MetaStoreShadowReader;
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
import com.linkedin.venice.router.httpclient.HttpClient5StorageNodeClient;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
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
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceJVMStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RouterServer extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(RouterServer.class);

  // Immutable state
  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private final MetricsRepository metricsRepository;
  private final Optional<SSLFactory> sslFactory;
  private final Optional<DynamicAccessController> accessController;

  private final VeniceRouterConfig config;

  // Mutable state
  // TODO: Make these final once the test constructors are cleaned up.
  private final ZkClient zkClient;
  private SafeHelixManager manager;
  private ReadOnlySchemaRepository schemaRepository;
  private Optional<MetaStoreShadowReader> metaStoreShadowReader;
  /*
   * Helix customized view is a mechanism to store customers' per partition customized states
   * under each instance, aggregate the states across the cluster, and provide the aggregation results to customers.
   */
  private HelixCustomizedViewOfflinePushRepository routingDataRepository;
  private Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository;
  private ReadOnlyStoreRepository metadataRepository;
  private RouterStats<AggRouterHttpRequestStats> routerStats;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;
  private HelixLiveInstanceMonitor liveInstanceMonitor;
  private HelixInstanceConfigRepository instanceConfigRepository;
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
  private final HelixAdapterSerializer adapter;
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

  // A map of optional ChannelHandlers that retains insertion order to be added at the end of the router pipeline
  private final Map<String, ChannelHandler> optionalChannelHandlers = new LinkedHashMap<>();

  private static final String ROUTER_SERVICE_NAME = "venice-router";

  /**
   * Thread number used to monitor the listening port;
   */
  private static final int ROUTER_BOSS_THREAD_NUM = 1;
  private VeniceJVMStats jvmStats;

  private final AggHostHealthStats aggHostHealthStats;

  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      Utils.exit("USAGE: java -jar venice-router-all.jar <router_config_file_path>");
    }

    try {
      String routerConfigFilePath = args[0];
      run(routerConfigFilePath, true);
    } catch (Exception e) {
      throw new VeniceException("No config file parameter found", e);
    }
  }

  public static void run(String routerConfigFilePath, boolean runForever) throws Exception {

    VeniceProperties props = Utils.parseProperties(routerConfigFilePath);
    LOGGER.info("Zookeeper: {}", props.getString(ConfigKeys.ZOOKEEPER_ADDRESS));
    LOGGER.info("Cluster: {}", props.getString(ConfigKeys.CLUSTER_NAME));
    LOGGER.info("Port: {}", props.getInt(ConfigKeys.LISTENER_PORT));
    LOGGER.info("SSL Port: {}", props.getInt(ConfigKeys.LISTENER_SSL_PORT));
    LOGGER.info("IO worker count: {}", props.getInt(ConfigKeys.ROUTER_IO_WORKER_COUNT));

    Optional<SSLFactory> sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());
    RouterServer server = new RouterServer(props, new ArrayList<>(), Optional.empty(), sslFactory);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server.isRunning()) {
          try {
            server.stop();
          } catch (Exception e) {
            LOGGER.error("Error shutting the server. ", e);
          }
        }
      }
    });

    if (runForever) {
      while (true) {
        Thread.sleep(TimeUnit.HOURS.toMillis(1));
      }
    }
  }

  public RouterServer(
      VeniceProperties properties,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<SSLFactory> sslFactory) {
    this(
        properties,
        serviceDiscoveryAnnouncers,
        accessController,
        sslFactory,
        TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME));
  }

  // for test purpose
  public MetricsRepository getMetricsRepository() {
    return this.metricsRepository;
  }

  public RouterServer(
      VeniceProperties properties,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<SSLFactory> sslFactory,
      MetricsRepository metricsRepository) {
    this(properties, serviceDiscoveryAnnouncers, accessController, sslFactory, metricsRepository, true);

    HelixReadOnlyZKSharedSystemStoreRepository readOnlyZKSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, config.getSystemSchemaClusterName());
    HelixReadOnlyStoreRepository readOnlyStoreRepository = new HelixReadOnlyStoreRepository(
        zkClient,
        adapter,
        config.getClusterName(),
        config.getRefreshAttemptsForZkReconnect(),
        config.getRefreshIntervalForZkReconnectInMs());
    this.metadataRepository = new HelixReadOnlyStoreRepositoryAdapter(
        readOnlyZKSharedSystemStoreRepository,
        readOnlyStoreRepository,
        config.getClusterName());
    this.routerStats = new RouterStats<>(
        requestType -> new AggRouterHttpRequestStats(
            metricsRepository,
            requestType,
            config.isKeyValueProfilingEnabled(),
            metadataRepository,
            config.isUnregisterMetricForDeletedStoreEnabled()));
    this.schemaRepository = new HelixReadOnlySchemaRepositoryAdapter(
        new HelixReadOnlyZKSharedSchemaRepository(
            readOnlyZKSharedSystemStoreRepository,
            zkClient,
            adapter,
            config.getSystemSchemaClusterName(),
            config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs()),
        new HelixReadOnlySchemaRepository(
            readOnlyStoreRepository,
            zkClient,
            adapter,
            config.getClusterName(),
            config.getRefreshAttemptsForZkReconnect(),
            config.getRefreshIntervalForZkReconnectInMs()));
    this.metaStoreShadowReader = config.isMetaStoreShadowReadEnabled()
        ? Optional.of(new MetaStoreShadowReader(this.schemaRepository))
        : Optional.empty();
    this.routingDataRepository = new HelixCustomizedViewOfflinePushRepository(manager);
    this.hybridStoreQuotaRepository = config.isHelixHybridStoreQuotaEnabled()
        ? Optional.of(new HelixHybridStoreQuotaRepository(manager))
        : Optional.empty();
    this.storeConfigRepository = new HelixReadOnlyStoreConfigRepository(
        zkClient,
        adapter,
        config.getRefreshAttemptsForZkReconnect(),
        config.getRefreshIntervalForZkReconnectInMs());
    this.liveInstanceMonitor = new HelixLiveInstanceMonitor(this.zkClient, config.getClusterName());
  }

  /**
   * CTOR maintain the common logic for both test and normal CTOR.
   */
  private RouterServer(
      VeniceProperties properties,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<SSLFactory> sslFactory,
      MetricsRepository metricsRepository,
      boolean isCreateHelixManager) {
    config = new VeniceRouterConfig(properties);
    zkClient =
        new ZkClient(config.getZkConnection(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "router-zk-client"));

    this.adapter = new HelixAdapterSerializer();
    if (isCreateHelixManager) {
      this.manager = new SafeHelixManager(
          new ZKHelixManager(config.getClusterName(), null, InstanceType.SPECTATOR, config.getZkConnection()));
    }
    this.metaStoreShadowReader = Optional.empty();
    this.metricsRepository = metricsRepository;

    this.aggHostHealthStats = new AggHostHealthStats(metricsRepository);

    this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
    this.accessController = accessController;
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
      HelixCustomizedViewOfflinePushRepository routingDataRepository,
      Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository,
      HelixReadOnlyStoreRepository metadataRepository,
      HelixReadOnlySchemaRepository schemaRepository,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<SSLFactory> sslFactory,
      HelixLiveInstanceMonitor liveInstanceMonitor) {
    this(properties, serviceDiscoveryAnnouncers, Optional.empty(), sslFactory, new MetricsRepository(), false);
    this.routingDataRepository = routingDataRepository;
    this.hybridStoreQuotaRepository = hybridStoreQuotaRepository;
    this.metadataRepository = metadataRepository;
    this.routerStats = new RouterStats<>(
        requestType -> new AggRouterHttpRequestStats(
            metricsRepository,
            requestType,
            config.isKeyValueProfilingEnabled(),
            metadataRepository,
            config.isUnregisterMetricForDeletedStoreEnabled()));
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
    ResourceRegistry.setGlobalShutdownDelayMillis(
        TimeUnit.SECONDS.toMillis(config.getRouterNettyGracefulShutdownPeriodSeconds() * 2L));

    jvmStats = new VeniceJVMStats(metricsRepository, "VeniceJVMStats");

    metadataRepository.refresh();
    storeConfigRepository.refresh();
    // No need to call schemaRepository.refresh() since it will do nothing.
    registry = new ResourceRegistry();
    workerExecutor = registry.factory(ShutdownableExecutors.class)
        .newCachedThreadPool(new DefaultThreadFactory("RouterThread", true, Thread.MAX_PRIORITY));
    /**
     * Use TreeMap inside TimeoutProcessor; the other option ConcurrentSkipList has performance issue.
     *
     * Refer to more context on {@link VeniceRouterConfig#checkProperties(VeniceProperties)}
     */
    timeoutProcessor = new TimeoutProcessor(registry, true, 1);

    Optional<SSLFactory> sslFactoryForRequests = config.isSslToStorageNodes() ? sslFactory : Optional.empty();
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository, metadataRepository);
    Class<? extends AbstractChannel> serverSocketChannelClass;
    boolean useEpoll = true;
    try {
      serverEventLoopGroup = new EpollEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      workerEventLoopGroup = new EpollEventLoopGroup(config.getRouterIOWorkerCount(), workerExecutor);
      serverSocketChannelClass = EpollServerSocketChannel.class;
    } catch (LinkageError error) {
      useEpoll = false;
      LOGGER.info("Epoll is only supported on Linux; switching to NIO");
      serverEventLoopGroup = new NioEventLoopGroup(ROUTER_BOSS_THREAD_NUM);
      workerEventLoopGroup = new NioEventLoopGroup(config.getRouterIOWorkerCount(), workerExecutor);
      serverSocketChannelClass = NioServerSocketChannel.class;
    }

    switch (config.getStorageNodeClientType()) {
      case APACHE_HTTP_ASYNC_CLIENT:
        LOGGER.info("Router will use Apache_Http_Async_Client");
        storageNodeClient =
            new ApacheHttpAsyncStorageNodeClient(config, sslFactoryForRequests, metricsRepository, liveInstanceMonitor);
        break;
      case HTTP_CLIENT_5_CLIENT:
        LOGGER.info("Router will use HTTP CLIENT5");
        storageNodeClient = new HttpClient5StorageNodeClient(sslFactoryForRequests, config);
        break;
      default:
        throw new VeniceException(
            "Router client type " + config.getStorageNodeClientType().toString() + " is not supported!");
    }

    RouteHttpRequestStats routeHttpRequestStats = new RouteHttpRequestStats(metricsRepository, storageNodeClient);

    VeniceHostHealth healthMonitor =
        new VeniceHostHealth(liveInstanceMonitor, storageNodeClient, config, routeHttpRequestStats, aggHostHealthStats);
    dispatcher = new VeniceDispatcher(
        config,
        metadataRepository,
        routerStats,
        metricsRepository,
        storageNodeClient,
        routeHttpRequestStats,
        aggHostHealthStats,
        routerStats);
    scatterGatherMode = new VeniceDelegateMode(config, routerStats, routeHttpRequestStats);

    if (config.isRouterHeartBeatEnabled()) {
      heartbeat =
          new RouterHeartbeat(liveInstanceMonitor, healthMonitor, config, sslFactoryForRequests, storageNodeClient);
      heartbeat.startInner();
    }

    /**
     * TODO: find a way to add read compute stats in host finder;
     *
     * Host finder uses http method to distinguish single-get from multi-get and it doesn't have any other information,
     * so there is no way to distinguish compute request from multi-get; all read compute metrics in host finder will
     * be recorded as multi-get metrics; affected metric is "find_unhealthy_host_request"
     */

    CompressorFactory compressorFactory = new CompressorFactory();

    dictionaryRetrievalService = new DictionaryRetrievalService(
        routingDataRepository,
        config,
        sslFactoryForRequests,
        metadataRepository,
        storageNodeClient,
        compressorFactory);

    MetaDataHandler metaDataHandler = new MetaDataHandler(
        routingDataRepository,
        schemaRepository,
        storeConfigRepository,
        config.getClusterToD2Map(),
        config.getClusterToServerD2Map(),
        metadataRepository,
        hybridStoreQuotaRepository,
        config.getClusterName(),
        config.getZkConnection(),
        config.getKafkaBootstrapServers());

    VeniceHostFinder hostFinder = new VeniceHostFinder(routingDataRepository, routerStats, healthMonitor);

    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        metadataRepository,
        routingDataRepository,
        new StaleVersionStats(metricsRepository, "stale_version"),
        storeConfigRepository,
        config.getClusterToD2Map(),
        config.getClusterName(),
        compressorFactory);
    VenicePathParser pathParser = new VenicePathParser(
        versionFinder,
        partitionFinder,
        routerStats,
        metadataRepository,
        config,
        compressorFactory);

    // Setup stat tracking for exceptional case
    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    // Fixed retry future
    AsyncFuture<LongSupplier> singleGetRetryFuture =
        new SuccessAsyncFuture<>(config::getLongTailRetryForSingleGetThresholdMs);
    LongTailRetrySupplier retrySupplier = new LongTailRetrySupplier<VenicePath, RouterKey>() {
      private final TreeMap<Integer, Integer> longTailRetryConfigForBatchGet =
          config.getLongTailRetryForBatchGetThresholdMs();

      @Nonnull
      @Override
      public AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(
          @Nonnull VenicePath path,
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
          if (keyNum == 0) {
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

    responseAggregator = new VeniceResponseAggregator(routerStats, metaStoreShadowReader);
    /**
     * No need to setup {@link com.linkedin.alpini.router.api.HostHealthMonitor} here since
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
                .withComputeTardyThreshold(config.getComputeTardyLatencyThresholdMs(), TimeUnit.MILLISECONDS))
        .metricsProvider(new VeniceMetricsProvider())
        .longTailRetrySupplier(retrySupplier)
        .scatterGatherStatsProvider(new LongTailRetryStatsProvider(routerStats))
        .enableStackTraceResponseForException(true)
        .enableRetryRequestAlwaysUseADifferentHost(true)
        .build();

    SecurityStats securityStats = new SecurityStats(
        this.metricsRepository,
        "security",
        secureRouter != null ? () -> secureRouter.getConnectedCount() : () -> 0);
    RouterThrottleStats routerThrottleStats = new RouterThrottleStats(this.metricsRepository, "router_throttler_stats");
    routerEarlyThrottler = new EventThrottler(
        config.getMaxRouterReadCapacityCu(),
        config.getRouterQuotaCheckWindow(),
        "router-early-throttler",
        true,
        EventThrottler.REJECT_STRATEGY);

    RouterSslVerificationHandler unsecureRouterSslVerificationHandler =
        new RouterSslVerificationHandler(securityStats, config.isEnforcingSecureOnly());
    HealthCheckStats healthCheckStats = new HealthCheckStats(this.metricsRepository, "healthcheck_stats");
    AdminOperationsStats adminOperationsStats = new AdminOperationsStats(this.metricsRepository, "admin_stats", config);
    AdminOperationsHandler adminOperationsHandler =
        new AdminOperationsHandler(accessController.orElse(null), this, adminOperationsStats);

    // TODO: deprecate non-ssl port
    if (!config.isEnforcingSecureOnly()) {
      router = Optional.of(
          Router.builder(scatterGather)
              .name("VeniceRouterHttp")
              .resourceRegistry(registry)
              .serverSocketChannel(serverSocketChannelClass)
              .bossPoolBuilder(EventLoopGroup.class, ignored -> serverEventLoopGroup)
              .ioWorkerPoolBuilder(EventLoopGroup.class, ignored -> workerEventLoopGroup)
              .connectionLimit(config.getConnectionLimit())
              .timeoutProcessor(timeoutProcessor)
              .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
                pipeline.addLast(
                    "RouterThrottleHandler",
                    new RouterThrottleHandler(routerThrottleStats, routerEarlyThrottler, config));
                pipeline.addLast("HealthCheckHandler", new HealthCheckHandler(healthCheckStats));
                pipeline.addLast("VerifySslHandler", unsecureRouterSslVerificationHandler);
                pipeline.addLast("MetadataHandler", metaDataHandler);
                pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
                addStreamingHandler(pipeline);
                addOptionalChannelHandlersToPipeline(pipeline);
              })
              .idleTimeout(3, TimeUnit.HOURS)
              .enableInboundHttp2(config.isHttp2InboundEnabled())
              .build());
    }

    RouterSslVerificationHandler routerSslVerificationHandler = new RouterSslVerificationHandler(securityStats);
    StoreAclHandler aclHandler =
        accessController.isPresent() ? new StoreAclHandler(accessController.get(), metadataRepository) : null;
    final SslInitializer sslInitializer;
    if (sslFactory.isPresent()) {
      sslInitializer = new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory.get()), false);
      if (config.isThrottleClientSslHandshakesEnabled()) {
        ExecutorService sslHandshakeExecutor = registry.factory(ShutdownableExecutors.class)
            .newFixedThreadPool(
                config.getClientSslHandshakeThreads(),
                new DefaultThreadFactory("RouterSSLHandshakeThread", true, Thread.NORM_PRIORITY));
        int clientSslHandshakeThreads = config.getClientSslHandshakeThreads();
        int maxConcurrentClientSslHandshakes = config.getMaxConcurrentClientSslHandshakes();
        int clientSslHandshakeAttempts = config.getClientSslHandshakeAttempts();
        long clientSslHandshakeBackoffMs = config.getClientSslHandshakeBackoffMs();
        if (useEpoll) {
          sslResolverEventLoopGroup = new EpollEventLoopGroup(clientSslHandshakeThreads, sslHandshakeExecutor);
        } else {
          sslResolverEventLoopGroup = new NioEventLoopGroup(clientSslHandshakeThreads, sslHandshakeExecutor);
        }
        sslInitializer.enableResolveBeforeSSL(
            sslResolverEventLoopGroup,
            clientSslHandshakeAttempts,
            clientSslHandshakeBackoffMs,
            maxConcurrentClientSslHandshakes);
      }
    } else {
      sslInitializer = null;
    }

    Consumer<ChannelPipeline> noop = pipeline -> {};
    Consumer<ChannelPipeline> addSslInitializer = pipeline -> {
      pipeline.addFirst("SSL Initializer", sslInitializer);
    };
    HealthCheckHandler secureRouterHealthCheckHander = new HealthCheckHandler(healthCheckStats);
    RouterThrottleHandler routerThrottleHandler =
        new RouterThrottleHandler(routerThrottleStats, routerEarlyThrottler, config);
    Consumer<ChannelPipeline> withoutAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", routerSslVerificationHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
      pipeline.addLast("RouterThrottleHandler", routerThrottleHandler);
      addStreamingHandler(pipeline);
      addOptionalChannelHandlersToPipeline(pipeline);
    };
    Consumer<ChannelPipeline> withAcl = pipeline -> {
      pipeline.addLast("HealthCheckHandler", secureRouterHealthCheckHander);
      pipeline.addLast("VerifySslHandler", routerSslVerificationHandler);
      pipeline.addLast("MetadataHandler", metaDataHandler);
      pipeline.addLast("AdminOperationsHandler", adminOperationsHandler);
      pipeline.addLast("StoreAclHandler", aclHandler);
      pipeline.addLast("RouterThrottleHandler", routerThrottleHandler);
      addStreamingHandler(pipeline);
      addOptionalChannelHandlersToPipeline(pipeline);
    };

    secureRouter = Router.builder(scatterGather)
        .name("SecureVeniceRouterHttps")
        .resourceRegistry(registry)
        .serverSocketChannel(serverSocketChannelClass)
        .bossPoolBuilder(EventLoopGroup.class, ignored -> serverEventLoopGroup)
        .ioWorkerPoolBuilder(EventLoopGroup.class, ignored -> workerEventLoopGroup)
        .connectionLimit(config.getConnectionLimit())
        .timeoutProcessor(timeoutProcessor)
        .beforeHttpServerCodec(ChannelPipeline.class, sslFactory.isPresent() ? addSslInitializer : noop) // Compare once
                                                                                                         // per router.
                                                                                                         // Previously
                                                                                                         // compared
                                                                                                         // once per
                                                                                                         // request.
        .beforeHttpRequestHandler(ChannelPipeline.class, accessController.isPresent() ? withAcl : withoutAcl) // Compare
                                                                                                              // once
                                                                                                              // per
                                                                                                              // router.
                                                                                                              // Previously
                                                                                                              // compared
                                                                                                              // once
                                                                                                              // per
                                                                                                              // request.
        .idleTimeout(3, TimeUnit.HOURS)
        .enableInboundHttp2(config.isHttp2InboundEnabled())
        .http2MaxConcurrentStreams(config.getHttp2MaxConcurrentStreams())
        .http2HeaderTableSize(config.getHttp2HeaderTableSize())
        .http2InitialWindowSize(config.getHttp2InitialWindowSize())
        .http2MaxFrameSize(config.getHttp2MaxFrameSize())
        .http2MaxHeaderListSize(config.getHttp2MaxHeaderListSize())
        .build();

    boolean asyncStart = config.isAsyncStartEnabled();
    CompletableFuture<Void> startFuture = startServices(asyncStart);
    if (asyncStart) {
      startFuture.whenComplete((Object v, Throwable e) -> {
        if (e != null) {
          LOGGER.error("Router has failed to start", e);
          close();
        }
      });
    } else {
      startFuture.get();
      LOGGER.info("All the required services have been started");
    }
    // The start up process is not finished yet if async start is enabled, because it is continuing asynchronously.
    return !asyncStart;
  }

  private void addStreamingHandler(ChannelPipeline pipeline) {
    pipeline.addLast("VeniceChunkedWriteHandler", new VeniceChunkedWriteHandler());
  }

  private void addOptionalChannelHandlersToPipeline(ChannelPipeline pipeline) {
    for (Map.Entry<String, ChannelHandler> channelHandler: optionalChannelHandlers.entrySet()) {
      pipeline.addLast(channelHandler.getKey(), channelHandler.getValue());
    }
  }

  public void addOptionalChannelHandler(String key, ChannelHandler channelHandler) {
    optionalChannelHandlers.put(key, channelHandler);
  }

  @Override
  public void stopInner() throws Exception {
    for (ServiceDiscoveryAnnouncer serviceDiscoveryAnnouncer: serviceDiscoveryAnnouncers) {
      LOGGER.info("Unregistering from service discovery: {}", serviceDiscoveryAnnouncer);
      try {
        serviceDiscoveryAnnouncer.unregister();
      } catch (RuntimeException e) {
        LOGGER.error("Service discovery announcer {} failed to unregister properly", serviceDiscoveryAnnouncer, e);
      }
    }
    // Graceful shutdown
    Thread.sleep(TimeUnit.SECONDS.toMillis(config.getRouterNettyGracefulShutdownPeriodSeconds()));
    if (serverFuture != null && !serverFuture.cancel(false)) {
      serverFuture.awaitUninterruptibly();
    }
    if (secureServerFuture != null && !secureServerFuture.cancel(false)) {
      secureServerFuture.awaitUninterruptibly();
    }
    /**
     * The following change is trying to solve the router stop stuck issue.
     * From the logs, it seems {@link ResourceRegistry.State#performShutdown()} is not shutting down
     * resources synchronously when necessary, which could cause some race condition.
     * For example, "executor" initialized in {@link #startInner()} could be shutdown earlier than
     * {@link #router} and {@link #secureRouter}, then the shutdown of {@link #router} and {@link #secureRouter}
     * would be blocked because of the following exception:
     * 2018/08/21 17:55:40.855 ERROR [rejectedExecution] [shutdown-com.linkedin.alpini.router.impl.netty4.Router4Impl$$Lambda$230/594142688@5a8fd55c]
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

    router.ifPresent(Router::shutdown);
    secureRouter.shutdown();

    if (router.isPresent()) {
      router.get().waitForShutdown();
    }
    LOGGER.info("Non-secure router has been shutdown completely");
    secureRouter.waitForShutdown();
    LOGGER.info("Secure router has been shutdown completely");
    registry.shutdown();
    registry.waitForShutdown();
    LOGGER.info("Other resources managed by local ResourceRegistry have been shutdown completely");

    routersClusterManager.unregisterRouter(Utils.getHelixNodeIdentifier(config.getHostname(), config.getPort()));
    routersClusterManager.clear();
    routingDataRepository.clear();
    metadataRepository.clear();
    schemaRepository.clear();
    storeConfigRepository.clear();
    hybridStoreQuotaRepository.ifPresent(repo -> repo.clear());
    liveInstanceMonitor.clear();
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
        if (this.manager == null) {
          // TODO: Remove this check once test constructor is removed or otherwise fixed.
          LOGGER.info("Not connecting to Helix because the HelixManager is null (the test constructor was used)");
        } else {
          HelixUtils.connectHelixManager(manager, 30, 1);
          LOGGER.info("{} finished connectHelixManager()", this);
        }
      } catch (VeniceException ve) {
        LOGGER.error("{} got an exception while trying to connectHelixManager()", this, ve);
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
        LOGGER.error("Encountered issue when starting storage node client", e);
        handleExceptionInStartServices(e, async);
      }

      // Register current router into ZK.
      routersClusterManager = new ZkRoutersClusterManager(
          zkClient,
          adapter,
          config.getClusterName(),
          config.getRefreshAttemptsForZkReconnect(),
          config.getRefreshIntervalForZkReconnectInMs());
      routersClusterManager.refresh();
      routersClusterManager.registerRouter(Utils.getHelixNodeIdentifier(config.getHostname(), config.getPort()));
      routingDataRepository.refresh();
      hybridStoreQuotaRepository.ifPresent(HelixHybridStoreQuotaRepository::refresh);

      readRequestThrottler = new ReadRequestThrottler(
          routersClusterManager,
          metadataRepository,
          routingDataRepository,
          routerStats.getStatsByType(RequestType.SINGLE_GET),
          config);

      noopRequestThrottler = new NoopRouterThrottler(
          routersClusterManager,
          metadataRepository,
          routerStats.getStatsByType(RequestType.SINGLE_GET));

      // Setup read requests throttler.
      setReadRequestThrottling(config.isReadThrottlingEnabled());

      if (config.getMultiKeyRoutingStrategy().equals(VeniceMultiKeyRoutingStrategy.HELIX_ASSISTED_ROUTING)) {
        /**
         * This statement should be invoked after {@link #manager} is connected.
         */
        instanceConfigRepository = new HelixInstanceConfigRepository(manager, config.isUseGroupFieldInHelixDomain());
        instanceConfigRepository.refresh();
        helixGroupSelector = new HelixGroupSelector(
            metricsRepository,
            instanceConfigRepository,
            config.getHelixGroupSelectionStrategy(),
            timeoutProcessor);
        scatterGatherMode.initHelixGroupSelector(helixGroupSelector);
        responseAggregator.initHelixGroupSelector(helixGroupSelector);
      }

      // Dictionary retrieval service should start only after "metadataRepository.refresh()" otherwise it won't be able
      // to preload dictionaries from SN.
      try {
        dictionaryRetrievalService.startInner();
      } catch (VeniceException e) {
        LOGGER.error("Encountered issue when starting dictionary retriever", e);
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

      for (ServiceDiscoveryAnnouncer serviceDiscoveryAnnouncer: serviceDiscoveryAnnouncers) {
        LOGGER.info("Registering to service discovery: {}", serviceDiscoveryAnnouncer);
        serviceDiscoveryAnnouncer.register();
      }

      try {
        if (router.isPresent()) {
          int port = ((InetSocketAddress) serverFuture.get()).getPort();
          LOGGER.info("{} started on non-ssl port: {}", this, port);
        }
        int sslPort = ((InetSocketAddress) secureServerFuture.get()).getPort();
        LOGGER.info("{} started on ssl port: {}", this, sslPort);
      } catch (Exception e) {
        LOGGER.error("Exception while waiting for {} to start", this, e);
        serviceState.set(ServiceState.STOPPED);
        handleExceptionInStartServices(new VeniceException(e), async);
      }

      serviceState.set(ServiceState.STARTED);
    });
  }

  private void verifySslOk() {
    if (config.isSslToStorageNodes() && !sslFactory.isPresent()) {
      throw new VeniceException("Must specify an SSLFactory in order to use SSL in requests to storage nodes");
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
    hybridStoreQuotaRepository.ifPresent(HelixHybridStoreQuotaRepository::refresh);
  }
}
