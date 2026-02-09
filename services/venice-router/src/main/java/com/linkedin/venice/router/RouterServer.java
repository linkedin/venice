package com.linkedin.venice.router;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigConstants.DEFAULT_PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.utils.concurrent.BlockingQueueType.LINKED_BLOCKING_QUEUE;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.concurrency.impl.SuccessAsyncFuture;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableExecutors;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.alpini.router.api.LongTailRetrySupplier;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.impl.Router;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.d2.D2ConfigUtils;
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
import com.linkedin.venice.helix.HelixReadOnlyStoreViewConfigRepositoryAdapter;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.NameRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.acl.RouterStoreAclHandler;
import com.linkedin.venice.router.api.DictionaryRetrievalService;
import com.linkedin.venice.router.api.MetaStoreShadowReader;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterHeartbeat;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.router.api.VeniceRole;
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
import com.linkedin.venice.router.stats.RouterMetricEntity;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.stats.RouterThrottleStats;
import com.linkedin.venice.router.stats.SecurityStats;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.stats.VeniceJVMStats;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.ThreadPoolFactory;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RouterServer extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(RouterServer.class);
  public static final String DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME = "venice-discovery";
  private static final String ROUTER_RETRY_MANAGER_THREAD_PREFIX = "Router-retry-manager-thread";
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
  private HelixReadOnlyStoreViewConfigRepositoryAdapter storeConfigRepository;

  private PushStatusStoreReader pushStatusStoreReader;
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
  private D2Client d2Client;
  private String d2ServiceName;
  private DictionaryRetrievalService dictionaryRetrievalService;
  private RouterThrottler readRequestThrottler;

  private MultithreadEventLoopGroup workerEventLoopGroup;
  private MultithreadEventLoopGroup serverEventLoopGroup;
  private MultithreadEventLoopGroup sslResolverEventLoopGroup;

  private ExecutorService workerExecutor;
  private EventThrottler routerEarlyThrottler;

  private final IdentityParser identityParser;

  // A map of optional ChannelHandlers that retains insertion order to be added at the end of the router pipeline
  private final Map<String, ChannelHandler> optionalChannelHandlers = new LinkedHashMap<>();

  public static final String ROUTER_SERVICE_NAME = "venice-router";
  public static final String ROUTER_SERVICE_METRIC_PREFIX = "router";
  public static final Collection<MetricEntity> ROUTER_SERVICE_METRIC_ENTITIES =
      ModuleMetricEntityInterface.getUniqueMetricEntities(RouterMetricEntity.class);
  /**
   * Thread number used to monitor the listening port;
   */
  private static final int ROUTER_BOSS_THREAD_NUM = 1;
  private VeniceJVMStats jvmStats;

  private final AggHostHealthStats aggHostHealthStats;

  private ScheduledExecutorService retryManagerExecutorService;
  private ThreadPoolExecutor responseAggregationExecutor;
  private ThreadPoolExecutor dnsResolveExecutor;

  private InFlightRequestStat inFlightRequestStat;

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

    Optional<SSLFactory> sslFactory;
    if (props.getBoolean(ConfigKeys.ROUTER_ENABLE_SSL, true)) {
      if (props.getBoolean(ConfigKeys.ROUTER_USE_LOCAL_SSL_SETTINGS, true)) {
        sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());
      } else {
        String sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
        sslFactory = Optional.of(SslUtils.getSSLFactory(props.toProperties(), sslFactoryClassName));
      }
    } else {
      sslFactory = Optional.empty();
    }

    List<ServiceDiscoveryAnnouncer> d2Servers = new ArrayList<>();

    if (props.getBoolean("router.d2.announce.enabled", false)) {
      String zkAddress = props.getString(ConfigKeys.ZOOKEEPER_ADDRESS);
      String announceHost = props.getString("router.d2.announce.host", "localhost");
      int port = props.getInt(ConfigKeys.LISTENER_PORT);
      String localUri = "http://" + announceHost + ":" + port;
      Map<String, String> clusterToD2 = props.getMap(ConfigKeys.CLUSTER_TO_D2);

      for (Map.Entry<String, String> entry: clusterToD2.entrySet()) {
        String d2ServiceName = entry.getValue();
        String d2ClusterName = d2ServiceName + "_d2_cluster";
        D2ConfigUtils.setupD2Config(zkAddress, false, d2ClusterName, d2ServiceName);
        d2Servers.addAll(D2ConfigUtils.getD2Servers(zkAddress, d2ClusterName, localUri));
      }

      // Always announce the global cluster discovery service (separate from per-cluster D2)
      String discoveryServiceName = DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
      String discoveryClusterName = discoveryServiceName + "_d2_cluster";
      D2ConfigUtils.setupD2Config(zkAddress, false, discoveryClusterName, discoveryServiceName);
      d2Servers.addAll(D2ConfigUtils.getD2Servers(zkAddress, discoveryClusterName, localUri));

      LOGGER.info("D2 announcement enabled with {} announcers for router URI: {}", d2Servers.size(), localUri);
    }

    RouterServer server = new RouterServer(props, d2Servers, Optional.empty(), sslFactory);
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
        VeniceMetricsRepository.getVeniceMetricsRepository(
            ROUTER_SERVICE_NAME,
            ROUTER_SERVICE_METRIC_PREFIX,
            ROUTER_SERVICE_METRIC_ENTITIES,
            properties.getAsMap()),
        null,
        "venice-discovery");
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
    this(
        properties,
        serviceDiscoveryAnnouncers,
        accessController,
        sslFactory,
        metricsRepository,
        null,
        DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME);
  }

  public RouterServer(
      VeniceProperties properties,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<SSLFactory> sslFactory,
      MetricsRepository metricsRepository,
      D2Client d2Client,
      String d2ServiceName) {
    this(properties, serviceDiscoveryAnnouncers, accessController, sslFactory, metricsRepository, true);
    HelixReadOnlyZKSharedSystemStoreRepository readOnlyZKSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, config.getSystemSchemaClusterName());
    HelixReadOnlyStoreRepository readOnlyStoreRepository =
        new HelixReadOnlyStoreRepository(zkClient, adapter, config.getClusterName());
    this.metadataRepository = new HelixReadOnlyStoreRepositoryAdapter(
        readOnlyZKSharedSystemStoreRepository,
        readOnlyStoreRepository,
        config.getClusterName());
    this.routerStats = new RouterStats<>(
        requestType -> new AggRouterHttpRequestStats(
            config.getClusterName(),
            metricsRepository,
            requestType,
            config.isKeyValueProfilingEnabled(),
            metadataRepository,
            config.isUnregisterMetricForDeletedStoreEnabled(),
            inFlightRequestStat.getTotalInflightRequestSensor()));
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
    this.routingDataRepository = new HelixCustomizedViewOfflinePushRepository(manager, metadataRepository, false);
    this.hybridStoreQuotaRepository = config.isHelixHybridStoreQuotaEnabled()
        ? Optional.of(new HelixHybridStoreQuotaRepository(manager))
        : Optional.empty();
    this.storeConfigRepository =
        new HelixReadOnlyStoreViewConfigRepositoryAdapter(new HelixReadOnlyStoreConfigRepository(zkClient, adapter));
    this.liveInstanceMonitor = new HelixLiveInstanceMonitor(this.zkClient, config.getClusterName());

    this.pushStatusStoreReader = new PushStatusStoreReader(
        d2Client,
        d2ServiceName,
        DEFAULT_PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS);
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

    this.aggHostHealthStats = new AggHostHealthStats(config.getClusterName(), metricsRepository);

    this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
    this.accessController = accessController;
    this.sslFactory = sslFactory;

    Class<IdentityParser> identityParserClass = ReflectUtils.loadClass(config.getIdentityParserClassName());
    this.identityParser = ReflectUtils.callConstructor(identityParserClass, new Class[0], new Object[0]);
    inFlightRequestStat = new InFlightRequestStat(config);
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
      HelixReadOnlyStoreViewConfigRepositoryAdapter storeConfigRepository,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<SSLFactory> sslFactory,
      HelixLiveInstanceMonitor liveInstanceMonitor) {
    this(
        properties,
        serviceDiscoveryAnnouncers,
        Optional.empty(),
        sslFactory,
        VeniceMetricsRepository.getVeniceMetricsRepository(
            ROUTER_SERVICE_NAME,
            ROUTER_SERVICE_METRIC_PREFIX,
            ROUTER_SERVICE_METRIC_ENTITIES,
            properties.getAsMap()),
        false);
    this.routingDataRepository = routingDataRepository;
    this.hybridStoreQuotaRepository = hybridStoreQuotaRepository;
    this.metadataRepository = metadataRepository;
    this.routerStats = new RouterStats<>(
        requestType -> new AggRouterHttpRequestStats(
            config.getClusterName(),
            metricsRepository,
            requestType,
            config.isKeyValueProfilingEnabled(),
            metadataRepository,
            config.isUnregisterMetricForDeletedStoreEnabled(),
            inFlightRequestStat.getTotalInflightRequestSensor()));
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

    Optional<SSLFactory> sslFactoryForRequests = Optional.empty();
    if (config.isSslToStorageNodes()) {
      if (!sslFactory.isPresent()) {
        throw new VeniceException("SSLFactory is required when enabling ssl to storage nodes");
      }
      if (config.isHttpClientOpensslEnabled()) {
        sslFactoryForRequests = Optional.of(SslUtils.toSSLFactoryWithOpenSSLSupport(sslFactory.get()));
      } else {
        sslFactoryForRequests = sslFactory;
      }
    }
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
    scatterGatherMode =
        new VeniceDelegateMode(config, routerStats, routeHttpRequestStats, dispatcher.getPerRouteStatsByType());

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

    VeniceHostFinder hostFinder = new VeniceHostFinder(routingDataRepository, routerStats, healthMonitor);

    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        metadataRepository,
        routingDataRepository,
        new StaleVersionStats(metricsRepository, "stale_version"),
        storeConfigRepository,
        config.getClusterToD2Map(),
        config.getClusterName(),
        compressorFactory,
        metricsRepository);

    retryManagerExecutorService = Executors.newScheduledThreadPool(
        config.getRetryManagerCorePoolSize(),
        new DaemonThreadFactory(ROUTER_RETRY_MANAGER_THREAD_PREFIX, config.getLogContext()));

    VenicePathParser pathParser = new VenicePathParser(
        versionFinder,
        partitionFinder,
        routerStats,
        metadataRepository,
        config,
        compressorFactory,
        metricsRepository,
        retryManagerExecutorService,
        new NameRepository(this.config.getNameRepoMaxEntryCount()));

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
        config.getKafkaBootstrapServers(),
        config.isSslToKafka(),
        versionFinder,
        pushStatusStoreReader,
        metricsRepository);

    // Setup stat tracking for exceptional case
    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    // Fixed retry future
    LongTailRetrySupplier<VenicePath, RouterKey> retrySupplier =
        (path, methodName) -> new SuccessAsyncFuture<>(path::getLongTailRetryThresholdMs);

    responseAggregator = new VeniceResponseAggregator(routerStats, metaStoreShadowReader);
    /**
     * No need to setup {@link com.linkedin.alpini.router.api.HostHealthMonitor} here since
     * {@link VeniceHostFinder} will always do health check.
     */
    // Create dedicated thread pool for response aggregation if enabled (size > 0), to move work off the Netty EventLoop
    // (stageExecutor(ctx)) and isolate it from slow client I/O.
    int responseAggregationThreadPoolSize = config.getResponseAggregationThreadPoolSize();
    if (responseAggregationThreadPoolSize > 0) {
      int responseAggregationQueueCapacity = config.getResponseAggregationQueueCapacity();
      this.responseAggregationExecutor = ThreadPoolFactory.createThreadPool(
          responseAggregationThreadPoolSize,
          "ResponseAggregationThread",
          config.getLogContext(),
          responseAggregationQueueCapacity,
          LINKED_BLOCKING_QUEUE);
      new ThreadPoolStats(metricsRepository, responseAggregationExecutor, "response_aggregation_thread_pool");
      LOGGER.info(
          "Response aggregation thread pool enabled with size: {}, queue capacity: {}",
          responseAggregationThreadPoolSize,
          responseAggregationQueueCapacity);
    } else {
      LOGGER.info("Response aggregation thread pool disabled (size <= 0), using Netty EventLoop for aggregation");
    }

    ScatterGatherHelper scatterGather = ScatterGatherHelper
        .<Instance, VenicePath, RouterKey, VeniceRole, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus>builder()
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
        .metricsProvider(request -> new Metrics())
        .longTailRetrySupplier(retrySupplier)
        .scatterGatherStatsProvider(new LongTailRetryStatsProvider(routerStats))
        .enableStackTraceResponseForException(true)
        .enableRetryRequestAlwaysUseADifferentHost(true)
        .responseAggregationExecutor(responseAggregationExecutor)
        .build();

    SecurityStats securityStats = new SecurityStats(this.metricsRepository, "security");
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
              .connectionHandleMode(config.getConnectionHandleMode())
              .connectionCountRecorder(securityStats::recordLiveConnectionCount)
              .rejectedConnectionCountRecorder(securityStats::recordRejectedConnectionCount)
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
    RouterStoreAclHandler aclHandler = accessController.isPresent()
        ? new RouterStoreAclHandler(
            identityParser,
            accessController.get(),
            metadataRepository,
            config.getAclInMemoryCacheTTLMs())
        : null;
    final SslInitializer sslInitializer;
    if (sslFactory.isPresent()) {
      sslInitializer = new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory.get()), false);
      if (config.getResolveThreads() > 0) {
        this.dnsResolveExecutor = ThreadPoolFactory.createThreadPool(
            config.getResolveThreads(),
            "DNSResolveThread",
            config.getLogContext(),
            config.getResolveQueueCapacity(),
            LINKED_BLOCKING_QUEUE);
        new ThreadPoolStats(metricsRepository, this.dnsResolveExecutor, "dns_resolution_thread_pool");
        int resolveThreads = config.getResolveThreads();
        int maxConcurrentSslHandshakes = config.getMaxConcurrentSslHandshakes();
        int clientResolutionRetryAttempts = config.getClientResolutionRetryAttempts();
        long clientResolutionRetryBackoffMs = config.getClientResolutionRetryBackoffMs();
        if (useEpoll) {
          sslResolverEventLoopGroup = new EpollEventLoopGroup(resolveThreads, dnsResolveExecutor);
        } else {
          sslResolverEventLoopGroup = new NioEventLoopGroup(resolveThreads, dnsResolveExecutor);
        }
        sslInitializer.enableResolveBeforeSSL(
            sslResolverEventLoopGroup,
            clientResolutionRetryAttempts,
            clientResolutionRetryBackoffMs,
            maxConcurrentSslHandshakes,
            config.isClientIPSpoofingCheckEnabled());
      }
      sslInitializer.setIdentityParser(identityParser::parseIdentityFromCert);
      securityStats.registerSslHandshakeSensors(sslInitializer);
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
      pipeline.addLast("RouterStoreAclHandler", aclHandler);
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
        .connectionHandleMode(config.getConnectionHandleMode())
        .connectionCountRecorder(securityStats::recordLiveConnectionCount)
        .rejectedConnectionCountRecorder(securityStats::recordRejectedConnectionCount)
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

  public double getInFlightRequestRate() {
    return inFlightRequestStat.getInFlightRequestRate();
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

    LOGGER.info("Waiting to make sure all in-flight requests are drained");
    // Graceful shutdown: Wait till all the requests are drained
    try {
      RetryUtils.executeWithMaxAttempt(() -> {
        double inFlightRequestRate = inFlightRequestStat.getInFlightRequestRate();
        if (inFlightRequestRate > 0.0) {
          throw new VeniceException("There are still in-flight requests in router :" + inFlightRequestRate);
        }
      }, 30, Duration.ofSeconds(1), Collections.singletonList(VeniceException.class));
    } catch (VeniceException e) {
      LOGGER.error(
          "There are still in-flight request during router shutdown, still continuing shutdown, it might cause unhealthy request in client");
    }
    LOGGER.info("Drained all in-flight requests, starting to shutdown the router.");
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
    if (retryManagerExecutorService != null) {
      retryManagerExecutorService.shutdownNow();
    }
    if (responseAggregationExecutor != null) {
      responseAggregationExecutor.shutdownNow();
    }
    if (dnsResolveExecutor != null) {
      dnsResolveExecutor.shutdownNow();
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
          HelixUtils.connectHelixManager(manager, config.getRefreshAttemptsForZkReconnect());
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
      routersClusterManager.registerRouter(Utils.getHelixNodeIdentifier(config.getHostname(), config.getSslPort()));
      routingDataRepository.refresh();
      hybridStoreQuotaRepository.ifPresent(HelixHybridStoreQuotaRepository::refresh);

      readRequestThrottler = new ReadRequestThrottler(
          routersClusterManager,
          metadataRepository,
          routerStats.getStatsByType(RequestType.SINGLE_GET),
          config);

      // Setup read requests throttler.
      scatterGatherMode.initReadRequestThrottler(readRequestThrottler);
      setReadRequestThrottling(config.isReadThrottlingEnabled());

      if (config.getMultiKeyRoutingStrategy().equals(VeniceMultiKeyRoutingStrategy.HELIX_ASSISTED_ROUTING)) {
        /**
         * This statement should be invoked after {@link #manager} is connected.
         */
        instanceConfigRepository = new HelixInstanceConfigRepository(manager);
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
    boolean isNoopThrottlerEnabled = !throttle;
    readRequestThrottler.setIsNoopThrottlerEnabled(isNoopThrottlerEnabled);
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
