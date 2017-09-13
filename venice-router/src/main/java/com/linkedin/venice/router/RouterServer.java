package com.linkedin.venice.router;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
import com.linkedin.ddsstorage.base.concurrency.impl.SuccessAsyncFuture;
import com.linkedin.ddsstorage.base.registry.ResourceRegistry;
import com.linkedin.ddsstorage.base.registry.ShutdownableExecutors;
import com.linkedin.ddsstorage.router.api.ScatterGatherHelper;
import com.linkedin.ddsstorage.router.impl.Router;
import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterHeartbeat;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VeniceMetricsProvider;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.ChannelPipeline;
import io.tehuti.metrics.MetricsRepository;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class RouterServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(RouterServer.class);

  // Immutable state
  private final List<D2Server> d2ServerList;
  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;
  private final Optional<SSLEngineComponentFactory> sslFactory;

  private final VeniceRouterConfig config;

  // Mutable state
  // TODO: Make these final once the test constructors are cleaned up.
  private ZkClient zkClient;
  private HelixManager manager;
  private HelixReadOnlySchemaRepository schemaRepository;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixReadOnlyStoreRepository metadataRepository;

  // These are initialized in startInner()... TODO: Consider refactoring this to be immutable as well.
  private AsyncFuture<SocketAddress> serverFuture = null;
  private AsyncFuture<SocketAddress> secureServerFuture = null;
  private ResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;
  private RouterHeartbeat heartbeat;
  private VeniceDelegateMode scatterGatherMode;
  private  HelixAdapterSerializer adapter;
  private ZkRoutersClusterManager routersClusterManager;
  private Router router;
  private Router secureRouter;

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
    RouterServer server = new RouterServer(props, new ArrayList<>(), sslFactory);
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

  public RouterServer(VeniceProperties properties, List<D2Server> d2Servers,
      Optional<SSLEngineComponentFactory> sslEngineComponentFactory) {
    this(properties, d2Servers, sslEngineComponentFactory, TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME));
  }

  public RouterServer(
      VeniceProperties properties,
      List<D2Server> d2ServerList,
      Optional<SSLEngineComponentFactory> sslFactory,
      MetricsRepository metricsRepository) {
    this(properties, d2ServerList, sslFactory, metricsRepository, true);
    this.metadataRepository = new HelixReadOnlyStoreRepository(zkClient, adapter, config.getClusterName());
    this.schemaRepository =
        new HelixReadOnlySchemaRepository(this.metadataRepository, this.zkClient, adapter, config.getClusterName());
    this.routingDataRepository = new HelixRoutingDataRepository(manager);
  }

  /**
   * CTOR maintain the common logic for both test and normal CTOR.
   */
  private RouterServer(
      VeniceProperties properties,
      List<D2Server> d2ServerList,
      Optional<SSLEngineComponentFactory> sslFactory,
      MetricsRepository metricsRepository, boolean isCreateHelixManager) {
    config = new VeniceRouterConfig(properties);
    zkClient = new ZkClient(config.getZkConnection());
    this.adapter = new HelixAdapterSerializer();
    if(isCreateHelixManager) {
      this.manager = new ZKHelixManager(config.getClusterName(), null, InstanceType.SPECTATOR, config.getZkConnection());
    }

    this.statsForSingleGet = new AggRouterHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);
    this.statsForMultiGet = new AggRouterHttpRequestStats(metricsRepository, RequestType.MULTI_GET);

    this.d2ServerList = d2ServerList;
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
      List<D2Server> d2ServerList,
      Optional<SSLEngineComponentFactory> sslFactory){
    this(properties, d2ServerList, sslFactory, new MetricsRepository(), false);
    this.routingDataRepository = routingDataRepository;
    this.metadataRepository = metadataRepository;
    this.schemaRepository = schemaRepository;
  }



  @Override
  public boolean startInner() throws Exception {
    metadataRepository.refresh();
    // No need to call schemaRepository.refresh() since it will do nothing.
    registry = new ResourceRegistry();
    ExecutorService executor = registry
        .factory(ShutdownableExecutors.class)
        .newFixedThreadPool(ROUTER_THREAD_POOL_SIZE, new DaemonThreadFactory("RouterThread")); //TODO: configurable number of threads
    Executor workerExecutor = registry.factory(ShutdownableExecutors.class).newCachedThreadPool();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    Map<String, Object> serverSocketOptions = null;

    Optional<SSLEngineComponentFactory> sslFactoryForRequests = config.isSslToStorageNodes()? sslFactory : Optional.empty();
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository);
    VeniceHostHealth healthMonitor = new VeniceHostHealth();
    scatterGatherMode = new VeniceDelegateMode();
    dispatcher = new VeniceDispatcher(config, healthMonitor, sslFactoryForRequests);
    heartbeat = new RouterHeartbeat(manager, healthMonitor, 10, TimeUnit.SECONDS, config.getHeartbeatTimeoutMs(), sslFactoryForRequests);
    heartbeat.startInner();
    MetaDataHandler metaDataHandler = new MetaDataHandler(routingDataRepository, schemaRepository, config.getClusterName());
    VenicePathParser pathParser = new VenicePathParser(new VeniceVersionFinder(metadataRepository), partitionFinder,
        statsForSingleGet, statsForMultiGet, config.getMaxKeyCountInMultiGetReq());
    // Setup stat tracking for exceptional case
    RouterExceptionAndTrackingUtils.setStatsForSingleGet(statsForSingleGet);
    RouterExceptionAndTrackingUtils.setStatsForMultiGet(statsForMultiGet);

    // Fixed retry future
    AsyncFuture<LongSupplier> retryFuture = new SuccessAsyncFuture<>(() -> config.getLongTailRetryThresholdMs());

    ScatterGatherHelper scatterGather = ScatterGatherHelper.builder()
        .roleFinder(new VeniceRoleFinder())
        .pathParserExtended(pathParser)
        .partitionFinder(partitionFinder)
        .hostFinder(new VeniceHostFinder(routingDataRepository))
        .hostHealthMonitor(healthMonitor)
        .dispatchHandler(dispatcher)
        .scatterMode(scatterGatherMode)
        .responseAggregatorFactory(new VeniceResponseAggregator(statsForSingleGet, statsForMultiGet))
        .metricsProvider(new VeniceMetricsProvider())
        /**
         * TODO: need to figure out a proper way to enable long-tail retry
         */
        //.longTailRetrySupplier((resourceName, methodName) -> retryFuture) // TODO: add metric to track retries
        .build();

    router = Router.builder(scatterGather)
        .name("VeniceRouterHttp")
        .resourceRegistry(registry)
        .executor(executor) // Executor provides the
        .bossPoolSize(ROUTER_BOSS_THREAD_NUM) // One boss thread to monitor the socket for new connections.  Netty only uses one thread from this pool
        .ioWorkerPoolSize(ROUTER_IO_THREAD_NUM)  // While they're shared between http router and https router
        .workerExecutor(workerExecutor)
        .connectionLimit(config.getConnectionLimit())
        .timeoutProcessor(timeoutProcessor)
        .serverSocketOptions(serverSocketOptions)
        .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
          pipeline.addLast("MetadataHandler", metaDataHandler);
        })
        .idleTimeout(3, TimeUnit.HOURS)
        .build();

    VerifySslHandler verifySsl = new VerifySslHandler();
    secureRouter = Router.builder(scatterGather)
        .name("SecureVeniceRouterHttps")
        .resourceRegistry(registry)
        .executor(executor) // Executor provides the
        .bossPoolSize(ROUTER_BOSS_THREAD_NUM) // One boss thread to monitor the socket for new connections.  Netty only uses one thread from this pool
        .ioWorkerPoolSize(ROUTER_IO_THREAD_NUM) // While they're shared between http router and https router
        .workerExecutor(workerExecutor)
        .connectionLimit(config.getConnectionLimit())
        .timeoutProcessor(timeoutProcessor)
        .serverSocketOptions(serverSocketOptions)
        .beforeHttpServerCodec(ChannelPipeline.class, (pipeline) -> {
          if (sslFactory.isPresent()) {
            pipeline.addFirst("SSL Initializer", new SSLInitializer(sslFactory.get()));
          }
        })
        .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
          pipeline.addLast("SSL Verifier", verifySsl);
          pipeline.addLast("MetadataHandler", metaDataHandler);
        })
        .idleTimeout(3, TimeUnit.HOURS)
        .build();

    asyncStart();

    // The start up process is not finished yet, because it is continuing asynchronously.
    return false;
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
    if (!serverFuture.cancel(false)){
      serverFuture.awaitUninterruptibly();
    }
    if (!secureServerFuture.cancel(false)){
      secureServerFuture.awaitUninterruptibly();
    }
    registry.shutdown();
    registry.waitForShutdown();
    routersClusterManager.unregisterRouter(Utils.getHelixNodeIdentifier(config.getPort()));
    routersClusterManager.clear();
    dispatcher.close();
    routingDataRepository.clear();
    metadataRepository.clear();
    if (manager != null) {
      manager.disconnect();
    }
    if (zkClient != null) {
      zkClient.close();
    }
    heartbeat.stopInner();
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

        System.exit(1);
      }
      // Register current router into ZK.
      routersClusterManager = new ZkRoutersClusterManager(zkClient, adapter, config.getClusterName());
      routersClusterManager.refresh();
      routersClusterManager.registerRouter(Utils.getHelixNodeIdentifier(config.getPort()));
      routingDataRepository.refresh();

      // Setup read requests throttler.
      ReadRequestThrottler throttler =
          new ReadRequestThrottler(routersClusterManager, metadataRepository, routingDataRepository,
              config.getMaxReadCapacityCu(), statsForSingleGet);
      scatterGatherMode.initReadRequestThrottler(throttler);

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
        logger.info(this.toString() + " started on port: " + ((InetSocketAddress) serverFuture.get()).getPort()
            + " and ssl port: " + ((InetSocketAddress) secureServerFuture.get()).getPort());
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
}
