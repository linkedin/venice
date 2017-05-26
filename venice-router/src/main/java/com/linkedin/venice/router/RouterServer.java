package com.linkedin.venice.router;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
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
import com.linkedin.venice.router.api.RouterHeartbeat;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceVersionFinder;
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
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class RouterServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(RouterServer.class);
  private static final long DEFAULT_MAX_ROUTER_READ_CAPCITY = 100000;

  // Immutable state
  private final int port;
  private final int sslPort;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixReadOnlyStoreRepository metadataRepository;
  private final String clusterName;
  private final List<D2Server> d2ServerList;
  private final MetricsRepository metricsRepository;
  private final int clientTimeout;
  private final int heartbeatTimeout;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final boolean sslToStorageNodes;
  private final long maxRouterReadCapacity;

  // Mutable state
  // TODO: Make these final once the test constructors are cleaned up.
  private ZkClient zkClient;
  private HelixManager manager;
  private HelixReadOnlySchemaRepository schemaRepository;

  // These are initialized in startInner()... TODO: Consider refactoring this to be immutable as well.
  private AsyncFuture<SocketAddress> serverFuture = null;
  private AsyncFuture<SocketAddress> secureServerFuture = null;
  private ResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;
  private RouterHeartbeat heartbeat;
  private ZkRoutersClusterManager routersClusterManager;

  private final static String ROUTER_SERVICE_NAME = "venice-router";
  /**
   * How many threads should be used by router for directly handling requests
   */
  private final static int ROUTER_THREADS;
  /**
   * How big should the thread pool used by the router be.  This is the number of threads used for handling
   * requests plus the threads used by the boss thread pool per bound socket (ie 1 for SSL and 1 for non-SSL)
   */
  private final static int ROUTER_THREAD_POOL_SIZE;
  static {
    int cores = Runtime.getRuntime().availableProcessors();
    ROUTER_THREADS = cores > 2 ? cores : 2;
    ROUTER_THREAD_POOL_SIZE = ROUTER_THREADS + 2;
  }
  private final static int CONNECTION_LIMIT = 10000; // TODO, configurable

  public static void main(String args[]) throws Exception {

    VeniceProperties props;
    try {
      String clusterConfigFilePath = args[0];
      props = Utils.parseProperties(clusterConfigFilePath);
    } catch (Exception e){
      throw new VeniceException("No config file parameter found", e);
    }

    String zkConnection = props.getString(ConfigKeys.ZOOKEEPER_ADDRESS);
    String clusterName = props.getString(ConfigKeys.CLUSTER_NAME);
    int port = props.getInt(ConfigKeys.ROUTER_PORT);
    int sslPort = props.getInt(ConfigKeys.ROUTER_SSL_PORT);
    int clientTimeout = props.getInt(ConfigKeys.CLIENT_TIMEOUT);
    int heartbeatTimeout = props.getInt(ConfigKeys.HEARTBEAT_TIMEOUT);

    logger.info("Zookeeper: " + zkConnection);
    logger.info("Cluster: " + clusterName);
    logger.info("Port: " + port);
    logger.info("SSL Port: " + sslPort);
    logger.info("Thread count: " + ROUTER_THREAD_POOL_SIZE);

    Optional<SSLEngineComponentFactory> sslFactory = Optional.of(SslUtils.getLocalSslFactory());
    boolean sslToStorageNodes = false;
    RouterServer server = new RouterServer(port, sslPort, clusterName, zkConnection, new ArrayList<>(), clientTimeout, heartbeatTimeout, sslFactory, sslToStorageNodes);
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

  private static MetricsRepository createMetricsRepository() {
    return TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME);
  }

  public RouterServer(int port, int sslPort, String clusterName, String zkConnection, List<D2Server> d2Servers, Optional<SSLEngineComponentFactory> sslFactory, boolean sslToStorageNodes){
    this(port, sslPort, clusterName, zkConnection, d2Servers, 10000, 1000, sslFactory, sslToStorageNodes);
  }

  public RouterServer(int port, int sslPort, String clusterName, String zkConnection, List<D2Server> d2ServerList, int clientTimeout, int heartbeatTimeout, Optional<SSLEngineComponentFactory> sslFactory, boolean sslToStorageNodes){
    this(port,
        sslPort,
        clusterName,
        zkConnection,
        d2ServerList,
        clientTimeout,
        heartbeatTimeout,
        TehutiUtils.getMetricsRepository(ROUTER_SERVICE_NAME),
        sslFactory,
        sslToStorageNodes,
        DEFAULT_MAX_ROUTER_READ_CAPCITY);
  }

  public RouterServer(int port, int sslPort, String clusterName, String zkConnection, List<D2Server> d2ServerList,
                      int clientTimeout, int heartbeatTimeout, MetricsRepository metricsRepository,
                      Optional<SSLEngineComponentFactory> sslEngineComponentFactory, boolean sslToStorageNodes,
                      long maxRouterReadCapacity) {
    this.port = port;
    this.sslPort = sslPort;
    this.clientTimeout = clientTimeout;
    this.heartbeatTimeout = heartbeatTimeout;
    this.clusterName = clusterName;
    zkClient = new ZkClient(zkConnection);
    manager = new ZKHelixManager(this.clusterName, null, InstanceType.SPECTATOR, zkConnection);

    this.metricsRepository = metricsRepository;

    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    this.metadataRepository = new HelixReadOnlyStoreRepository(zkClient, adapter, this.clusterName);
    this.schemaRepository = new HelixReadOnlySchemaRepository(this.metadataRepository,
            this.zkClient, adapter, this.clusterName);
    this.routingDataRepository = new HelixRoutingDataRepository(manager);
    this.d2ServerList = d2ServerList;
    this.sslFactory = sslEngineComponentFactory;
    this.sslToStorageNodes = sslToStorageNodes;
    this.maxRouterReadCapacity = maxRouterReadCapacity;
    verifySslOk();
  }

  /**
   * Only use this constructor for testing when you want to pass mock repositories
   *
   * TODO: This needs to be cleaned up. These constructors should be telescopic.
   *
   * Having separate constructors just for tests is hard to maintain, especially since in this case,
   * the test constructor does not initialize manager...
   *
   * @param port
   * @param clusterName
   * @param routingDataRepository
   * @param metadataRepository
   * @param schemaRepository
   * @param d2ServerList
   */
  public RouterServer(int port,
                      int sslPort,
                      String clusterName,
                      ZkClient zkClient,
                      HelixRoutingDataRepository routingDataRepository,
                      HelixReadOnlyStoreRepository metadataRepository,
                      HelixReadOnlySchemaRepository schemaRepository,
                      List<D2Server> d2ServerList,
                      Optional<SSLEngineComponentFactory> sslFactory,
                      boolean sslToStorageNodes){
    this.port = port;
    this.sslPort = sslPort;
    this.clusterName = clusterName;
    this.metadataRepository = metadataRepository;
    this.schemaRepository = schemaRepository;
    this.routingDataRepository = routingDataRepository;
    this.d2ServerList = d2ServerList;
    this.metricsRepository = new MetricsRepository();
    this.clientTimeout = 10000;
    this.heartbeatTimeout = 1000;
    this.sslFactory = sslFactory;
    this.sslToStorageNodes = sslToStorageNodes;
    this.zkClient = zkClient;
    this.maxRouterReadCapacity = DEFAULT_MAX_ROUTER_READ_CAPCITY;
    verifySslOk();
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

    Optional<SSLEngineComponentFactory> sslFactoryForRequests = sslToStorageNodes ? sslFactory : Optional.empty();
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository);
    VeniceHostHealth healthMonitor = new VeniceHostHealth();
    dispatcher = new VeniceDispatcher(healthMonitor, clientTimeout, metricsRepository, sslFactoryForRequests);
    heartbeat = new RouterHeartbeat(manager, healthMonitor, 10, TimeUnit.SECONDS, heartbeatTimeout, sslFactoryForRequests);
    heartbeat.startInner();
    MetaDataHandler metaDataHandler = new MetaDataHandler(routingDataRepository, schemaRepository, clusterName);

    ScatterGatherHelper scatterGather = ScatterGatherHelper.builder()
        .roleFinder(new VeniceRoleFinder())
        .pathParser(new VenicePathParser(new VeniceVersionFinder(metadataRepository), partitionFinder))
        .partitionFinder(partitionFinder)
        .hostFinder(new VeniceHostFinder(routingDataRepository))
        .hostHealthMonitor(healthMonitor)
        .dispatchHandler(dispatcher)
        .build();

    Router router = Router.builder(scatterGather)
        .name("VeniceRouterHttp")
        .resourceRegistry(registry)
        .executor(executor) // Executor provides the
        .bossPoolSize(1) // One boss thread to monitor the socket for new connections.  Netty only uses one thread from this pool
        .ioWorkerPoolSize(ROUTER_THREADS / 2) // While they're shared between http router and https router
        .workerExecutor(workerExecutor)
        .connectionLimit(CONNECTION_LIMIT)
        .timeoutProcessor(timeoutProcessor)
        .serverSocketOptions(serverSocketOptions)
        .beforeHttpRequestHandler(ChannelPipeline.class, (pipeline) -> {
          pipeline.addLast("MetadataHandler", metaDataHandler);
        })
        .idleTimeout(3, TimeUnit.HOURS)
        .build();

    VerifySslHandler verifySsl = new VerifySslHandler();
    Router secureRouter = Router.builder(scatterGather)
        .name("SecureVeniceRouterHttps")
        .resourceRegistry(registry)
        .executor(executor) // Executor provides the
        .bossPoolSize(1) // One boss thread to monitor the socket for new connections.  Netty only uses one thread from this pool
        .ioWorkerPoolSize(ROUTER_THREADS / 2) // While they're shared between http router and https router
        .workerExecutor(workerExecutor)
        .connectionLimit(CONNECTION_LIMIT)
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

    serverFuture = router.start(new InetSocketAddress(port));
    secureServerFuture = secureRouter.start(new InetSocketAddress(sslPort));
    serverFuture.await();
    secureServerFuture.await();

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
    routersClusterManager.unregisterCurrentRouter();
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
      routersClusterManager = new ZkRoutersClusterManager(zkClient, clusterName, Utils.getHelixNodeIdentifier(port));
      routersClusterManager.registerCurrentRouter();
      routingDataRepository.refresh();

      // Setup read requests throttler.
      ReadRequestThrottler throttler =
          new ReadRequestThrottler(routersClusterManager, metadataRepository, routingDataRepository, maxRouterReadCapacity);
      dispatcher.setReadRequestThrottler(throttler);

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
    if (this.sslToStorageNodes && !sslFactory.isPresent()){
      throw new VeniceException("Must specify an SSLEngineComponentFactory in order to use SSL in requests to storage nodes");
    }
  }

  public ZkRoutersClusterManager getRoutersClusterManager() {
    return routersClusterManager;
  }
}
