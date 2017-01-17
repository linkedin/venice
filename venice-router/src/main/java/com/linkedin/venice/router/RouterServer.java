package com.linkedin.venice.router;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
import com.linkedin.ddsstorage.base.registry.ResourceRegistry;
import com.linkedin.ddsstorage.base.registry.ShutdownableExecutors;
import com.linkedin.ddsstorage.base.registry.SyncResourceRegistry;
import com.linkedin.ddsstorage.netty3.handlers.ConnectionLimitUpstreamHandler;
import com.linkedin.ddsstorage.netty3.handlers.DefaultExecutionHandler;
import com.linkedin.ddsstorage.netty3.misc.NettyResourceRegistry;
import com.linkedin.ddsstorage.netty3.misc.ShutdownableHashedWheelTimer;
import com.linkedin.ddsstorage.netty3.misc.ShutdownableOrderedMemoryAwareExecutor;
import com.linkedin.ddsstorage.router.api.ScatterGatherHelper;
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
import com.linkedin.venice.router.stats.RouterAggStats;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import io.tehuti.metrics.MetricsRepository;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioServerBossPool;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.util.Timer;


/**
 * Note: Router uses Netty 3
 */
public class RouterServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(RouterServer.class);

  // Immutable state
  private final int port;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixReadOnlyStoreRepository metadataRepository;
  private final String clusterName;
  private final List<D2Server> d2ServerList;
  private final MetricsRepository metricsRepository;
  private final int clientTimeout;
  private final int heartbeatTimeout;

  // Mutable state
  // TODO: Make these final once the test constructors are cleaned up.
  private ZkClient zkClient;
  private HelixManager manager;
  private HelixReadOnlySchemaRepository schemaRepository;

  // These are initialized in startInner()... TODO: Consider refactoring this to be immutable as well.
  private ChannelFuture serverFuture = null;
  private NettyResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;
  private RouterHeartbeat heartbeat;

  private final static String ROUTER_SERVICE_NAME = "venice-router";

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
    int clientTimeout = props.getInt(ConfigKeys.CLIENT_TIMEOUT);
    int heartbeatTimeout = props.getInt(ConfigKeys.HEARTBEAT_TIMEOUT);

    logger.info("Zookeeper: " + zkConnection);
    logger.info("Cluster: " + clusterName);
    logger.info("Port: " + port);

    RouterServer server = new RouterServer(port, clusterName, zkConnection, new ArrayList<>(), clientTimeout, heartbeatTimeout);
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

  public RouterServer(int port, String clusterName, String zkConnection, List<D2Server> d2Servers){
    this(port, clusterName, zkConnection, d2Servers, 10000, 1000);
  }

  // TODO: Do we need both of these constructors?  If we're always going to use the jmxReporterMetricsRepo() method, then drop the explicit constructor
  public RouterServer(int port, String clusterName, String zkConnection, List<D2Server> d2ServerList, int clientTimeout, int heartbeatTimeout){
    this(port, clusterName, zkConnection, d2ServerList, clientTimeout, heartbeatTimeout, createMetricsRepository());

  }

  public RouterServer(int port, String clusterName, String zkConnection, List<D2Server> d2ServerList,
                      int clientTimeout, int heartbeatTimeout, MetricsRepository metricsRepository) {
    this.port = port;
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

    RouterAggStats.init(this.metricsRepository);
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
                      String clusterName,
                      HelixRoutingDataRepository routingDataRepository,
                      HelixReadOnlyStoreRepository metadataRepository,
                      HelixReadOnlySchemaRepository schemaRepository,
                      List<D2Server> d2ServerList){
    this.port = port;
    this.clusterName = clusterName;
    this.metadataRepository = metadataRepository;
    this.schemaRepository = schemaRepository;
    this.routingDataRepository = routingDataRepository;
    this.d2ServerList = d2ServerList;
    this.metricsRepository = new MetricsRepository();
    this.clientTimeout = 10000;
    this.heartbeatTimeout = 1000;
    RouterAggStats.init(this.metricsRepository); //TODO: re-evaluate doing a global init for the RouterAggStats class.
  }



  @Override
  public boolean startInner() throws Exception {
    metadataRepository.refresh();
    // No need to call schemaRepository.refresh() since it will do nothing.

    registry = new NettyResourceRegistry();
    ExecutorService executor = registry
        .factory(ShutdownableExecutors.class)
        .newFixedThreadPool(10, new DaemonThreadFactory("RouterThread")); //TODO: configurable number of threads
    NioServerBossPool serverBossPool = registry.register(new NioServerBossPool(executor, 1));
    //TODO: configurable workerPool size (and probably other things in this section)
    NioWorkerPool ioWorkerPool = registry.register(new NioWorkerPool(executor, 8));
    ExecutionHandler workerExecutor = new DefaultExecutionHandler(
        registry.register(new ShutdownableOrderedMemoryAwareExecutor(8, 0, 0, 60, TimeUnit.SECONDS)));
    ConnectionLimitUpstreamHandler connectionLimit = new ConnectionLimitUpstreamHandler(10000);
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    Timer idleTimer = registry.register(new ShutdownableHashedWheelTimer(1, TimeUnit.MILLISECONDS));
    Map<String, Object> serverSocketOptions = null;
    ResourceRegistry routerRegistry = registry.register(new SyncResourceRegistry());
    VenicePartitionFinder partitionFinder = new VenicePartitionFinder(routingDataRepository);
    VeniceHostHealth healthMonitor = new VeniceHostHealth();
    dispatcher = new VeniceDispatcher(healthMonitor, clientTimeout);
    heartbeat = new RouterHeartbeat(manager, healthMonitor, 10, TimeUnit.SECONDS, heartbeatTimeout);
    heartbeat.startInner();
    VeniceRouterImpl router
        = routerRegistry.register(new VeniceRouterImpl(
        "test", serverBossPool, ioWorkerPool, workerExecutor, connectionLimit, timeoutProcessor, idleTimer, serverSocketOptions,
        ScatterGatherHelper.builder()
            .roleFinder(new VeniceRoleFinder())
            .pathParser(new VenicePathParser(new VeniceVersionFinder(metadataRepository), partitionFinder))
            .partitionFinder(partitionFinder)
            .hostFinder(new VeniceHostFinder(routingDataRepository))
            .hostHealthMonitor(healthMonitor)
            .dispatchHandler(dispatcher)
            .build(),
        new MetaDataHandler(routingDataRepository, schemaRepository, clusterName)));

    serverFuture = router.start(new InetSocketAddress(port), factory -> factory);
    serverFuture.await();

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
    if (!serverFuture.cancel()){
      if (serverFuture.awaitUninterruptibly().isSuccess()){
        serverFuture.getChannel().close().awaitUninterruptibly();
      }
    }
    registry.shutdown();
    registry.waitForShutdown();
    dispatcher.close();
    //routingDataRepository.clear(); //TODO: when the clear or stop method is added to the routingDataRepository
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

      routingDataRepository.refresh();

      for (D2Server d2Server : d2ServerList) {
        logger.info("Starting d2 announcer: " + d2Server);
        d2Server.forceStart();
      }

      serviceState.set(ServiceState.STARTED);

      logger.info(this.toString() + " is started on port:" + serverFuture.getChannel().getLocalAddress());
    });
  }
}
