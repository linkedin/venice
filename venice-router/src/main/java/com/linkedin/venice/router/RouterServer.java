package com.linkedin.venice.router;

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
import com.linkedin.ddsstorage.router.impl.RouterImpl;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Props;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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

  private final int port;
  private ZkClient zkClient;
  private HelixManager manager;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixCachedMetadataRepository metadataRepository;
  private String clusterName;

  private ChannelFuture serverFuture = null;
  private NettyResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;

  public static void main(String args[]) throws Exception {

    Props props;
    try {
      String clusterConfigFilePath = args[0];
      props = new Props(new File(clusterConfigFilePath));
    } catch (Exception e){
      throw new VeniceException("No config file parameter found", e);
    }

    String zkConnection = props.get(ConfigKeys.ZOOKEEPER_ADDRESS);
    String clusterName = props.get(ConfigKeys.CLUSTER_NAME);
    int port = props.getInt(ConfigKeys.ROUTER_PORT);

    logger.info("Zookeeper: " + zkConnection);
    logger.info("Cluster: " + clusterName);
    logger.info("Port: " + port);

    RouterServer server = new RouterServer(port, clusterName, zkConnection);
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

  public RouterServer(int port, String clusterName, String zkConnection){
    super(RouterServer.class.getName());
    this.port = port;
    this.clusterName = clusterName;
    zkClient = new ZkClient(zkConnection);
    manager = new ZKHelixManager(this.clusterName, null, InstanceType.SPECTATOR, zkConnection);
    try {
      manager.connect();
    } catch (Exception e) {
      throw new VeniceException("Failed to start manager when creating Venice Router", e);
    }
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    this.metadataRepository = new HelixCachedMetadataRepository(zkClient, adapter, this.clusterName);
    this.routingDataRepository = new HelixRoutingDataRepository(manager);
  }

  //Only use this constructor for testing when you want to pass mock repositories
  RouterServer(int port, String clusterName, HelixRoutingDataRepository routingDataRepository,
      HelixCachedMetadataRepository metadataRepository){
    super(RouterServer.class.getName());
    this.port = port;
    this.clusterName = clusterName;
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
  }

  @Override
  public void startInner() throws Exception {

    metadataRepository.refresh();
    routingDataRepository.refresh();

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
    dispatcher = new VeniceDispatcher(healthMonitor);

    RouterImpl router
        = routerRegistry.register(new RouterImpl(
        "test", serverBossPool, ioWorkerPool, workerExecutor, connectionLimit, timeoutProcessor, idleTimer, serverSocketOptions,
        ScatterGatherHelper.builder()
            .roleFinder(new VeniceRoleFinder())
            .pathParser(new VenicePathParser(new VeniceVersionFinder(metadataRepository), partitionFinder))
            .partitionFinder(partitionFinder)
            .hostFinder(new VeniceHostFinder(routingDataRepository))
            .hostHealthMonitor(healthMonitor)
            .dispatchHandler(dispatcher)
            .build()));

    serverFuture = router.start(new InetSocketAddress(port), factory -> factory);
    serverFuture.await();

    logger.info("Router server is started on port:" + serverFuture.getChannel().getLocalAddress());
  }

  @Override
  public void stopInner() throws Exception {
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
    logger.info("Router Server is stopped");
  }
}
