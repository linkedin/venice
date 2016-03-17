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
import com.linkedin.venice.helix.HelixMetadataRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
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
 *
 * For now this is meant to be run from the IDE for development and testing.
 * It won't actually work until the metadata repository starts to make versions 'active'
 */
public class RouterServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(RouterServer.class);

  private final int port;
  private RoutingDataRepository routingDataRepository;
  private MetadataRepository metadataRepository;
  private String clusterName;

  private ChannelFuture serverFuture = null;
  private NettyResourceRegistry registry = null;
  private VeniceDispatcher dispatcher;

  /***
   * This main method is not meant to be the way of invoking the router for a deployment.  It is only provided as a
   * convenience method for developement and will eventually be replaced with a more standard invokation process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String args[])
      throws Exception {

    Props props;
    try {
      String clusterConfigFilePath = args[0];
      props = new Props(new File(clusterConfigFilePath));
    } catch (Exception e){
      logger.warn("No config file parameter found, using default values for local testing", e);
      props = new Props();
    }

    String zkConnection = props.getOrDefault("zookeeper.connection.string", "localhost:2181");
    String clusterName = props.getOrDefault("cluster.name", "localhost:2181");
    int port = props.getInt("router.port", 54333);

    ZkClient zkClient = new ZkClient(zkConnection);
    HelixManager manager = new ZKHelixManager(clusterName, null, InstanceType.SPECTATOR, zkConnection);

    MetadataRepository metaRepo = new HelixMetadataRepository(zkClient, clusterName);
    RoutingDataRepository routingRepo = new HelixRoutingDataRepository(manager);

    RouterServer server = new RouterServer(port, clusterName, routingRepo, metaRepo);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server.isStarted()) {
          try {
            server.stop();
            manager.disconnect();
            zkClient.close();
          } catch (Exception e) {
            logger.error("Error shutting the server. ", e);
          }
        }
      }
    });


  }

  public RouterServer(int port, String clusterName,
      RoutingDataRepository routingDataRepository, MetadataRepository metadataRepository){
    super(RouterServer.class.getName());
    this.port = port;
    this.clusterName = clusterName;
    this.routingDataRepository = routingDataRepository;
    this.metadataRepository = metadataRepository;
  }

  @Override
  public void startInner()
      throws Exception {
    registry = new NettyResourceRegistry();
    ExecutorService executor = registry
        .factory(ShutdownableExecutors.class)
        .newFixedThreadPool(8, new DaemonThreadFactory("RouterThread")); //TODO: configurable number of threads
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
    dispatcher = new VeniceDispatcher();

    RouterImpl router
        = routerRegistry.register(new RouterImpl(
        "test", serverBossPool, ioWorkerPool, workerExecutor, connectionLimit, timeoutProcessor, idleTimer, serverSocketOptions,
        ScatterGatherHelper.builder()
            .roleFinder(new VeniceRoleFinder())
            .pathParser(new VenicePathParser(new VeniceVersionFinder(metadataRepository)))
            .partitionFinder(new VenicePartitionFinder(routingDataRepository))
            .hostFinder(new VeniceHostFinder(routingDataRepository))
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
    logger.info("Router Server is stopped");
  }
}
