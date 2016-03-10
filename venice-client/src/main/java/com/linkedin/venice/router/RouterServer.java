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
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceRoleFinder;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.service.AbstractVeniceService;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private final String clusterName;
  private RoutingDataRepository routingDataRepository;
  private MetadataRepository metadataRepository;

  private ChannelFuture serverFuture = null;
  private NettyResourceRegistry registry = null;

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
    ExecutorService executor = registry.factory(ShutdownableExecutors.class).newCachedThreadPool();
    NioServerBossPool serverBossPool = registry.register(new NioServerBossPool(executor, 1));
    NioWorkerPool ioWorkerPool = registry.register(new NioWorkerPool(executor, 8));
    ExecutionHandler workerExecutor = new DefaultExecutionHandler(
        registry.register(new ShutdownableOrderedMemoryAwareExecutor(8, 0, 0, 60, TimeUnit.SECONDS)));
    ConnectionLimitUpstreamHandler connectionLimit = new ConnectionLimitUpstreamHandler(10000);
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    Timer idleTimer = registry.register(new ShutdownableHashedWheelTimer(1, TimeUnit.MILLISECONDS));
    Map<String, Object> serverSocketOptions = null;

    ScheduledExecutorService scheduledExecutorService
        = registry.factory(ShutdownableExecutors.class).newScheduledThreadPool(1);

    ResourceRegistry routerRegistry = registry.register(new SyncResourceRegistry());

    RouterImpl router
        = routerRegistry.register(new RouterImpl(
        "test", serverBossPool, ioWorkerPool, workerExecutor, connectionLimit, timeoutProcessor, idleTimer, serverSocketOptions,
        ScatterGatherHelper.builder()
            .roleFinder(new VeniceRoleFinder())
            .pathParser(new VenicePathParser(new VeniceVersionFinder(metadataRepository)))
            .partitionFinder(new VenicePartitionFinder(routingDataRepository))
            .hostFinder(new VeniceHostFinder(routingDataRepository))
            .dispatchHandler(new VeniceDispatcher())
            .build()));

    serverFuture = router.start(new InetSocketAddress(port), factory -> factory);
    serverFuture.await();
    logger.info("Router server is started on port:"+port);
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
    logger.info("Router Server is stoped");
  }
}
