package com.linkedin.venice.listener;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.MetadataRetriever;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * Service that listens on configured port to accept incoming GET requests
 */
public class ListenerService extends AbstractVeniceService{
  private static final Logger logger = Logger.getLogger(ListenerService.class);

  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  private final int port;

  private final VeniceServerConfig serverConfig;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public ListenerService(StoreRepository storeRepository,
                         ReadOnlyStoreRepository storeMetadataRepository,
                         ReadOnlySchemaRepository schemaRepo,
                         CompletableFuture<RoutingDataRepository> routingRepository,
                         MetadataRetriever metadataRetriever,
                         VeniceConfigLoader veniceConfigLoader,
                         MetricsRepository metricsRepository,
                         Optional<SSLEngineComponentFactory> sslFactory,
                         Optional<StaticAccessController> accessController) {
    serverConfig = veniceConfigLoader.getVeniceServerConfig();
    this.port = serverConfig.getListenerPort();

    //TODO: configurable worker group
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(serverConfig.getNettyWorkerThreadCount()); //if 0, defaults to 2*cpu count

    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
        .childHandler(new HttpChannelInitializer(storeRepository, storeMetadataRepository, schemaRepo, routingRepository,
            metadataRetriever, metricsRepository, sslFactory, serverConfig, accessController))
        .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }


  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    logger.info("Listener service started on port: " + port);

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    /**
     * Netty shutdown gracefully is NOT working well for us since it will close all the connections right away.
     * By sleeping the configured period, Storage Node could serve all the requests, which are already received.
     *
     * Since Storage Node will stop {@link com.linkedin.venice.helix.HelixParticipationService}
     * (disconnect from Zookeeper, which makes it unavailable in Venice Router) before Netty,
     * there shouldn't be a lot of requests coming during this grace period, otherwise, we need to tune the config.
     */
    Thread.sleep(TimeUnit.SECONDS.toMillis(serverConfig.getNettyGracefulShutdownPeriodSeconds()));
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }
}
