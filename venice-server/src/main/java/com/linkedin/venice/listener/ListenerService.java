package com.linkedin.venice.listener;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.log4j.Logger;

/**
 * Service that listens on configured port to accept incoming GET requests
 */
public class ListenerService extends AbstractVeniceService{
  private static final Logger logger = Logger.getLogger(ListenerService.class);

  private ServerBootstrap bootstrap;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  int port;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public ListenerService(StoreRepository storeRepository,
                         InMemoryOffsetRetriever offsetRetriever,
                         VeniceConfigLoader veniceConfigLoader,
                         MetricsRepository metricsRepository,
                         Optional<SSLEngineComponentFactory> sslFactory) {
    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    this.port = serverConfig.getListenerPort();

    //TODO: configurable worker group
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
        .childHandler(new HttpChannelInitializer(storeRepository, offsetRetriever, metricsRepository, sslFactory, serverConfig))
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
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }
}
