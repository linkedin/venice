package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

/**
 * Service that listens on configured port to accept incoming GET requests
 */
public class ListenerService extends AbstractVeniceService{
  private static final Logger logger = Logger.getLogger(ListenerService.class.getName());

  private ServerBootstrap bootstrap;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  int port;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public ListenerService(StoreRepository storeRepository, VeniceConfigService veniceConfigService) {
    super("listener-service");
    this.port = Integer.parseInt(veniceConfigService.getVeniceServerConfig().getListenerPort());

    //TODO: configurable worker group
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
        .childHandler(new BinaryChannelInitializer(storeRepository))  // For Binary TCP reads
        //.childHandler(new HttpChannelInitializer(storeRepository))    // For HTTP reads (GET /store/key/partition)
        .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }


  @Override
  public void startInner() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    logger.info("Listener service started on port: " + port);
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }

}
