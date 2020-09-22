package com.linkedin.venice.ingestion;

import com.linkedin.venice.ingestion.channel.IngestionReportChannelInitializer;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

/**
 * IngestionReportListener is the listener server that handles IngestionTaskReport sent from child process.
 */
public class IngestionReportListener extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionReportListener.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  private final int applicationPort;
  private VeniceNotifier ingestionNotifier = null;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public IngestionReportListener(int applicationPort) {
    this.applicationPort = applicationPort;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
            .childHandler(new IngestionReportChannelInitializer(this))
            .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true);
  }

  public IngestionReportListener(int applicationPort, VeniceNotifier ingestionNotifier) {
    this(applicationPort);
    setIngestionNotifier(ingestionNotifier);
  }

  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(applicationPort).sync();
    logger.info("Report listener service started on port: " + applicationPort);
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

  public void setIngestionNotifier(VeniceNotifier ingestionListener) {
    this.ingestionNotifier = ingestionListener;
  }

  public VeniceNotifier getIngestionNotifier() {
    return ingestionNotifier;
  }
}
