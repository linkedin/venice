package com.linkedin.venice.utils;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.ExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SimpleServer extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(SimpleServer.class);
  Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
  private final EventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(16);
  private final ExecutorService executorService = Executors.newFixedThreadPool(20);

  ServerBootstrap bootstrap;
  EventLoopGroup bossGroup;
  EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;

  private final int servicePort;

  boolean isInitiated;

  @Override
  public boolean startInner() {
    int maxAttempt = 100;
    long waitTime = 500;
    int retryCount = 0;
    while (true) {
      try {
        serverFuture = bootstrap.bind(servicePort).sync();
        break;
      } catch (Exception e) {
        retryCount += 1;
        if (retryCount > maxAttempt) {
          throw new VeniceException(
              "Ingestion Service is unable to bind to target port " + servicePort + " after " + maxAttempt
                  + " retries.");
        }
        LOGGER.warn("Failed to bind to port {}, will retry in {} ms.", servicePort, waitTime, e);
        Utils.sleep(waitTime);
      }
    }
    LOGGER.info("Listener service started on port: {}", servicePort);
    isInitiated = true;
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    isInitiated = false;
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }

  public SimpleServer(int servicePort) {
    this.servicePort = servicePort;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(serverSocketChannelClass)
        .childHandler(new SimpleServerChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, 1000)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }

  public boolean isInitiated() {
    return isInitiated;
  }

  public EventExecutorGroup getEventExecutorGroup() {
    return eventExecutorGroup;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public static void main(String[] args) {
    SimpleServer simpleServer = new SimpleServer(Integer.parseInt(args[0]));
    simpleServer.start();
  }

}
