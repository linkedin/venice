package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import org.apache.log4j.Logger;


/**
 * Send a POST request as if the fields are form data:
 * curl -X POST --data "storename=mystore&partitions=4&replicas=1" [host]:[port]/create
 *
 * request to another path or a GET request yields an HTML form to use
 */
public class AdminServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminServer.class);

  private final int port;

  private ServerBootstrap bootstrap;
  private EventLoopGroup group;
  private ChannelFuture serverFuture;
  private final String clusterName;
  private final Admin admin;

  public AdminServer(int port, String clusterName, Admin admin){
    super("controller-admin-server");
    this.port = port;
    this.clusterName = clusterName;
    //Note: admin is passed in as a reference.  The expectation is the source of the admin will
    //      close it so we don't close it in stopInner()
    this.admin = admin;
  }

  @Override
  public void startInner()
      throws Exception {
    group = new NioEventLoopGroup();

    bootstrap = new ServerBootstrap();
    bootstrap.group(group)
        .channel(NioServerSocketChannel.class).localAddress(new InetSocketAddress(port))
        .childHandler(new Initializer(clusterName, admin, null));
    serverFuture = bootstrap.bind().sync();
    logger.info("Admin server is started on port:"+port);
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    group.shutdownGracefully();
    shutdown.sync();
    logger.info("Admin Server is stoped on port:"+port);
  }

}
