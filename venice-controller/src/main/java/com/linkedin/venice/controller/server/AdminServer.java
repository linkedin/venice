package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.service.AbstractVeniceService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;


/**
 * Send a POST request as if the fields are form data:
 * curl -X POST --data "storename=mystore&partitions=4&replicas=1" [host]:[port]/create
 *
 * request to another path or a GET request yields an HTML form to use
 */
public class AdminServer extends AbstractVeniceService {

  private final int port;

  private ServerBootstrap bootstrap;
  private EventLoopGroup group;
  private ChannelFuture serverFuture;
  private String clusterName;
  private Admin admin;

  public AdminServer(int port, String clusterName, Admin admin){
    super("controller-admin-server");
    this.port = port;
    this.clusterName = clusterName;
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
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    group.shutdownGracefully();
    shutdown.sync();
  }

}
