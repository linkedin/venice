package com.linkedin.alpini.netty4.handlers;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Abhishek Andhavarapu
 */
@Test(groups = "unit")
public class TestAllChannelsHandler {
  @Test
  public void testLocalChannel() throws InterruptedException {
    DefaultEventLoopGroup group = new DefaultEventLoopGroup(1);
    LocalAddress addr = new LocalAddress("test");
    AllChannelsHandler allChannelsHandler = new AllChannelsHandler();

    ServerBootstrap serverBootstrap = new ServerBootstrap().channel(LocalServerChannel.class)
        .group(group)
        .localAddress(addr)
        .childHandler(allChannelsHandler);

    Channel server = serverBootstrap.bind().sync().channel();
    Assert.assertTrue(server.isRegistered());

    Bootstrap clientBootstrap = new Bootstrap().channel(LocalChannel.class)
        .group(group)
        .remoteAddress(addr)
        .handler(new SimpleChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) {
          }
        });

    Channel client = clientBootstrap.connect().sync().channel();

    String msg = "Hello World";

    client.writeAndFlush(msg).sync();

    // Assert channel is added to the Active HTTP2 connection.
    Assert.assertEquals(allChannelsHandler.size(), 1);

    client.close().sync();
    server.close().sync();

    group.shutdownGracefully().await();

    // Assert channel is removed from the active HTTP/2 connections.
    Assert.assertEquals(allChannelsHandler.size(), 0);
  }
}
