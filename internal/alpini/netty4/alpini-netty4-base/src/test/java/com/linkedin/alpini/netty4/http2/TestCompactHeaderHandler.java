package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Abhishek Andhavarapu
 */
@Test(groups = "unit")
public class TestCompactHeaderHandler {
  @Test
  public void testCompactHeader() throws Exception {
    DefaultEventLoopGroup group = new DefaultEventLoopGroup(1);
    LocalAddress addr = new LocalAddress("test");

    ServerBootstrap serverBootstrap = new ServerBootstrap().channel(LocalServerChannel.class)
        .group(group)
        .localAddress(addr)
        .childHandler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) {
            ch.pipeline().addLast(new CompactHeaderHandler(Header.class, true), new ChannelDuplexHandler() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof HttpMessage) {
                  HttpRequest request = (HttpRequest) msg;
                  // Assert the compact header is removed
                  Assert.assertFalse(request.headers().contains(CompactHeaderHandler.COMPACT_HEADER));
                  // Assert the correct headers are set
                  Assert.assertTrue(request.headers().contains(Header.ESPRESSO_SERVED_BY));
                  Assert.assertEquals(request.headers().get(Header.ESPRESSO_SERVED_BY), "localhost");
                }
              }
            });
          }
        });

    Channel server = serverBootstrap.bind().sync().channel();
    Assert.assertTrue(server.isRegistered());

    Bootstrap clientBootstrap = new Bootstrap().channel(LocalChannel.class)
        .group(group)
        .remoteAddress(addr)
        .handler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) {
            ch.pipeline().addLast(new ChannelDuplexHandler() {
              @Override
              public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                if (msg instanceof HttpRequest) {
                  HttpRequest request = (HttpRequest) msg;
                  Assert.assertTrue(request.headers().contains(CompactHeaderHandler.COMPACT_HEADER));
                  // Assert the other headers are removed.
                  Assert.assertFalse(request.headers().contains(Header.ESPRESSO_SERVED_BY));
                }
                super.write(ctx, msg, promise);
              }
            }, new CompactHeaderHandler(Header.class, false));
          }
        });

    Channel client = clientBootstrap.connect().sync().channel();

    String checkUri = "/testDB/testTable/1";
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, checkUri, false, 0L, 0L);
    request.headers().add(Header.ESPRESSO_SERVED_BY, "localhost");

    client.writeAndFlush(request).sync();

    client.close().sync();
    server.close().sync();

    group.shutdownGracefully().await();
  }

  private static final class Header {
    public static final String ESPRESSO_SERVED_BY = "X-ESPRESSO-Served-By";
  }
}
