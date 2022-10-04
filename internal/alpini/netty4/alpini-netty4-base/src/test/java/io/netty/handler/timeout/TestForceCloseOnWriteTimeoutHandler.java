package io.netty.handler.timeout;

import com.linkedin.alpini.netty4.handlers.Http2ExceptionHandler;
import com.linkedin.alpini.netty4.handlers.SimpleChannelInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests ForceCloseOnWriteTimeoutHandler and Http2ExceptionHandler.
 *
 * @author Abhishek Andhavarapu
 */
public class TestForceCloseOnWriteTimeoutHandler {
  @Test(groups = "unit")
  public void testWriteTimeOut() throws Exception {
    DefaultEventLoopGroup group = new DefaultEventLoopGroup(2);
    LocalAddress addr = new LocalAddress("test");

    final ScheduledExecutorService execService = Executors.newSingleThreadScheduledExecutor();
    CompletableFuture<Object> message = new CompletableFuture<>();
    CompletableFuture<Channel> localServerChannel = new CompletableFuture<>();

    ServerBootstrap serverBootstrap = new ServerBootstrap().channel(LocalServerChannel.class)
        .group(group)
        .localAddress(addr)
        .childHandler(new SimpleChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) {
            localServerChannel.complete(ch);
            EventExecutorGroup executorGroup = NettyUtils.executorGroup(ch);
            ch.pipeline().addLast(executorGroup, new ChannelOutboundHandlerAdapter() {
              // Like SSL Handler, the write are blocked in the queue.
              @Override
              public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                // Do nothing
              }
            }, new ForceCloseOnWriteTimeoutHandler(1) {
              @Override
              protected void writeTimedOut(ChannelHandlerContext ctx) throws Exception {
                super.writeTimedOut(ctx);
                message.complete("Hello World");
              }
            }, Http2ExceptionHandler.INSTANCE, new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                // Write back to the client
                ctx.write("Hello back");
              }
            });
          }
        });

    Channel server = serverBootstrap.bind().sync().channel();

    CountDownLatch latch = new CountDownLatch(1);

    Bootstrap clientBootstrap = new Bootstrap().channel(LocalChannel.class)
        .group(group)
        .remoteAddress(addr)
        .handler(new SimpleChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) {
            addAfter(ch, new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                latch.countDown();
                super.channelRead(ctx, msg);
              }
            });
          }
        });

    Channel client = clientBootstrap.connect().sync().channel();
    client.config().setAutoRead(false);

    // Write a message to server
    client.writeAndFlush("Hello World").sync();
    Assert.assertNotNull(message.get(20000, TimeUnit.MILLISECONDS));
    // Timeout causes the server channel to be closed.
    Assert.assertFalse(localServerChannel.get().isOpen());

    // Close
    client.close().sync();
    server.close().sync();
    group.shutdownGracefully().await();
    execService.shutdown();
    execService.awaitTermination(10, TimeUnit.SECONDS);
  }
}
