package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.util.TestNettyUtil;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/30/17.
 */
public class TestChannelPoolResolver {
  @Test(groups = "unit")
  public void testBasicDnsResolver() throws InterruptedException {
    BasicDnsResolver resolver = new BasicDnsResolver();

    Future<InetSocketAddress> result = resolver
        .resolve(InetSocketAddress.createUnresolved("localhost", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "localhost");
    Assert.assertEquals(result.getNow().getPort(), 80);
    Assert.assertEquals(result.getNow().getAddress().getHostAddress(), "127.0.0.1");

    result = resolver
        .resolve(InetSocketAddress.createUnresolved("google.com", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "google.com");
    Assert.assertEquals(result.getNow().getPort(), 80);

    result = resolver.resolve(
        InetSocketAddress.createUnresolved("unresolved.linkedin.com", 80),
        ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertTrue(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "unresolved.linkedin.com");
    Assert.assertEquals(result.getNow().getPort(), 80);
  }

  @Test(groups = "unit")
  public void testNettyDnsResolverNIO() throws InterruptedException {
    NioEventLoopGroup eventLoop = new NioEventLoopGroup(4);
    try {
      testNettyDnsResolver(new NettyDnsResolver(NioDatagramChannel.class, eventLoop));
    } finally {
      eventLoop.shutdownGracefully().sync();
    }
  }

  @Test(groups = "unit")
  public void testNettyDnsResolverEPOLL() throws InterruptedException {
    EpollEventLoopGroup eventLoop = TestNettyUtil.skipEpollIfNotFound(() -> new EpollEventLoopGroup(4));
    try {
      testNettyDnsResolver(new NettyDnsResolver(EpollDatagramChannel.class, eventLoop));
    } finally {
      eventLoop.shutdownGracefully().sync();
    }
  }

  private void testNettyDnsResolver(NettyDnsResolver resolver) throws InterruptedException {
    Future<InetSocketAddress> result = resolver
        .resolve(InetSocketAddress.createUnresolved("localhost", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "localhost");
    Assert.assertEquals(result.getNow().getPort(), 80);
    Assert.assertEquals(result.getNow().getAddress().getHostAddress(), "127.0.0.1");

    result = resolver
        .resolve(InetSocketAddress.createUnresolved("google.com", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "google.com");
    Assert.assertEquals(result.getNow().getPort(), 80);

    result = resolver.resolve(
        InetSocketAddress.createUnresolved("unresolved.linkedin.com", 80),
        ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isDone());
    Assert.assertFalse(result.isSuccess());
    Assert.assertTrue(result.cause().getMessage().startsWith("Failed to resolve 'unresolved.linkedin.com'"));
  }

}
