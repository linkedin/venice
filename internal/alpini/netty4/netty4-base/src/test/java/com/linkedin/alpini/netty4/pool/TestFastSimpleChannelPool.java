package com.linkedin.alpini.netty4.pool;

import static org.testng.AssertJUnit.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 *
 * Forked from Netty's SimplehannelPoolTest
 *
 */
public class TestFastSimpleChannelPool {
  private static final String LOCAL_ADDR_ID = "test.id";

  static class CountingChannelPoolHandler implements ChannelPoolHandler {
    private final AtomicInteger channelCount = new AtomicInteger(0);
    private final AtomicInteger acquiredCount = new AtomicInteger(0);
    private final AtomicInteger releasedCount = new AtomicInteger(0);

    @Override
    public void channelCreated(Channel ch) {
      channelCount.incrementAndGet();
    }

    @Override
    public void channelReleased(Channel ch) {
      releasedCount.incrementAndGet();
    }

    @Override
    public void channelAcquired(Channel ch) {
      acquiredCount.incrementAndGet();
    }

    public int channelCount() {
      return channelCount.get();
    }

    public int acquiredCount() {
      return acquiredCount.get();
    }

    public int releasedCount() {
      return releasedCount.get();
    }
  }

  @Test(groups = "unit")
  public void testAcquire() throws Exception {
    EventLoopGroup group = new DefaultEventLoopGroup();
    LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
    Bootstrap cb = new Bootstrap();
    cb.remoteAddress(addr);
    cb.group(group).channel(LocalChannel.class);

    ServerBootstrap sb = new ServerBootstrap();
    sb.group(group).channel(LocalServerChannel.class).childHandler(new ChannelInitializer<LocalChannel>() {
      @Override
      public void initChannel(LocalChannel ch) throws Exception {
        ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
      }
    });

    // Start server
    Channel sc = sb.bind(addr).sync().channel();
    CountingChannelPoolHandler handler = new CountingChannelPoolHandler();

    ChannelPool pool = new FastSimpleChannelPool(cb, handler);

    Channel channel = pool.acquire().sync().getNow();

    pool.release(channel).syncUninterruptibly();

    Channel channel2 = pool.acquire().sync().getNow();
    Assert.assertSame(channel, channel2);
    Assert.assertEquals(1, handler.channelCount());
    pool.release(channel2).syncUninterruptibly();

    // Should fail on multiple release calls.
    try {
      pool.release(channel2).syncUninterruptibly();
      fail();
    } catch (IllegalArgumentException e) {
      // expected
      assertFalse(channel.isActive());
    }

    assertEquals(2, handler.acquiredCount());
    assertEquals(2, handler.releasedCount());

    sc.close().sync();
    group.shutdownGracefully();
  }

  @Test(groups = "unit")
  public void testBoundedChannelPoolSegment() throws Exception {
    EventLoopGroup group = new DefaultEventLoopGroup();
    LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
    Bootstrap cb = new Bootstrap();
    cb.remoteAddress(addr);
    cb.group(group).channel(LocalChannel.class);

    ServerBootstrap sb = new ServerBootstrap();
    sb.group(group).channel(LocalServerChannel.class).childHandler(new ChannelInitializer<LocalChannel>() {
      @Override
      public void initChannel(LocalChannel ch) throws Exception {
        ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
      }
    });

    // Start server
    Channel sc = sb.bind(addr).sync().channel();
    CountingChannelPoolHandler handler = new CountingChannelPoolHandler();

    ChannelPool pool = new FastSimpleChannelPool(cb, handler, ChannelHealthChecker.ACTIVE) {
      private final Queue<Channel> queue = new LinkedBlockingQueue<Channel>(1);

      @Override
      protected Channel pollChannel() {
        return queue.poll();
      }

      @Override
      protected boolean offerChannel(Channel ch) {
        return queue.offer(ch);
      }
    };

    Channel channel = pool.acquire().sync().getNow();
    Channel channel2 = pool.acquire().sync().getNow();

    pool.release(channel).syncUninterruptibly().getNow();
    try {
      pool.release(channel2).syncUninterruptibly();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
    channel2.close().sync();

    assertEquals(2, handler.channelCount());
    assertEquals(2, handler.acquiredCount());
    assertEquals(2, handler.releasedCount()); // release is now called before offerChannel()
    sc.close().sync();
    channel.close().sync();
    channel2.close().sync();
    group.shutdownGracefully();
  }

  /**
   * Tests that if channel was unhealthy it is not offered back to the pool.
   *
   * @throws Exception
   */
  @Test(groups = "unit")
  public void testUnhealthyChannelIsNotOffered() throws Exception {
    EventLoopGroup group = new DefaultEventLoopGroup();
    LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
    Bootstrap cb = new Bootstrap();
    cb.remoteAddress(addr);
    cb.group(group).channel(LocalChannel.class);

    ServerBootstrap sb = new ServerBootstrap();
    sb.group(group).channel(LocalServerChannel.class).childHandler(new ChannelInitializer<LocalChannel>() {
      @Override
      public void initChannel(LocalChannel ch) throws Exception {
        ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
      }
    });

    // Start server
    Channel sc = sb.bind(addr).syncUninterruptibly().channel();
    ChannelPoolHandler handler = new CountingChannelPoolHandler();
    ChannelPool pool = new FastSimpleChannelPool(cb, handler);
    Channel channel1 = pool.acquire().syncUninterruptibly().getNow();
    pool.release(channel1).syncUninterruptibly();
    Channel channel2 = pool.acquire().syncUninterruptibly().getNow();
    // first check that when returned healthy then it actually offered back to the pool.
    assertSame(channel1, channel2);

    channel1.close().syncUninterruptibly();

    pool.release(channel1).syncUninterruptibly();
    Channel channel3 = pool.acquire().syncUninterruptibly().getNow();
    // channel1 was not healthy anymore so it should not get acquired anymore.
    assertNotSame(channel1, channel3);
    sc.close().syncUninterruptibly();
    channel3.close().syncUninterruptibly();
    group.shutdownGracefully();
  }

  /**
   * Tests that if channel was unhealthy it is was offered back to the pool because
   * it was requested not to validate channel health on release.
   *
   * @throws Exception
   */
  @Test(groups = "unit")
  public void testUnhealthyChannelIsOfferedWhenNoHealthCheckRequested() throws Exception {
    EventLoopGroup group = new DefaultEventLoopGroup();
    LocalAddress addr = new LocalAddress(LOCAL_ADDR_ID);
    Bootstrap cb = new Bootstrap();
    cb.remoteAddress(addr);
    cb.group(group).channel(LocalChannel.class);

    ServerBootstrap sb = new ServerBootstrap();
    sb.group(group).channel(LocalServerChannel.class).childHandler(new ChannelInitializer<LocalChannel>() {
      @Override
      public void initChannel(LocalChannel ch) throws Exception {
        ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
      }
    });

    // Start server
    Channel sc = sb.bind(addr).syncUninterruptibly().channel();
    ChannelPoolHandler handler = new CountingChannelPoolHandler();
    ChannelPool pool = new FastSimpleChannelPool(cb, handler, ChannelHealthChecker.ACTIVE, false);
    Channel channel1 = pool.acquire().syncUninterruptibly().getNow();
    channel1.close().syncUninterruptibly();
    Future<Void> releaseFuture = pool.release(channel1, channel1.eventLoop().<Void>newPromise()).syncUninterruptibly();
    Assert.assertTrue(releaseFuture.isSuccess());

    Channel channel2 = pool.acquire().syncUninterruptibly().getNow();
    // verifying that in fact the channel2 is different that means is not pulled from the pool
    assertNotSame(channel1, channel2);
    sc.close().syncUninterruptibly();
    channel2.close().syncUninterruptibly();
    group.shutdownGracefully();
  }

  @Test(groups = "unit")
  public void testBootstrap() {
    final FastSimpleChannelPool pool = new FastSimpleChannelPool(new Bootstrap(), new CountingChannelPoolHandler());

    try {
      // Checking for the actual bootstrap object doesn't make sense here, since the pool uses a copy with a
      // modified channel handler.
      assertNotNull(pool.bootstrap());
    } finally {
      pool.close();
    }
  }

  @Test(groups = "unit")
  public void testHandler() {
    final ChannelPoolHandler handler = new CountingChannelPoolHandler();
    final FastSimpleChannelPool pool = new FastSimpleChannelPool(new Bootstrap(), handler);

    try {
      assertSame(handler, pool.handler());
    } finally {
      pool.close();
    }
  }

  @Test(groups = "unit")
  public void testHealthChecker() {
    final ChannelHealthChecker healthChecker = ChannelHealthChecker.ACTIVE;
    final FastSimpleChannelPool pool =
        new FastSimpleChannelPool(new Bootstrap(), new CountingChannelPoolHandler(), healthChecker);

    try {
      assertSame(healthChecker, pool.healthChecker());
    } finally {
      pool.close();
    }
  }

  @Test(groups = "unit")
  public void testReleaseHealthCheck() {
    final FastSimpleChannelPool healthCheckOnReleasePool =
        new FastSimpleChannelPool(new Bootstrap(), new CountingChannelPoolHandler(), ChannelHealthChecker.ACTIVE, true);

    try {
      assertTrue(healthCheckOnReleasePool.releaseHealthCheck());
    } finally {
      healthCheckOnReleasePool.close();
    }

    final FastSimpleChannelPool noHealthCheckOnReleasePool = new FastSimpleChannelPool(
        new Bootstrap(),
        new CountingChannelPoolHandler(),
        ChannelHealthChecker.ACTIVE,
        false);

    try {
      assertFalse(noHealthCheckOnReleasePool.releaseHealthCheck());
    } finally {
      noHealthCheckOnReleasePool.close();
    }
  }
}
