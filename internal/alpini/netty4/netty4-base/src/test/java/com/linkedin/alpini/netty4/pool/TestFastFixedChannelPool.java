package com.linkedin.alpini.netty4.pool;

import static org.testng.Assert.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 *
 * Forked from Netty's FixedChannelPoolTest
 *
 */
public class TestFastFixedChannelPool {
  private static final String LOCAL_ADDR_ID = "test.id";

  private EventLoopGroup group;

  @BeforeMethod(groups = "unit")
  public void createEventLoop() {
    group = new DefaultEventLoopGroup();
  }

  @AfterMethod(groups = "unit")
  public void destroyEventLoop() throws InterruptedException {
    if (group != null) {
      group.shutdownGracefully().sync();
    }
  }

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
    CountingChannelPoolHandler handler = new CountingChannelPoolHandler();

    ChannelPool pool = new FastFixedChannelPool(cb, handler, 1, Integer.MAX_VALUE);

    Channel channel = pool.acquire().syncUninterruptibly().getNow();
    Future<Channel> future = pool.acquire();
    Assert.assertFalse(future.isDone());

    pool.release(channel).syncUninterruptibly();
    assertTrue(future.await(1, TimeUnit.SECONDS));

    Channel channel2 = future.getNow();
    assertSame(channel, channel2);
    assertEquals(1, handler.channelCount());

    assertEquals(2, handler.acquiredCount());
    assertEquals(1, handler.releasedCount());

    sc.close().syncUninterruptibly();
    channel2.close().syncUninterruptibly();
  }

  @Test(groups = "unit", expectedExceptions = TimeoutException.class)
  public void testAcquireTimeout() throws Exception {
    testAcquireTimeout(500);
  }

  @Test(groups = "unit", expectedExceptions = TimeoutException.class)
  public void testAcquireWithZeroTimeout() throws Exception {
    testAcquireTimeout(0);
  }

  private void testAcquireTimeout(long timeoutMillis) throws Exception {
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
    ChannelPoolHandler handler = new TestChannelPoolHandler();
    ChannelPool pool = new FastFixedChannelPool(
        cb,
        handler,
        ChannelHealthChecker.ACTIVE,
        AcquireTimeoutAction.FAIL,
        timeoutMillis,
        1,
        Integer.MAX_VALUE);

    Channel channel = pool.acquire().syncUninterruptibly().getNow();
    Future<Channel> future = pool.acquire();
    try {
      future.syncUninterruptibly();
    } finally {
      sc.close().syncUninterruptibly();
      channel.close().syncUninterruptibly();
    }
  }

  @Test(groups = "unit")
  public void testAcquireNewConnection() throws Exception {
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
    ChannelPoolHandler handler = new TestChannelPoolHandler();
    ChannelPool pool = new FastFixedChannelPool(
        cb,
        handler,
        ChannelHealthChecker.ACTIVE,
        AcquireTimeoutAction.NEW,
        500,
        1,
        Integer.MAX_VALUE);

    Channel channel = pool.acquire().syncUninterruptibly().getNow();
    Channel channel2 = pool.acquire().syncUninterruptibly().getNow();
    assertNotSame(channel, channel2);
    sc.close().syncUninterruptibly();
    channel.close().syncUninterruptibly();
    channel2.close().syncUninterruptibly();
  }

  /**
   * Tests that the acquiredChannelCount is not added up several times for the same channel acquire request.
   * @throws Exception
   */
  @Test(groups = "unit")
  public void testAcquireNewConnectionWhen() throws Exception {
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
    ChannelPoolHandler handler = new TestChannelPoolHandler();
    ChannelPool pool = new FastFixedChannelPool(cb, handler, 1);
    Channel channel1 = pool.acquire().syncUninterruptibly().getNow();
    channel1.close().syncUninterruptibly();
    pool.release(channel1);

    Channel channel2 = pool.acquire().syncUninterruptibly().getNow();

    assertNotSame(channel1, channel2);
    sc.close().syncUninterruptibly();
    channel2.close().syncUninterruptibly();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testAcquireBoundQueue() throws Exception {
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
    ChannelPoolHandler handler = new TestChannelPoolHandler();
    ChannelPool pool = new FastFixedChannelPool(cb, handler, 1, 1);

    Channel channel = pool.acquire().syncUninterruptibly().getNow();
    Future<Channel> future = pool.acquire();
    assertFalse(future.isDone());

    try {
      pool.acquire().syncUninterruptibly();
    } finally {
      sc.close().syncUninterruptibly();
      channel.close().syncUninterruptibly();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testReleaseDifferentPool() throws Exception {
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
    ChannelPoolHandler handler = new TestChannelPoolHandler();
    ChannelPool pool = new FastFixedChannelPool(cb, handler, 1, 1);
    ChannelPool pool2 = new FastFixedChannelPool(cb, handler, 1, 1);

    Channel channel = pool.acquire().syncUninterruptibly().getNow();

    try {
      pool2.release(channel).syncUninterruptibly();
    } finally {
      sc.close().syncUninterruptibly();
      channel.close().syncUninterruptibly();
    }
  }

  @Test(groups = "unit")
  public void testReleaseAfterClosePool() throws Exception {
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

    FastFixedChannelPool pool = new FastFixedChannelPool(cb, new TestChannelPoolHandler(), 2);
    final Future<Channel> acquire = pool.acquire();
    final Channel channel = acquire.get();
    pool.close();
    group.submit(new Runnable() {
      @Override
      public void run() {
        // NOOP
      }
    }).syncUninterruptibly();
    try {
      pool.release(channel).syncUninterruptibly();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
    // Since the pool is closed, the Channel should have been closed as well.
    channel.closeFuture().syncUninterruptibly();
    assertFalse(channel.isOpen());
    sc.close().syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void testReleaseClosed() {
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

    FastFixedChannelPool pool = new FastFixedChannelPool(cb, new TestChannelPoolHandler(), 2);
    Channel channel = pool.acquire().syncUninterruptibly().getNow();
    channel.close().syncUninterruptibly();
    pool.release(channel).syncUninterruptibly();

    sc.close().syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void testCloseAsync() throws ExecutionException, InterruptedException {
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
    final Channel sc = sb.bind(addr).syncUninterruptibly().channel();

    final FastFixedChannelPool pool = new FastFixedChannelPool(cb, new TestChannelPoolHandler(), 2);

    pool.acquire().get();
    pool.acquire().get();

    final ChannelPromise closePromise = sc.newPromise();
    pool.closeAsync().addListener(new GenericFutureListener<Future<? super Void>>() {
      @Override
      public void operationComplete(Future<? super Void> future) throws Exception {
        Assert.assertEquals(0, pool.acquiredChannelCount());
        sc.close(closePromise).syncUninterruptibly();
      }
    }).awaitUninterruptibly();
    closePromise.awaitUninterruptibly();
  }

  private static final class TestChannelPoolHandler extends AbstractChannelPoolHandler {
    @Override
    public void channelCreated(Channel ch) throws Exception {
      // NOOP
    }
  }
}
