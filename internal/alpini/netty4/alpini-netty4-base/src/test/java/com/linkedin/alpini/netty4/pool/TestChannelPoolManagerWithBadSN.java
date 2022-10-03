package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.CallTrackerImpl;
import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.alpini.netty4.misc.ExceptionWithResponseStatus;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.InstrumentedBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This class tests how the ChannelPoolManager behaves when the SN is unresponsive.
 *
 * The bad SN is simulated by just consuming the request and not responding.
 *
 * Created by aandhava on 10/24/17.
 * Based on TestChannelPoolManager
 */
public class TestChannelPoolManagerWithBadSN {
  private Logger _log;
  {
    _log = LogManager.getLogger(getClass());
  }

  long acquireTimeoutMillis = 150L;
  int maxConnections = 1;
  int maxPendingAcquires = 5;
  int maxWaitersPerPool = 5;
  boolean releaseHealthCheck = true;
  long healthCheckInterval = 10000L;
  ChannelHealthChecker channelHealthChecker = channel -> channel.eventLoop().newSucceededFuture(Boolean.TRUE);
  CallTracker healthCheckerTracker = null;

  CallTracker connectCallTracker = new CallTrackerImpl();

  ChannelPoolResolver channelPoolResolver = new BasicDnsResolver();

  /**
   * These tests fail under high concurrency, which unfortunately, the tests execute the subproject tests in parallel
   */
  public static class Retry implements IRetryAnalyzer {
    int _attempts = 0;

    @Override
    public boolean retry(ITestResult result) {
      if (!result.isSuccess() && _attempts-- > 0) {
        result.setStatus(ITestResult.SUCCESS_PERCENTAGE_FAILURE);
        try {
          Thread.sleep(1000 + ThreadLocalRandom.current().nextInt(10000));
        } catch (InterruptedException e) {
          // Ignored;
        }
        return true;
      }
      return false;
    }
  }

  @DataProvider
  public Object[][] modes() {
    return new Object[][] { new Object[] { false }, new Object[] { true } };
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class, successPercentage = 100, timeOut = 60000, dataProvider = "modes")
  public void testChannelPoolManagerNIOBadSN(boolean usingFastImpl) throws Exception {
    NettyUtils.setMode("NIO");
    EventLoopGroup eventLoopGroup = NettyUtils.newEventLoopGroup(1, Executors.defaultThreadFactory());
    try {
      channelPoolManagerTestBadSN((MultithreadEventLoopGroup) eventLoopGroup, usingFastImpl);
    } finally {
      eventLoopGroup.shutdownGracefully().await();
    }
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class, successPercentage = 100, timeOut = 60000, dataProvider = "modes")
  public void testChannelPoolManagerEPOLLBadSN(boolean usingFastImpl) throws Exception {
    NettyUtils.setMode("EPOLL");
    EventLoopGroup eventLoopGroup = NettyUtils.newEventLoopGroup(1, Executors.defaultThreadFactory());
    try {
      channelPoolManagerTestBadSN((MultithreadEventLoopGroup) eventLoopGroup, usingFastImpl);
    } finally {
      eventLoopGroup.shutdownGracefully().await();
    }
  }

  private Future<Channel> _cancelInAcquireResolved;
  private Future<Channel> _cancelInAcquireEventLoop;
  private Future<Channel> _cancelInAcquireEventLoopListener;
  private Future<Channel> _cancelInAcquireTrySuccess;

  private <E extends MultithreadEventLoopGroup> ChannelPoolManager setUpChannelPoolManager(
      E eventLoopGroup,
      boolean usingFastImpl) {
    Bootstrap bootstrap = new InstrumentedBootstrap(connectCallTracker).channel(NettyUtils.socketChannel())
        .option(ChannelOption.ALLOW_HALF_CLOSURE, true)
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            _log.debug("initChannel({})", ch.id());
            ch.pipeline()
                .addLast(new HttpClientCodec(), new HttpObjectAggregator(4096), new HttpClientResponseHandler());
          }

          @Override
          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            super.handlerAdded(ctx);
          }
        });

    ChannelPoolFactory channelPoolFactory = new FixedChannelPoolFactory(
        bootstrap,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        healthCheckInterval,
        channelHealthChecker,
        healthCheckerTracker);
    ((FixedChannelPoolFactory) channelPoolFactory).setUsingFastPool(usingFastImpl);

    return new ChannelPoolManagerImpl(eventLoopGroup, channelPoolFactory, channelPoolResolver, maxWaitersPerPool) {
      @Nonnull
      @Override
      protected CallTracker createHostAcquireCallTracker(@Nonnull String hostAndPort) {
        Assert.assertSame(super.createHostAcquireCallTracker(hostAndPort), CallTracker.nullTracker());
        return CallTracker.create();
      }

      @Nonnull
      @Override
      protected CallTracker createHostBusyCallTracker(@Nonnull String hostAndPort) {
        Assert.assertSame(super.createHostBusyCallTracker(hostAndPort), CallTracker.nullTracker());
        return CallTracker.create();
      }

      @Nonnull
      @Override
      protected CallTracker createQueueAcquireCallTracker(@Nonnull String queueName) {
        Assert.assertSame(super.createQueueAcquireCallTracker(queueName), CallTracker.nullTracker());
        return CallTracker.create();
      }

      @Nonnull
      @Override
      protected CallTracker createQueueBusyCallTracker(@Nonnull String queueName) {
        Assert.assertSame(super.createQueueBusyCallTracker(queueName), CallTracker.nullTracker());
        return CallTracker.create();
      }

      @Override
      void debugAcquireResolved(Future<Channel> promise) {
        Time.sleepUninterruptably(1);
        super.debugAcquireResolved(promise);
        if (promise == _cancelInAcquireResolved) {
          _cancelInAcquireResolved.cancel(false);
        }
      }

      @Override
      void debugAcquireInEventLoop(Future<Channel> promise) {
        Time.sleepUninterruptably(1);
        super.debugAcquireInEventLoop(promise);
        if (promise == _cancelInAcquireEventLoop) {
          _cancelInAcquireEventLoop.cancel(false);
        }
      }

      @Override
      void debugAcquireInEventLoopListener(Future<Channel> promise) {
        Time.sleepUninterruptably(1);
        super.debugAcquireInEventLoopListener(promise);
        if (promise == _cancelInAcquireEventLoopListener) {
          _cancelInAcquireEventLoopListener.cancel(false);
        }
      }

      @Override
      void debugAcquireInTrySuccess(Future<Channel> promise) {
        Time.sleepUninterruptably(1);
        super.debugAcquireInTrySuccess(promise);
        if (promise == _cancelInAcquireTrySuccess) {
          _cancelInAcquireTrySuccess.cancel(false);
        }
      }
    };
  }

  private <E extends MultithreadEventLoopGroup> Channel dummySNServerWhichDontRespond(E eventLoopGroup) {
    ServerBootstrap bootstrap = new ServerBootstrap().channel(NettyUtils.serverSocketChannel())
        .group(eventLoopGroup)
        .localAddress(InetAddress.getLoopbackAddress(), 0)
        .option(ChannelOption.ALLOW_HALF_CLOSURE, true)
        .option(ChannelOption.TCP_NODELAY, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                .addLast(
                    new HttpServerCodec(),
                    new HttpObjectAggregator(4096),
                    new SimpleChannelInboundHandler<FullHttpRequest>() {
                      @Override
                      protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
                        _log.info("Request Received");
                        // Eat the request, don't respond. Simulate a SSD failure.
                      }
                    });
          }
        });

    return bootstrap.bind().syncUninterruptibly().channel();
  }

  private <E extends MultithreadEventLoopGroup> void channelPoolManagerTestBadSN(
      E eventLoopGroup,
      boolean usingFastImpl) throws Exception {
    ChannelPoolManager manager = setUpChannelPoolManager(eventLoopGroup, usingFastImpl);
    try {
      Future<Channel> channelFuture;
      TimeoutException timeOutException = new TimeoutException();
      CountDownLatch latch = new CountDownLatch(5);

      Assert.assertEquals(manager.openConnections(), 0);
      Assert.assertEquals(manager.activeCount(), 0);

      Channel serverChannel = dummySNServerWhichDontRespond(eventLoopGroup);
      try {
        InetSocketAddress serverAddress = (InetSocketAddress) serverChannel.localAddress();
        String hostNameAndPort = "localhost:" + serverAddress.getPort();

        // Acquire the only connection
        channelFuture = manager.acquire(hostNameAndPort, "default", QOS.NORMAL);
        Assert.assertTrue(channelFuture.await().isSuccess());

        ChannelPoolManager.PoolStats poolStats = manager.getPoolStats(hostNameAndPort).get();
        Assert.assertNotNull(poolStats);

        // Verify Pool Parameters
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getMaxConnections)
                .sum(),
            1);
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getMaxPendingAcquires)
                .sum(),
            5);

        // Verify Pool Stats
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getPendingAcquireCount)
                .sum(),
            0);
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getAcquiredChannelCount)
                .sum(),
            1);
        Assert.assertTrue(
            poolStats.getThreadPoolStats().values().stream().allMatch(threadPoolStats -> !threadPoolStats.isClosed()));

        // Exhaust all the wait queue
        for (int i = 5; i > 0; i--) {
          Promise acquireFuture =
              eventLoopGroup.submit(() -> (Promise) manager.acquire(hostNameAndPort, "default", QOS.NORMAL))
                  .get()
                  .addListener(f -> {
                    // Assert they are failed because of the client timeout.
                    Assert.assertEquals(f.cause(), timeOutException);
                    latch.countDown();
                  });

          // Simulate a client timeout of 300ms.
          eventLoopGroup.schedule(() -> acquireFuture.tryFailure(timeOutException), 300, TimeUnit.MILLISECONDS);
        }

        channelFuture = manager.acquire(hostNameAndPort, "default", QOS.NORMAL);
        // Fails as the queue is full.
        Assert.assertTrue(!channelFuture.await().isSuccess());
        // Assert this is failed because of the queue being full.
        Assert.assertTrue(channelFuture.cause() instanceof ExceptionWithResponseStatus);

        // Should be 5 waiting for the connections
        Assert.assertEquals(poolStats.waitingCount(), 5);
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getPendingAcquireCount)
                .sum(),
            5);

        // Wait until all the channel acquire requests, come back.
        latch.await();

        // Should be 6, as futures are cancelled due to request timeout.
        Assert.assertEquals(poolStats.acquireCallTracker().getCurrentErrorCountTotal(), 6);

        // Sleep for 200ms (acquireTimeoutMillis for the FixedChannelPool is 100ms, so that FixedChannelPool rejects the
        // requests.
        Thread.sleep(200);

        // Assert the inFlight requests. As there are no waiters, the inflight requests should be zero.
        Assert.assertEquals(poolStats.inFlightCount(), 0);
        Assert.assertEquals(
            poolStats.getThreadPoolStats()
                .values()
                .stream()
                .mapToInt(ChannelPoolManager.ThreadPoolStats::getPendingAcquireCount)
                .sum(),
            0);

        // Non-existent pools should always return success
        Assert.assertTrue(manager.close("localhost:1").isSuccess());
      } finally {
        _log.info("Closing server channel");
        serverChannel.close().awaitUninterruptibly();
      }
    } finally {
      _log.debug("Closing all");
      Future<Void> closeFuture = manager.closeAll().awaitUninterruptibly();
      if (!closeFuture.isSuccess()) {
        _log.warn("Exception in closeAll", closeFuture.cause());
      }
      _log.debug("Closing all done");
      Assert.assertEquals(manager.openConnections(), 0);
    }
  }
}
