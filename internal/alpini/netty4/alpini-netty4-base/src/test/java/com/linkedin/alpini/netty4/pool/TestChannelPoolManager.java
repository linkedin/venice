package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.CallTrackerImpl;
import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.InstrumentedBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/30/17.
 */
public class TestChannelPoolManager {
  private Logger _log;
  {
    // org.apache.log4j.BasicConfigurator.configure();
    _log = LogManager.getLogger(getClass());
  }

  private class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
    private Consumer<Object> _responseConsumer;

    Request(
        HttpVersion httpVersion,
        HttpMethod method,
        String uri,
        ByteBuf content,
        Consumer<Object> responseConsumer) {
      super(httpVersion, method, uri, content);
      _responseConsumer = responseConsumer;
    }

    @Override
    public Consumer<Object> responseConsumer() {
      return _responseConsumer;
    }
  }

  @DataProvider
  public static Object[][] poolModes() {
    return new Object[][] { { false, false, false, false }, // Use Thread Local Pool
        { true, false, false, false }, // Use Global Pool
        { false, false, false, true }, // Use Thread Local Pool, fast impl
        { true, false, false, true } // Use Global Pool, fast impl
    };
  }

  long acquireTimeoutMillis = 1000L;
  int maxConnections = 1;
  int maxPendingAcquires = 100;
  int maxWaitersPerPool = 100;
  boolean releaseHealthCheck = true;
  long healthCheckInterval = 10000L;
  ChannelHealthChecker channelHealthChecker = new ChannelHealthChecker() {
    @Override
    public Future<Boolean> isHealthy(Channel channel) {
      Promise<Boolean> health = channel.eventLoop().newPromise();
      Promise<Object> response = channel.eventLoop().newPromise();
      channel
          .writeAndFlush(
              new Request(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/", Unpooled.EMPTY_BUFFER, response::setSuccess))
          .addListener(writeFuture -> {
            if (writeFuture.isSuccess()) {
              response.addListener(responseFuture -> {
                if (responseFuture.isSuccess()) {
                  if (responseFuture.getNow() instanceof FullHttpResponse) {
                    int statusCode = ((FullHttpResponse) responseFuture.getNow()).status().code();
                    _log.debug("Health check response code {}", statusCode);
                    health.setSuccess(statusCode >= 200 && statusCode < 300);
                  } else {
                    _log.warn("bad response type");
                    health.setSuccess(false);
                  }
                } else {
                  _log.warn("unhealthy", writeFuture.cause());
                  health.setSuccess(false);
                }
              });
            } else {
              _log.warn("unhealthy", writeFuture.cause());
              health.setSuccess(false);
            }
          });
      return health;
    }
  };
  CallTracker healthCheckerTracker = null;

  CallTracker connectCallTracker = new CallTrackerImpl();

  ChannelPoolResolver channelPoolResolver = new BasicDnsResolver();

  /**
   * These tests fail under high concurrency, which unfortunately, the tests execute the subproject tests in parallel
   */
  public static class Retry implements IRetryAnalyzer {
    int _attempts = 3;

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

  @Test(groups = "unit", retryAnalyzer = Retry.class, successPercentage = 30, dataProvider = "poolModes")
  public void testChannelPoolManagerNIO(
      boolean useGlobalPool,
      boolean createConnectionsOnWorkerGroup,
      boolean enableSimpleAcquire,
      boolean usingFastImpl) throws InterruptedException {

    NettyUtils.setMode("NIO");

    EventLoopGroup eventLoopGroup = NettyUtils.newEventLoopGroup(4, Executors.defaultThreadFactory());
    try {
      channelPoolManagerTest(
          (MultithreadEventLoopGroup) eventLoopGroup,
          useGlobalPool,
          createConnectionsOnWorkerGroup,
          enableSimpleAcquire,
          usingFastImpl);
    } finally {
      eventLoopGroup.shutdownGracefully().await();
    }
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class, successPercentage = 30, dataProvider = "poolModes")
  public void testChannelPoolManagerEPOLL(
      boolean useGlobalPool,
      boolean createConnectionsOnWorkerGroup,
      boolean enableSimpleAcquire,
      boolean usingFastImpl) throws InterruptedException {

    NettyUtils.setMode("EPOLL");

    EventLoopGroup eventLoopGroup = NettyUtils.newEventLoopGroup(4, Executors.defaultThreadFactory());
    try {
      channelPoolManagerTest(
          (MultithreadEventLoopGroup) eventLoopGroup,
          useGlobalPool,
          createConnectionsOnWorkerGroup,
          enableSimpleAcquire,
          usingFastImpl);
    } finally {
      eventLoopGroup.shutdownGracefully().await();
    }
  }

  private static void assertSupplierEquals(IntSupplier supplier, int value) {
    long deadline = Time.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
    while (supplier.getAsInt() != value && Time.nanoTime() < deadline) {
      Thread.yield();
    }
    Assert.assertEquals(supplier.getAsInt(), value);
  }

  private Future<Channel> _cancelInAcquireResolved;
  private Future<Channel> _cancelInAcquireEventLoop;
  private Future<Channel> _cancelInAcquireEventLoopListener;
  private Future<Channel> _cancelInAcquireTrySuccess;

  private <E extends MultithreadEventLoopGroup> void channelPoolManagerTest(
      E eventLoopGroup,
      boolean useGlobalPool,
      boolean createConnectionsOnWorkerGroup,
      boolean enableSimpleAcquire,
      boolean usingFastImpl) throws InterruptedException {

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

    ChannelPoolManager manager = new ChannelPoolManagerImpl(
        eventLoopGroup,
        channelPoolFactory,
        channelPoolResolver,
        maxWaitersPerPool,
        useGlobalPool,
        createConnectionsOnWorkerGroup,
        enableSimpleAcquire) {
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
    try {
      Future<Channel> channelFuture;

      Assert.assertEquals(manager.openConnections(), 0);
      Assert.assertEquals(manager.activeCount(), 0);

      try {
        channelFuture = manager.acquire("bad host name and port", "fail", QOS.NORMAL);
        Assert.fail("should not get here: " + channelFuture);
      } catch (IllegalArgumentException ex) {
        // expected
      }

      for (int i = 10; i > 0; i--) {
        channelFuture = manager.acquire("localhost:79", "fail", QOS.NORMAL);

        Assert.assertTrue(channelFuture.await((long) (acquireTimeoutMillis * 1.1), TimeUnit.MILLISECONDS));
        Assert.assertFalse(channelFuture.isSuccess());
        Assert.assertTrue(
            channelFuture.cause().getMessage().matches(".*Connection refused: localhost/.*"),
            channelFuture.cause().getMessage());

        channelFuture = manager.acquire("localhost:79", "fail", QOS.HIGH);

        Assert.assertFalse(channelFuture.await().isSuccess());
        Assert.assertTrue(
            channelFuture.cause().getMessage().matches(".*Connection refused: localhost/.*"),
            channelFuture.cause().getMessage());
      }

      Thread.sleep(100);
      Assert.assertEquals(manager.openConnections(), 0);
      Assert.assertEquals(manager.activeCount(), 0);

      Channel serverChannel = dummyServer(eventLoopGroup);
      try {
        InetSocketAddress serverAddress = (InetSocketAddress) serverChannel.localAddress();

        for (int i = 10; i > 0; i--) {

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "default", QOS.NORMAL);
          Assert.assertTrue(channelFuture.await().isSuccess());
          ChannelPoolManager.PoolStats stats =
              manager.getPoolStats("localhost:" + serverAddress.getPort()).orElseThrow(NullPointerException::new);
          // Queue stats are no longer implemented
          Assert.assertFalse(manager.getQueueStats("default").isPresent());
          Assert.assertEquals(stats.activeCount(), 1);
          Assert.assertTrue(manager.release(channelFuture.getNow()).syncUninterruptibly().isSuccess());
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture =
              manager.acquire(eventLoopGroup.next(), "localhost:" + serverAddress.getPort(), "default", QOS.HIGH);
          Assert.assertTrue(channelFuture.await().isSuccess());
          Assert.assertEquals(stats.activeCount(), 1);
          Assert.assertTrue(manager.release(channelFuture.getNow()).syncUninterruptibly().isSuccess());
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.NORMAL);
          Assert.assertTrue(channelFuture.await().isSuccess());
          // Queue stats are no longer implemented
          Assert.assertFalse(manager.getQueueStats("other").isPresent());
          Assert.assertEquals(stats.activeCount(), 1);
          Assert.assertTrue(manager.release(channelFuture.getNow()).syncUninterruptibly().isSuccess());
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          Assert.assertTrue(channelFuture.cancel(false));
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          _cancelInAcquireResolved = channelFuture;
          Assert.assertTrue(channelFuture.await(1, TimeUnit.SECONDS));
          Assert.assertTrue(channelFuture.isCancelled());
          // Sleep for 100ms, so that the stats can catch up.
          Thread.sleep(100);
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          _cancelInAcquireEventLoop = channelFuture;
          Assert.assertTrue(channelFuture.await(1, TimeUnit.SECONDS));
          Assert.assertTrue(channelFuture.isCancelled());
          // Sleep for 100ms, so that the stats can catch up.
          Thread.sleep(100);
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          _cancelInAcquireEventLoopListener = channelFuture;
          Assert.assertTrue(channelFuture.await(1, TimeUnit.SECONDS));
          Assert.assertTrue(channelFuture.isCancelled());
          // Sleep for 100ms, so that the stats can catch up.
          Thread.sleep(100);
          Assert.assertEquals(stats.activeCount(), 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          _cancelInAcquireTrySuccess = channelFuture;
          Assert.assertTrue(channelFuture.await(1, TimeUnit.SECONDS));
          Assert.assertTrue(channelFuture.isCancelled());
          // Sleep for 100ms, so that the stats can catch up.
          Thread.sleep(100);
          assertSupplierEquals(stats::activeCount, 0);

          channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "other", QOS.HIGH);
          Assert.assertTrue(channelFuture.await().isSuccess());
          Assert.assertEquals(stats.activeCount(), 1);

          Channel ch = channelFuture.getNow();
          Assert.assertTrue(
              ch.eventLoop()
                  .submit(() -> manager.release(ch))
                  .syncUninterruptibly()
                  .getNow()
                  .syncUninterruptibly()
                  .isSuccess());
          Assert.assertEquals(stats.activeCount(), 0);

        }

        Thread.sleep(100);

        Assert.assertEquals(manager.activeCount(), 0);
        Assert.assertTrue(manager.openConnections() > 0);

        ChannelPoolManager.PoolStats stats =
            manager.getPoolStats("localhost:" + serverAddress.getPort()).orElseThrow(NullPointerException::new);
        Assert.assertTrue(stats.isHealthy());
        Assert.assertEquals(stats.activeCount(), 0);

        // Queue stats are no longer implemented
        Assert.assertFalse(manager.getQueueStats("other").isPresent());

        manager.getPoolStats().forEach((key, value) -> _log.debug("Pool of '{}' = '{}'", key, value));

        // Queue stats are no longer implemented
        Assert.assertTrue(manager.getQueueStats().isEmpty());

        Assert.assertEquals(stats.closeErrorCount(), 0);
        Assert.assertEquals(stats.closeBadCount(), 0);

        // Non-existent pools should always return success
        Assert.assertTrue(manager.close("localhost:1").isSuccess());

      } finally {
        _log.debug("Closing server channel");
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

  private <E extends MultithreadEventLoopGroup> Channel dummyServer(E eventLoopGroup) {
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
                        ByteBuf content = Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII);
                        FullHttpResponse response = new BasicFullHttpResponse(msg, HttpResponseStatus.OK, content);
                        HttpUtil.setContentLength(response, content.readableBytes());
                        HttpUtil.setKeepAlive(response, HttpUtil.isKeepAlive(msg));
                        ctx.writeAndFlush(response);
                      }
                    });
          }
        });

    return bootstrap.bind().syncUninterruptibly().channel();
  }
}
