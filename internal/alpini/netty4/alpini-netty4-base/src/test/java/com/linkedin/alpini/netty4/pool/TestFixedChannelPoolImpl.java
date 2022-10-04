package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.handlers.Log4J2LoggingHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/8/18.
 */
public class TestFixedChannelPoolImpl {
  private static final Logger LOG = LogManager.getLogger(TestFixedChannelPoolImpl.class);

  private EventLoopGroup _eventLoopGroup;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _eventLoopGroup = new NioEventLoopGroup(1);
  }

  @AfterClass(groups = "unit")
  public void afterClass() {
    Optional.ofNullable(_eventLoopGroup).ifPresent(EventLoopGroup::shutdownGracefully);
  }

  @DataProvider
  public Object[][] useQueueSizeForAcquiredChannelCount() {
    return new Object[][] { new Object[] { true }, new Object[] { false } };
  }

  @Test(groups = "unit", dataProvider = "useQueueSizeForAcquiredChannelCount")
  public void testMinPoolSize(boolean useQueueSizeForAcquiredChannelCount) throws InterruptedException {

    LocalAddress localAddress = new LocalAddress("testMinPoolSize");

    LoggingHandler listenLog = new Log4J2LoggingHandler("listen", LogLevel.DEBUG);
    LoggingHandler serverLog = new Log4J2LoggingHandler("server", LogLevel.DEBUG);

    ServerBootstrap serverBootstrap = new ServerBootstrap().group(_eventLoopGroup)
        .channel(LocalServerChannel.class)
        .handler(listenLog)
        .childHandler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(serverLog);
          }
        });

    Bootstrap bootstrap =
        new Bootstrap().group(_eventLoopGroup).channel(LocalChannel.class).remoteAddress(localAddress);

    ChannelFuture server = serverBootstrap.bind(localAddress).sync();
    try {

      int minConnections = 10;
      int maxConnections = 100;
      int maxPendingAcquires = 100;

      LongAdder createAdder = new LongAdder();
      LongAdder closeAdder = new LongAdder();

      FixedChannelPoolImpl pool = createNewFixedChannelPool(
          useQueueSizeForAcquiredChannelCount,
          bootstrap,
          minConnections,
          maxConnections,
          maxPendingAcquires,
          createAdder,
          closeAdder);

      Thread.sleep(100L);

      Assert.assertEquals(pool.getConnectedChannels(), 0);
      Assert.assertEquals(pool.getAcquiredChannelCount(), 0);
      CountDownLatch released = new CountDownLatch(1);

      {
        Future<Channel> channelFuture = CompletableFuture.supplyAsync(pool::acquire, _eventLoopGroup.next()).join();
        channelFuture.sync();
        Assert.assertTrue(channelFuture.isSuccess());
        Assert.assertEquals(pool.getAcquiredChannelCount(), 1);
        _eventLoopGroup.execute(() -> pool.release(channelFuture.getNow()).addListener(f -> released.countDown()));
      }

      {
        Future<Channel> channelFuture = CompletableFuture.supplyAsync(pool::acquire, _eventLoopGroup.next()).join();
        channelFuture.sync();
        Assert.assertTrue(channelFuture.isSuccess());
        _eventLoopGroup.execute(() -> pool.release(channelFuture.getNow()).addListener(f -> released.countDown()));
      }

      long timeout = Time.currentTimeMillis() + 1000L;
      do {
        Thread.sleep(100L);
        LOG.info("created={}, closed={}", createAdder, closeAdder);
      } while (pool.getConnectedChannels() != minConnections && timeout > Time.currentTimeMillis());

      Assert.assertEquals(pool.getConnectedChannels(), minConnections);
      Assert.assertEquals(createAdder.intValue(), minConnections);
      Assert.assertEquals(closeAdder.intValue(), 0);

      released.await();
      Assert.assertEquals(pool.getAcquiredChannelCount(), 0);

      {
        Future<Channel> channelFuture = CompletableFuture.supplyAsync(pool::acquire, _eventLoopGroup.next()).join();
        channelFuture.sync();
        Assert.assertTrue(channelFuture.isSuccess());

        Assert.assertEquals(pool.getConnectedChannels(), minConnections);
        Assert.assertEquals(createAdder.intValue(), minConnections);
        Assert.assertEquals(closeAdder.intValue(), 0);

        _eventLoopGroup.execute(() -> pool.release(channelFuture.getNow()));
      }

      CountDownLatch latch = new CountDownLatch(1);

      // To test ESPENG-22776
      pool.acquire().addListener((Future<Channel> f) -> {
        if (f.isSuccess()) {
          LOG.info("Before close :" + pool.acquiredChannelCount());
          Assert.assertEquals(pool.acquiredChannelCount(), 1);

          // Close the channel
          f.getNow().close().addListener(ignored -> f.getNow().eventLoop().execute(() -> {
            Assert.assertEquals(pool.acquiredChannelCount(), 0);
            LOG.info("After close :" + pool.acquiredChannelCount());

            // Release the channel
            pool.release(f.getNow()).addListener(bar -> {
              Assert.assertEquals(pool.acquiredChannelCount(), 0);
              LOG.info("After release :" + pool.acquiredChannelCount());
              latch.countDown();
            });
          }));
        }
      });

      latch.await();
      LOG.info("closing pool {}", pool);
      CompletableFuture.runAsync(pool::close, _eventLoopGroup.next()).join();

      timeout = Time.currentTimeMillis() + 1000L;
      do {
        Thread.sleep(100L);
        LOG.info("created={}, closed={}", createAdder, closeAdder);
      } while (pool.getConnectedChannels() != 0 && timeout > Time.currentTimeMillis());

      Assert.assertEquals(pool.getConnectedChannels(), 0);
      Assert.assertEquals(closeAdder.intValue(), minConnections);
    } finally {
      server.channel().close().sync();
    }
  }

  @Test(groups = "unit", dataProvider = "useQueueSizeForAcquiredChannelCount")
  public void testPoolClose(boolean useQueueSizeForAcquiredChannelCount) throws InterruptedException {
    LocalAddress localAddress = new LocalAddress("testPoolClose");

    LoggingHandler listenLog = new Log4J2LoggingHandler("listen", LogLevel.DEBUG);
    LoggingHandler serverLog = new Log4J2LoggingHandler("server", LogLevel.DEBUG);

    ServerBootstrap serverBootstrap = new ServerBootstrap().group(_eventLoopGroup)
        .channel(LocalServerChannel.class)
        .handler(listenLog)
        .childHandler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(serverLog);
          }
        });

    Bootstrap bootstrap =
        new Bootstrap().group(_eventLoopGroup).channel(LocalChannel.class).remoteAddress(localAddress);

    ChannelFuture server = serverBootstrap.bind(localAddress).sync();
    try {

      int minConnections = 10;
      int maxConnections = 100;
      int maxPendingAcquires = 100;

      LongAdder createAdder = new LongAdder();
      LongAdder closeAdder = new LongAdder();

      FixedChannelPoolImpl pool = createNewFixedChannelPool(
          useQueueSizeForAcquiredChannelCount,
          bootstrap,
          minConnections,
          maxConnections,
          maxPendingAcquires,
          createAdder,
          closeAdder);

      Time.sleep(100L);

      Assert.assertEquals(pool.getConnectedChannels(), 0);

      // Acquire a connection, which will kick off the connection build process
      {
        Future<Channel> channelFuture = CompletableFuture.supplyAsync(pool::acquire, _eventLoopGroup.next()).join();
        channelFuture.sync();
        Assert.assertTrue(channelFuture.isSuccess());
        _eventLoopGroup.execute(() -> pool.release(channelFuture.getNow()));
      }

      // Wait until min connections are built.
      long timeout = Time.currentTimeMillis() + 1000L;
      do {
        Time.sleep(100L);
        LOG.info("created={}, closed={}", createAdder, closeAdder);
      } while (pool.getConnectedChannels() != minConnections && timeout > Time.currentTimeMillis());

      Assert.assertEquals(pool.getConnectedChannels(), minConnections);
      Assert.assertEquals(createAdder.intValue(), minConnections);
      Assert.assertEquals(closeAdder.intValue(), 0);

      LOG.info("closing pool {}", pool);
      CompletableFuture.runAsync(pool::close, _eventLoopGroup.next()).join();

      timeout = Time.currentTimeMillis() + 1000L;
      do {
        Time.sleep(100L);
        LOG.info("created={}, closed={}", createAdder, closeAdder);
      } while (pool.getConnectedChannels() != 0 && timeout > Time.currentTimeMillis());

      Assert.assertEquals(pool.getConnectedChannels(), 0);
      // All the connections are closed.
      Assert.assertEquals(closeAdder.intValue(), minConnections);
    } finally {
      server.channel().close().sync();
    }
  }

  private FixedChannelPoolImpl createNewFixedChannelPool(
      boolean useQueueSizeForAcquiredChannelCount,
      Bootstrap bootstrap,
      int minConnections,
      int maxConnections,
      int maxPendingAcquires,
      LongAdder createAdder,
      LongAdder closeAdder) {
    return new FixedChannelPoolImpl(bootstrap, new ChannelPoolHandler() {
      @Override
      public void channelReleased(Channel ch) throws Exception {
        LOG.info("channelReleased {}", ch.id());
      }

      @Override
      public void channelAcquired(Channel ch) throws Exception {
        LOG.info("channelAcquired {}", ch.id());
      }

      @Override
      public void channelCreated(Channel ch) throws Exception {
        LOG.info("channelCreated {}", ch.id());
        ch.closeFuture().addListener(future -> closeAdder.increment());
        createAdder.increment();
      }
    },
        ChannelHealthChecker.ACTIVE,
        FixedChannelPool.AcquireTimeoutAction.FAIL,
        100,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        true,
        false,
        () -> useQueueSizeForAcquiredChannelCount);
  }
}
