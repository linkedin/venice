package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.netty4.handlers.AllChannelsHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import java.util.IdentityHashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestBalancedEventLoopGroup {
  final Logger _log = LogManager.getLogger(getClass());
  final UUID _uuid = UUID.randomUUID();

  EventLoopGroup _unbalancedEventLoopGroup;

  @BeforeClass
  public void beforeClass() {
    ThreadFactory threadFactory = FastThreadLocalThread::new;
    _unbalancedEventLoopGroup =
        new MultithreadEventLoopGroup(32, new ThreadPerTaskExecutor(threadFactory), new UnbalancedChooser()) {
          @Override
          protected EventLoop newChild(Executor executor, Object... args) throws Exception {
            return new DefaultEventLoop(this, executor);
          }
        };
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_unbalancedEventLoopGroup != null) {
      _unbalancedEventLoopGroup.shutdownGracefully().sync();
    }
  }

  @Test()
  public void testBalanceLocal() throws Exception {
    testBalanceLocal(new LocalAddress("testBalanceLocal" + _uuid), true, true);
  }

  @Test(expectedExceptions = AssertionError.class, expectedExceptionsMessageRegExp = "Server Unbalanced.*")
  public void testUnbalancedServer() throws Exception {
    testBalanceLocal(new LocalAddress("testUnbalancedServer" + _uuid), false, true);
  }

  @Test(expectedExceptions = AssertionError.class, expectedExceptionsMessageRegExp = "Client Unbalanced.*")
  public void testUnbalancedClient() throws Exception {
    testBalanceLocal(new LocalAddress("testUnbalancedClient" + _uuid), true, false);
  }

  private void testBalanceLocal(LocalAddress bindAddress, boolean balanceServer, boolean balanceClient)
      throws Exception {

    AllChannelsHandler allClientConnections = new AllChannelsHandler();
    AllChannelsHandler allServerConnections = new AllChannelsHandler();
    Map<EventLoop, Integer> clientDist = new IdentityHashMap<>();
    Map<EventLoop, Integer> serverDist = new IdentityHashMap<>();

    ServerBootstrap serverBootstrap = new ServerBootstrap().channel(LocalServerChannel.class)
        .group(
            balanceServer
                ? new BalancedEventLoopGroup(_unbalancedEventLoopGroup, allServerConnections)
                : _unbalancedEventLoopGroup)
        .childHandler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) throws Exception {
            ch.pipeline().addLast(allServerConnections);
            synchronized (serverDist) {
              serverDist.compute(ch.eventLoop(), (eventLoop, integer) -> integer != null ? integer + 1 : 1);

            }
          }
        });

    Channel server = serverBootstrap.bind(bindAddress).sync().channel();

    try {

      Bootstrap bootstrap = new Bootstrap().channel(LocalChannel.class)
          .group(
              balanceClient
                  ? new BalancedEventLoopGroup(_unbalancedEventLoopGroup, allClientConnections)
                  : _unbalancedEventLoopGroup)
          .handler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) throws Exception {
              ch.pipeline().addLast(allClientConnections);
            }
          });

      final int iterations = 10000;
      final int thread = (int) StreamSupport.stream(_unbalancedEventLoopGroup.spliterator(), false).count();
      final int expect = iterations / thread;

      for (int i = iterations; i > 0; i--) {
        Channel ch = bootstrap.connect(server.localAddress()).sync().channel();

        clientDist.compute(ch.eventLoop(), (eventLoop, integer) -> integer != null ? integer + 1 : 1);
      }

      IntSummaryStatistics serverStats = serverDist.values().stream().mapToInt(Integer::intValue).summaryStatistics();
      IntSummaryStatistics clientStats = clientDist.values().stream().mapToInt(Integer::intValue).summaryStatistics();

      _log.info("Expected average = {}", expect);
      _log.info("Server Stats = {}", serverStats);
      _log.info("Client Stats = {}", clientStats);

      // We check that the average is as expected
      // Also check that the difference between min and max is less than or equal to 2

      Assert.assertEquals(serverStats.getAverage(), expect, 1.0, "Server Unbalanced");
      Assert.assertTrue(serverStats.getMax() - serverStats.getMin() <= 2, "Server Unbalanced");

      Assert.assertEquals(clientStats.getAverage(), expect, 1.0, "Client Unbalanced");
      Assert.assertTrue(clientStats.getMax() - clientStats.getMin() <= 2, "Client Unbalanced");

    } finally {
      server.close().syncUninterruptibly();
    }
  }

  private static class UnbalancedChooser implements EventExecutorChooserFactory {
    @Override
    public EventExecutorChooser newChooser(EventExecutor[] executors) {
      return () -> {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // Random gives a pretty even distribution but we want an uneven distribution.
        // Six random numbers averaged gives a reasonably good approximation of the bell curve.
        int index = (random.nextInt(executors.length) + random.nextInt(executors.length)
            + random.nextInt(executors.length) + random.nextInt(executors.length) + random.nextInt(executors.length)
            + random.nextInt(executors.length)) / 6;
        return executors[index];
      };
    }
  }

}
