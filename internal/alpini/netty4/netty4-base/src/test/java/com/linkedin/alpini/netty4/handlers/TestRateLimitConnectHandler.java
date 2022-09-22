package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.io.IOUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import org.testng.Assert;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/19/18.
 */
public class TestRateLimitConnectHandler {
  NioEventLoopGroup eventLoopGroup;

  ScheduledExecutorService scheduler;

  @BeforeTest(groups = "unit")
  public void beforeTest() {
    eventLoopGroup = new NioEventLoopGroup();

    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterTest(groups = "unit")
  public void afterTest() {
    eventLoopGroup.shutdownGracefully();
    scheduler.shutdown();
  }

  @DataProvider
  public Object[][] provideTimeBetweenAccepts() {
    return new Object[][] { { 10, 500 }, { 50, 100 }, };
  }

  /** Timing related test needs retries */
  public static class RetryTimeBetweenAccepts implements IRetryAnalyzer {
    int remaining = 5;

    @Override
    public boolean retry(ITestResult result) {
      if (!result.isSuccess() && remaining-- > 0) {
        result.setStatus(ITestResult.SUCCESS_PERCENTAGE_FAILURE);
        Reporter.log("Retries remaining: " + remaining);
        return true;
      }
      return false;
    }
  }

  @Test(groups = "unit", dataProvider = "provideTimeBetweenAccepts", retryAnalyzer = RetryTimeBetweenAccepts.class, successPercentage = 10)
  public void testTimeBetweenAccepts(final int limit, final int delay) throws IOException {
    List<Socket> accepted = new ArrayList<>(limit);
    List<Future<Channel>> acquires = new ArrayList<>(limit);
    try (final ServerSocket serverSocket = new ServerSocket(0)) {
      int port = serverSocket.getLocalPort();
      Assert.assertNotEquals(port, 0);

      List<Long> interval = new ArrayList<>(limit);

      RateLimitConnectHandler handler = new RateLimitConnectHandler(scheduler, delay, delay * 10);

      FixedChannelPool pool = new FixedChannelPool(
          new Bootstrap().group(eventLoopGroup).channel(NioSocketChannel.class).remoteAddress("localhost", port),
          new ChannelPoolHandler() {
            @Override
            public void channelReleased(Channel ch) throws Exception {

            }

            @Override
            public void channelAcquired(Channel ch) throws Exception {

            }

            @Override
            public void channelCreated(Channel ch) throws Exception {
              ch.pipeline().addLast(handler).addLast(new LoggingHandler(LogLevel.INFO));
            }
          },
          limit,
          Integer.MAX_VALUE);

      for (int i = 0; i < limit; i++) {
        acquires.add(pool.acquire());
      }

      accepted.add(serverSocket.accept());
      long time = Time.currentTimeMillis();

      for (int i = 1; i < limit; i++) {
        accepted.add(serverSocket.accept());
        long prev = time;
        time = Time.currentTimeMillis();
        interval.add(time - prev);
      }

      IntSummaryStatistics stats = interval.stream().mapToInt(Long::intValue).summaryStatistics();

      // Check that the time between receiving connections is at least the delay-1 ms
      // and that the average time is no more than the delay + 10 ms.
      Assert.assertTrue(stats.getMin() >= delay - 1, "stats=" + stats);
      Assert.assertTrue(stats.getAverage() < delay + 10, "stats=" + stats);

    } finally {
      accepted.forEach(IOUtils::closeQuietly);
      acquires.stream().map(Future::getNow).forEach(Channel::close);
    }
  }
}
