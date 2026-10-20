package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.Future;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerNettyThreadStatsHandlerTest {
  private MetricsRepository metricsRepository;
  private ServerNettyThreadStatsHandler handler;

  @BeforeMethod
  public void setUp() {
    metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    handler = new ServerNettyThreadStatsHandler(metricsRepository);
  }

  @AfterMethod
  public void tearDown() {
    metricsRepository.close();
  }

  @Test
  public void testForwardingAndSequentialRequestsAcrossChannels() {
    EmbeddedChannel first = new EmbeddedChannel(handler);
    EmbeddedChannel second = new EmbeddedChannel(handler);
    try {
      Object nonRequest = new Object();
      assertTrue(first.writeInbound(nonRequest));
      assertSame(first.readInbound(), nonRequest);
      assertTrue(Double.isNaN(metric("Avg")), "Non-request messages must not contribute samples");

      HttpRequest firstRequest = request();
      HttpRequest secondRequest = request();
      assertTrue(first.writeInbound(firstRequest));
      assertTrue(second.writeInbound(secondRequest));
      assertSame(first.readInbound(), firstRequest);
      assertSame(second.readInbound(), secondRequest);
      assertEquals(metric("Avg"), 1.0);
      assertEquals(metric("Max"), 1.0);
    } finally {
      first.finishAndReleaseAll();
      second.finishAndReleaseAll();
    }
  }

  @Test(timeOut = 30000)
  public void testConcurrentWorkersAreSampledAtRequestArrival() throws Exception {
    NioEventLoopGroup workers = new NioEventLoopGroup(2);
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ChannelHandlerContext firstContext = blockingContext(firstEntered, release);
    ChannelHandlerContext secondContext = blockingContext(secondEntered, release);
    try {
      Future<?> first = workers.next().submit(() -> handler.channelRead(firstContext, request()));
      assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
      Future<?> second = workers.next().submit(() -> handler.channelRead(secondContext, request()));
      assertTrue(secondEntered.await(5, TimeUnit.SECONDS));

      assertEquals(metric("Avg"), 1.5);
      assertEquals(metric("Max"), 2.0);
      release.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);

      handler.channelRead(mock(ChannelHandlerContext.class), request());
      assertEquals(metric("Avg"), 4.0 / 3);
      assertEquals(metric("Max"), 2.0);
    } finally {
      release.countDown();
      workers.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  public void testChunkedRequestIsSampledOnlyAfterAggregation() {
    EmbeddedChannel channel = new EmbeddedChannel(new HttpObjectAggregator(1024), handler);
    try {
      assertFalse(channel.writeInbound(request()));
      assertFalse(channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[] { 1, 2 }))));
      assertTrue(Double.isNaN(metric("Avg")));

      assertTrue(channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT));
      FullHttpRequest aggregatedRequest = channel.readInbound();
      try {
        assertEquals(aggregatedRequest.content().readableBytes(), 2);
        assertEquals(metric("Avg"), 1.0);
        assertEquals(metric("Max"), 1.0);
      } finally {
        aggregatedRequest.release();
      }
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void testNestedRequestsOnOneThreadAreCountedOnce() throws Exception {
    ChannelHandlerContext outerContext = mock(ChannelHandlerContext.class);
    ChannelHandlerContext innerContext = mock(ChannelHandlerContext.class);
    HttpRequest outerRequest = request();
    HttpRequest innerRequest = request();
    doAnswer(invocation -> {
      handler.channelRead(innerContext, innerRequest);
      return outerContext;
    }).when(outerContext).fireChannelRead(outerRequest);

    handler.channelRead(outerContext, outerRequest);
    handler.channelRead(mock(ChannelHandlerContext.class), request());
    requestOnAnotherWorker();

    verify(innerContext).fireChannelRead(innerRequest);
    assertEquals(metric("Avg"), 1.0);
    assertEquals(metric("Max"), 1.0);
  }

  @Test
  public void testExceptionReleasesThreadAndPreservesFailure() throws Exception {
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    HttpRequest request = request();
    doThrow(new IllegalStateException("request failed")).when(context).fireChannelRead(request);

    assertThrows(IllegalStateException.class, () -> handler.channelRead(context, request));
    requestOnAnotherWorker();
    handler.channelRead(mock(ChannelHandlerContext.class), request());

    assertEquals(metric("Avg"), 1.0);
    assertEquals(metric("Max"), 1.0);
  }

  private ChannelHandlerContext blockingContext(CountDownLatch entered, CountDownLatch release) {
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    doAnswer(invocation -> {
      entered.countDown();
      assertTrue(release.await(10, TimeUnit.SECONDS), "Request processing was not released");
      return context;
    }).when(context).fireChannelRead(any());
    return context;
  }

  private void requestOnAnotherWorker() throws Exception {
    NioEventLoopGroup worker = new NioEventLoopGroup(1);
    try {
      worker.next()
          .submit(() -> handler.channelRead(mock(ChannelHandlerContext.class), request()))
          .get(5, TimeUnit.SECONDS);
    } finally {
      worker.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    }
  }

  private double metric(String stat) {
    return metricsRepository.getMetric(".netty_worker_thread_pool--active_request_thread_count." + stat).value();
  }

  private HttpRequest request() {
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health");
  }
}
