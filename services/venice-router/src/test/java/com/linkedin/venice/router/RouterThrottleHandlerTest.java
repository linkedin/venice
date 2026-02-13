package com.linkedin.venice.router;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.router.stats.RouterThrottleStats;
import com.linkedin.venice.throttle.EventThrottler;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RouterThrottleHandlerTest {
  private RouterThrottleStats routerStats;
  private EventThrottler throttler;
  private VeniceRouterConfig config;
  private ChannelHandlerContext ctx;
  private Channel channel;

  @BeforeMethod
  public void setUp() {
    routerStats = mock(RouterThrottleStats.class);
    throttler = mock(EventThrottler.class);
    config = mock(VeniceRouterConfig.class);
    ctx = mock(ChannelHandlerContext.class);
    channel = mock(Channel.class);

    doReturn(channel).when(ctx).channel();
    doReturn(new InetSocketAddress("localhost", 1234)).when(channel).remoteAddress();
  }

  @Test
  public void testEarlyThrottleDisabledPassesThrough() throws IOException {
    doReturn(false).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Should pass through without throttling
    verify(ctx, times(1)).fireChannelRead(any());
    verify(throttler, never()).maybeThrottle(anyDouble());
  }

  @Test
  public void testOptionsRequestPassesThrough() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // OPTIONS should always pass through
    verify(ctx, times(1)).fireChannelRead(any());
    verify(throttler, never()).maybeThrottle(anyDouble());
  }

  @Test
  public void testNonBasicFullHttpRequestPassesThrough() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);
    // DefaultFullHttpRequest is not a BasicFullHttpRequest
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Non-BasicFullHttpRequest should pass through
    verify(ctx, times(1)).fireChannelRead(any());
    verify(throttler, never()).maybeThrottle(anyDouble());
  }

  @Test
  public void testSingleGetRequestWithBasicFullHttpRequest() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    // Create BasicFullHttpRequest for single-get
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/storage/testStore/testKey",
        Unpooled.EMPTY_BUFFER,
        EmptyHttpHeaders.INSTANCE,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Single-get should throttle with keyCount=1
    verify(throttler, times(1)).maybeThrottle(1.0);
    verify(ctx, times(1)).fireChannelRead(any());
  }

  @Test
  public void testSingleGetThrottledReturns429() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();
    doThrow(new QuotaExceededException("test", "10", "5")).when(throttler).maybeThrottle(anyDouble());

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/storage/testStore/testKey",
        Unpooled.EMPTY_BUFFER,
        EmptyHttpHeaders.INSTANCE,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Should return 429 Too Many Requests
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.TOO_MANY_REQUESTS);
    verify(routerStats, times(1)).recordRouterThrottledRequest();
    verify(ctx, never()).fireChannelRead(any());
  }

  @Test
  public void testBatchGetWithKeyCountHeader() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    // Create headers with key count
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpConstants.VENICE_KEY_COUNT, "10");

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/testStore",
        Unpooled.EMPTY_BUFFER,
        headers,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Should throttle with keyCount from header
    verify(throttler, times(1)).maybeThrottle(10.0);
    verify(ctx, times(1)).fireChannelRead(any());
  }

  @Test
  public void testComputeRequestWithoutHeaderPassesThrough() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    // Compute request without key count header (old client)
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/compute/testStore",
        Unpooled.EMPTY_BUFFER,
        EmptyHttpHeaders.INSTANCE,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Old compute clients without header should pass through
    verify(ctx, times(1)).fireChannelRead(any());
    verify(throttler, never()).maybeThrottle(anyDouble());
  }

  @Test
  public void testAdminRequestPassesThrough() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/admin",
        Unpooled.EMPTY_BUFFER,
        EmptyHttpHeaders.INSTANCE,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Admin requests should not be throttled
    verify(ctx, times(1)).fireChannelRead(any());
    verify(throttler, never()).maybeThrottle(anyDouble());
  }

  @Test
  public void testExceptionCaughtReturnsInternalServerError() {
    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);
    Exception testException = new RuntimeException("Test exception");

    handler.exceptionCaught(ctx, testException);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testBatchGetWithKeyCountHeaderThrottled() throws IOException {
    doReturn(true).when(config).isEarlyThrottleEnabled();
    doThrow(new QuotaExceededException("test", "100", "50")).when(throttler).maybeThrottle(anyDouble());

    RouterThrottleHandler handler = new RouterThrottleHandler(routerStats, throttler, config);

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpConstants.VENICE_KEY_COUNT, "100");

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/testStore",
        Unpooled.EMPTY_BUFFER,
        headers,
        EmptyHttpHeaders.INSTANCE,
        UUID.randomUUID(),
        0,
        0);

    handler.channelRead0(ctx, request);

    // Should return 429 and record throttled request
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.TOO_MANY_REQUESTS);
    verify(routerStats, times(1)).recordRouterThrottledRequest();
  }
}
