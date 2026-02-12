package com.linkedin.venice.router;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.router.stats.SecurityStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RouterSslVerificationHandlerTest {
  private SecurityStats securityStats;
  private ChannelHandlerContext ctx;
  private Channel channel;
  private ChannelPipeline pipeline;
  private Channel parentChannel;
  private ChannelPipeline parentPipeline;

  @BeforeMethod
  public void setUp() {
    securityStats = mock(SecurityStats.class);
    ctx = mock(ChannelHandlerContext.class);
    channel = mock(Channel.class);
    pipeline = mock(ChannelPipeline.class);
    parentChannel = mock(Channel.class);
    parentPipeline = mock(ChannelPipeline.class);

    doReturn(channel).when(ctx).channel();
    doReturn(pipeline).when(ctx).pipeline();
    doReturn(new InetSocketAddress("localhost", 1234)).when(channel).remoteAddress();
    doReturn(parentChannel).when(channel).parent();
    doReturn(parentPipeline).when(parentChannel).pipeline();
  }

  @Test
  public void testRequestWithSslHandlerPassesThrough() throws IOException {
    SslHandler sslHandler = mock(SslHandler.class);
    doReturn(sslHandler).when(pipeline).get(SslHandler.class);

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Should pass through to next handler
    verify(ctx, times(1)).fireChannelRead(any());
    verify(ctx, never()).writeAndFlush(any());
    verify(securityStats, times(1)).updateConnectionCountInCurrentMetricTimeWindow();
    verify(securityStats, never()).recordNonSslRequest();
  }

  @Test
  public void testRequestWithSslHandlerInParentPipelinePassesThrough() throws IOException {
    // No SSL in direct pipeline
    doReturn(null).when(pipeline).get(SslHandler.class);
    // SSL in parent pipeline (HTTP/2 case)
    SslHandler sslHandler = mock(SslHandler.class);
    doReturn(sslHandler).when(parentPipeline).get(SslHandler.class);

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Should pass through to next handler
    verify(ctx, times(1)).fireChannelRead(any());
    verify(ctx, never()).writeAndFlush(any());
    verify(securityStats, never()).recordNonSslRequest();
  }

  @Test
  public void testRequestWithoutSslHandlerReturnsForbiddenWhenRequired() throws IOException {
    doReturn(null).when(pipeline).get(SslHandler.class);
    doReturn(null).when(parentPipeline).get(SslHandler.class);

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.FORBIDDEN);
    verify(securityStats, times(1)).recordNonSslRequest();
    verify(ctx, times(1)).close();
    verify(ctx, never()).fireChannelRead(any());
  }

  @Test
  public void testRequestWithoutSslHandlerPassesThroughWhenNotRequired() throws IOException {
    doReturn(null).when(pipeline).get(SslHandler.class);
    doReturn(null).when(parentPipeline).get(SslHandler.class);

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, false);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Should pass through even without SSL when not required
    verify(ctx, times(1)).fireChannelRead(any());
    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, never()).close();
    verify(securityStats, times(1)).recordNonSslRequest();
  }

  @Test
  public void testSslHandshakeSuccessRecordsMetric() {
    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    SslHandshakeCompletionEvent successEvent = SslHandshakeCompletionEvent.SUCCESS;

    handler.userEventTriggered(ctx, successEvent);

    verify(securityStats, times(1)).recordSslSuccess();
    verify(securityStats, never()).recordSslError();
    verify(ctx, times(1)).fireUserEventTriggered(successEvent);
    verify(ctx, never()).close();
  }

  @Test
  public void testSslHandshakeFailureRecordsMetric() {
    doReturn(pipeline).when(ctx).pipeline();

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    Exception cause = new RuntimeException("SSL handshake failed");
    SslHandshakeCompletionEvent failureEvent = new SslHandshakeCompletionEvent(cause);

    handler.userEventTriggered(ctx, failureEvent);

    verify(securityStats, times(1)).recordSslError();
    verify(securityStats, never()).recordSslSuccess();
    verify(ctx, times(1)).close();
    verify(pipeline, times(1)).remove(handler);
  }

  @Test
  public void testNonSslHandshakeEventPassesThrough() {
    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    Object otherEvent = new Object();

    handler.userEventTriggered(ctx, otherEvent);

    verify(ctx, times(1)).fireUserEventTriggered(otherEvent);
    verify(securityStats, never()).recordSslSuccess();
    verify(securityStats, never()).recordSslError();
  }

  @Test
  public void testDefaultConstructorRequiresSsl() throws IOException {
    doReturn(null).when(pipeline).get(SslHandler.class);
    doReturn(null).when(parentPipeline).get(SslHandler.class);

    // Use default constructor which should default to requireSsl=true
    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    handler.channelRead0(ctx, request);

    // Should return 403 because SSL is required by default
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.FORBIDDEN);
  }

  @Test
  public void testConnectionCountUpdatedOnEveryRequest() throws IOException {
    SslHandler sslHandler = mock(SslHandler.class);
    doReturn(sslHandler).when(pipeline).get(SslHandler.class);

    RouterSslVerificationHandler handler = new RouterSslVerificationHandler(securityStats, true);
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/store/key");

    // Make multiple requests
    handler.channelRead0(ctx, request);
    handler.channelRead0(ctx, request);
    handler.channelRead0(ctx, request);

    // Connection count should be updated for each request
    verify(securityStats, times(3)).updateConnectionCountInCurrentMetricTimeWindow();
  }
}
