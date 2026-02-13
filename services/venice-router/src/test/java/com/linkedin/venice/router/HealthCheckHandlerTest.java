package com.linkedin.venice.router;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.router.stats.HealthCheckStats;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HealthCheckHandlerTest {
  private HealthCheckStats healthCheckStats;
  private HealthCheckHandler handler;
  private ChannelHandlerContext ctx;
  private Channel channel;

  @BeforeMethod
  public void setUp() {
    healthCheckStats = mock(HealthCheckStats.class);
    handler = new HealthCheckHandler(healthCheckStats);
    ctx = mock(ChannelHandlerContext.class);
    channel = mock(Channel.class);
    doReturn(channel).when(ctx).channel();
    doReturn(new InetSocketAddress("localhost", 1234)).when(channel).remoteAddress();
  }

  @Test
  public void testOptionsRequestReturnsOK() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/any/path");

    handler.channelRead0(ctx, request);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    try {
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      verify(healthCheckStats, times(1)).recordHealthCheck();
    } finally {
      response.release();
    }
  }

  @Test
  public void testGetAdminWithoutResourceReturnsOK() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/admin");

    handler.channelRead0(ctx, request);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    try {
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      verify(healthCheckStats, times(1)).recordHealthCheck();
    } finally {
      response.release();
    }
  }

  @Test
  public void testGetAdminWithTrailingSlashReturnsOK() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/admin/");

    handler.channelRead0(ctx, request);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    try {
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      verify(healthCheckStats, times(1)).recordHealthCheck();
    } finally {
      response.release();
    }
  }

  @Test
  public void testGetAdminWithResourcePassesThrough() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/admin/storeName");

    handler.channelRead0(ctx, request);

    // Should pass through to next handler, not write response
    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(1)).fireChannelRead(any());
    verify(healthCheckStats, never()).recordHealthCheck();
  }

  @Test
  public void testGetStoragePassesThrough() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/storeName/key");

    handler.channelRead0(ctx, request);

    // Should pass through to next handler
    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(1)).fireChannelRead(any());
    verify(healthCheckStats, never()).recordHealthCheck();
  }

  @Test
  public void testPostRequestPassesThrough() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/admin");

    handler.channelRead0(ctx, request);

    // POST should pass through even to /admin
    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(1)).fireChannelRead(any());
    verify(healthCheckStats, never()).recordHealthCheck();
  }

  @Test
  public void testExceptionCaughtReturnsInternalServerError() {
    Exception testException = new RuntimeException("Test exception");

    handler.exceptionCaught(ctx, testException);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    try {
      Assert.assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      verify(healthCheckStats, times(1)).recordErrorHealthCheck();
      verify(ctx, times(1)).close();
    } finally {
      response.release();
    }
  }

  @Test
  public void testExceptionCaughtWithRedundantExceptionStillRecordsMetric() {
    // Even redundant exceptions should record the error metric
    Exception testException = new RuntimeException("Connection reset by peer");

    handler.exceptionCaught(ctx, testException);

    verify(healthCheckStats, times(1)).recordErrorHealthCheck();
    verify(ctx, times(1)).close();
  }

  @Test
  public void testHealthCheckResponseHasEmptyBody() {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/");

    handler.channelRead0(ctx, request);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    try {
      ByteBuf content = response.content();
      Assert.assertEquals(content.readableBytes(), 0);
    } finally {
      response.release();
    }
  }
}
