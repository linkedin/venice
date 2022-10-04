package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/1/18.
 */
public class TestHttpServerStatisticsHandler {
  @Test(groups = "unit")
  public void testHttpLifecycle() throws InterruptedException {

    HttpServerStatisticsHandler handler = Mockito.spy(HttpServerStatisticsHandler.class);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Unpooled.EMPTY_BUFFER);
    ch.writeOneInbound(request).sync();
    Assert.assertSame(ch.readInbound(), request);

    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII));
    HttpUtil.setContentLength(response, response.content().readableBytes());
    ChannelFuture writeFuture = ch.writeOneOutbound(response);
    Thread.yield();
    ch.flushOutbound();
    Thread.yield();
    Assert.assertSame(ch.readOutbound(), response);
    writeFuture.sync();

    ArgumentCaptor<HttpServerStatisticsHandler.Stats> stats =
        ArgumentCaptor.forClass(HttpServerStatisticsHandler.Stats.class);

    Mockito.verify(handler).complete(stats.capture());

    Optional.of(stats.getValue()).ifPresent(s -> {
      Assert.assertEquals(s._requestHeaderReceivedTime, s._requestContentReceivedTime);
      Assert.assertEquals(s._responseContentReadyTime, s._responseHeaderReadyTime);
      Assert.assertTrue(s._aggregateReadProcessingTime > 0);
      Assert.assertTrue(s._responseContentWrittenTime > s._responseContentReadyTime);
      Assert.assertTrue(s._responseContentReadyTime > s._requestContentReceivedTime);

      System.out.print(s);
    });
  }

  @Test(groups = "unit")
  public void testHttpLifecycle2() throws InterruptedException {

    HttpServerStatisticsHandler handler = Mockito.spy(HttpServerStatisticsHandler.class);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    HttpUtil.setTransferEncodingChunked(request, true);
    ch.writeOneInbound(request).sync();
    Assert.assertSame(ch.readInbound(), request);

    HttpContent requestContent =
        new DefaultHttpContent(Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII));
    ch.writeOneInbound(requestContent).sync();
    Assert.assertSame(ch.readInbound(), requestContent);

    ch.writeOneInbound(LastHttpContent.EMPTY_LAST_CONTENT).sync();
    Assert.assertSame(ch.readInbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertNull(ch.readInbound());

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);

    HttpContent responseContent1 = new DefaultHttpContent(Unpooled.copiedBuffer("Goodbye ", StandardCharsets.US_ASCII));
    HttpContent responseContent2 = new DefaultHttpContent(Unpooled.copiedBuffer("world", StandardCharsets.US_ASCII));

    long expectedResponseHeaderReady;
    try {
      Time.freeze();
      expectedResponseHeaderReady = Time.nanoTime();
      ch.writeOutbound(response);
      Assert.assertSame(ch.readOutbound(), response);
    } finally {
      Time.restore();
      Thread.yield();
    }

    ch.writeOutbound(responseContent1);
    Thread.yield();
    Assert.assertSame(ch.readOutbound(), responseContent1);

    ch.writeOutbound(responseContent2);
    Thread.yield();
    Assert.assertSame(ch.readOutbound(), responseContent2);

    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
    Thread.yield();
    Assert.assertSame(ch.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);

    Assert.assertNull(ch.readOutbound());

    ArgumentCaptor<HttpServerStatisticsHandler.Stats> stats =
        ArgumentCaptor.forClass(HttpServerStatisticsHandler.Stats.class);

    Mockito.verify(handler).complete(stats.capture());

    Optional.of(stats.getValue()).ifPresent(s -> {
      Assert.assertNotEquals(s._requestHeaderReceivedTime, s._requestContentReceivedTime);
      Assert.assertNotEquals(s._responseContentReadyTime, s._responseHeaderReadyTime);
      Assert.assertTrue(s._aggregateReadProcessingTime > 0);
      Assert.assertTrue(s._responseContentWrittenTime > s._responseContentReadyTime);
      Assert.assertTrue(s._responseContentReadyTime > s._requestContentReceivedTime);

      Assert.assertEquals(s._responseHeaderReadyTime, expectedResponseHeaderReady);

      System.out.print(s);
    });

  }

  @Test(groups = "unit")
  public void testHttpLifecycle3() throws InterruptedException {

    HttpServerStatisticsHandler handler = Mockito.spy(HttpServerStatisticsHandler.class);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/",
        Unpooled.EMPTY_BUFFER,
        Time.currentTimeMillis(),
        Time.nanoTime());
    Thread.yield();

    ch.writeOneInbound(request).sync();
    Assert.assertSame(ch.readInbound(), request);

    BasicFullHttpResponse response = new BasicFullHttpResponse(
        request,
        HttpResponseStatus.OK,
        Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII));
    HttpUtil.setContentLength(response, response.content().readableBytes());
    ChannelFuture writeFuture = ch.writeOneOutbound(response);
    Thread.yield();
    ch.flushOutbound();
    Thread.yield();
    Assert.assertSame(ch.readOutbound(), response);
    writeFuture.sync();

    ArgumentCaptor<HttpServerStatisticsHandler.Stats> stats =
        ArgumentCaptor.forClass(HttpServerStatisticsHandler.Stats.class);

    Mockito.verify(handler).complete(stats.capture());

    Optional.of(stats.getValue()).ifPresent(s -> {
      Assert.assertNotEquals(s._requestHeaderReceivedTime, s._requestContentReceivedTime);
      Assert.assertEquals(s._responseContentReadyTime, s._responseHeaderReadyTime);
      Assert.assertTrue(s._aggregateReadProcessingTime > 0);
      Assert.assertTrue(s._responseContentWrittenTime > s._responseContentReadyTime);
      Assert.assertTrue(s._responseContentReadyTime > s._requestContentReceivedTime);

      System.out.print(s);
    });
  }

}
