package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
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
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/27/18.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestHttpObjectToBasicHttpObjectAdapter extends AbstractLeakDetect {
  @Test
  public void testFullHttpRequest() throws Exception {
    ChannelInboundHandler handler = new HttpObjectToBasicHttpObjectAdapter();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    FullHttpRequest msg = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/test",
        encodeString("Hello world", StandardCharsets.US_ASCII));
    Time.freeze();
    try {
      handler.channelRead(ctx, msg);
      ArgumentCaptor<FullHttpRequest> captor = ArgumentCaptor.forClass(FullHttpRequest.class);
      Mockito.verify(ctx).fireChannelRead(captor.capture());
      Assert.assertNotSame(captor.getValue(), msg);
      BasicFullHttpRequest request = (BasicFullHttpRequest) captor.getValue();
      Assert.assertSame(request.protocolVersion(), msg.protocolVersion());
      Assert.assertSame(request.method(), msg.method());
      Assert.assertSame(request.uri(), msg.uri());
      Assert.assertSame(request.content(), msg.content());
      Assert.assertSame(request.decoderResult(), msg.decoderResult());
      Assert.assertEquals(request.getRequestTimestamp(), Time.currentTimeMillis());
      Assert.assertEquals(request.getRequestNanos(), Time.nanoTime());
      request.release();
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testFullHttpResponse() throws Exception {
    ChannelInboundHandler handler = new HttpObjectToBasicHttpObjectAdapter();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    FullHttpResponse msg = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        encodeString("Hello world", StandardCharsets.US_ASCII));
    Time.freeze();
    try {
      handler.channelRead(ctx, msg);
      ArgumentCaptor<FullHttpResponse> captor = ArgumentCaptor.forClass(FullHttpResponse.class);
      Mockito.verify(ctx).fireChannelRead(captor.capture());
      Assert.assertNotSame(captor.getValue(), msg);
      BasicFullHttpResponse response = (BasicFullHttpResponse) captor.getValue();
      Assert.assertSame(response.protocolVersion(), msg.protocolVersion());
      Assert.assertSame(response.status(), msg.status());
      Assert.assertSame(response.content(), msg.content());
      Assert.assertSame(response.decoderResult(), msg.decoderResult());
      response.release();
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testHttpRequest() throws Exception {
    ChannelInboundHandler handler = new HttpObjectToBasicHttpObjectAdapter();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    HttpRequest msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    Time.freeze();
    try {
      handler.channelRead(ctx, msg);
      ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
      Mockito.verify(ctx).fireChannelRead(captor.capture());
      Assert.assertNotSame(captor.getValue(), msg);
      BasicHttpRequest request = (BasicHttpRequest) captor.getValue();
      Assert.assertFalse(request instanceof FullHttpRequest);
      Assert.assertSame(request.protocolVersion(), msg.protocolVersion());
      Assert.assertSame(request.method(), msg.method());
      Assert.assertSame(request.uri(), msg.uri());
      Assert.assertSame(request.decoderResult(), msg.decoderResult());
      Assert.assertEquals(request.getRequestTimestamp(), Time.currentTimeMillis());
      Assert.assertEquals(request.getRequestNanos(), Time.nanoTime());
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testHttpResponse() throws Exception {
    ChannelInboundHandler handler = new HttpObjectToBasicHttpObjectAdapter();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    HttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    Time.freeze();
    try {
      handler.channelRead(ctx, msg);
      ArgumentCaptor<HttpResponse> captor = ArgumentCaptor.forClass(HttpResponse.class);
      Mockito.verify(ctx).fireChannelRead(captor.capture());
      Assert.assertNotSame(captor.getValue(), msg);
      BasicHttpResponse response = (BasicHttpResponse) captor.getValue();
      Assert.assertFalse(response instanceof FullHttpResponse);
      Assert.assertSame(response.protocolVersion(), msg.protocolVersion());
      Assert.assertSame(response.status(), msg.status());
      Assert.assertSame(response.decoderResult(), msg.decoderResult());
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testHttpContent() throws Exception {
    ChannelInboundHandler handler = new HttpObjectToBasicHttpObjectAdapter();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    HttpContent msg = new DefaultHttpContent(encodeString("Hello world!", StandardCharsets.US_ASCII));
    handler.channelRead(ctx, msg);
    ArgumentCaptor<HttpContent> captor = ArgumentCaptor.forClass(HttpContent.class);
    Mockito.verify(ctx).fireChannelRead(captor.capture());
    Assert.assertSame(captor.getValue(), msg);
    captor.getValue().release();
  }

  /**
   * since TestNG tends to sort by method name, this tries to be the last test
   * in the class. We do this because the AfterClass annotated methods may
   * execute after other tests classes have run and doesn't execute immediately
   * after the methods in this test class.
   */
  @Test(alwaysRun = true)
  public final void zz9PluralZAlpha() throws InterruptedException {
    finallyLeakDetect();
  }
}
