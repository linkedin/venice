package com.linkedin.alpini.netty4.http2;

import static org.testng.AssertJUnit.*;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp2StreamFrameClientCodec {
  public void testDowngradeEmptyFullResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameClientCodec());

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());

    assertTrue(ch.writeOutbound(request));
    Http2HeadersFrame requestFrame = ch.readOutbound();
    assertTrue("GET".contentEquals(requestFrame.headers().method()));
    assertTrue(requestFrame.isEndStream());

    Http2Headers headers = new DefaultHttp2Headers();
    headers.status("200");
    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, true)));

    FullHttpResponse response = ch.readInbound();
    assertEquals(200, response.status().code());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeEmptyFullResponseLightweight() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameClientCodec());

    BasicFullHttpRequest request = new Http1FullRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());

    assertTrue(ch.writeOutbound(request));
    Http2HeadersFrame requestFrame = ch.readOutbound();
    assertTrue("GET".contentEquals(requestFrame.headers().method()));
    assertTrue(requestFrame.isEndStream());
    assertSame(requestFrame.headers(), ((Http1Headers) request.headers()).getHttp2Headers());

    Http2Headers headers = new DefaultHttp2Headers();
    headers.status("200");
    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, true)));

    FullHttpResponse response = ch.readInbound();
    assertEquals(200, response.status().code());
    assertSame(headers, ((Http1Headers) response.headers()).getHttp2Headers());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeNonEmptyFullResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameClientCodec());

    BasicFullHttpRequest request = new Http1FullRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());

    assertTrue(ch.writeOutbound(request));
    Http2HeadersFrame requestFrame = ch.readOutbound();
    assertTrue("GET".contentEquals(requestFrame.headers().method()));
    assertTrue(requestFrame.isEndStream());
    assertSame(requestFrame.headers(), ((Http1Headers) request.headers()).getHttp2Headers());

    Http2Headers headers = new DefaultHttp2Headers();
    headers.status("200");
    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, false)));

    ByteBuf content = ByteBufUtil.encodeString(
        UnpooledByteBufAllocator.DEFAULT,
        CharBuffer.wrap("Hello World".toCharArray()),
        StandardCharsets.UTF_8);
    assertTrue(ch.writeInbound(new DefaultHttp2DataFrame(content, true)));

    HttpResponse response = ch.readInbound();
    assertEquals(200, response.status().code());
    assertSame(headers, ((Http1Headers) response.headers()).getHttp2Headers());
    assertFalse(response instanceof FullHttpResponse);
    HttpContent httpContent = ch.readInbound();
    assertSame(httpContent.content(), content);
    assertTrue(httpContent instanceof LastHttpContent);

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeNonEmptyFullResponse2() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameClientCodec());

    BasicFullHttpRequest request = new Http1FullRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/hello/world",
        Time.currentTimeMillis(),
        Time.nanoTime());

    assertTrue(ch.writeOutbound(request));
    Http2HeadersFrame requestFrame = ch.readOutbound();
    assertTrue("GET".contentEquals(requestFrame.headers().method()));
    assertTrue(requestFrame.isEndStream());
    assertSame(requestFrame.headers(), ((Http1Headers) request.headers()).getHttp2Headers());

    Http2Headers headers = new DefaultHttp2Headers();
    headers.status("200");
    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, false)));

    ByteBuf content1 = ByteBufUtil.encodeString(
        UnpooledByteBufAllocator.DEFAULT,
        CharBuffer.wrap("Hello World!".toCharArray()),
        StandardCharsets.UTF_8);
    ByteBuf content2 = ByteBufUtil.encodeString(
        UnpooledByteBufAllocator.DEFAULT,
        CharBuffer.wrap("Goodbye World!".toCharArray()),
        StandardCharsets.UTF_8);
    assertTrue(ch.writeInbound(new DefaultHttp2DataFrame(content1, false)));
    assertTrue(ch.writeInbound(new DefaultHttp2DataFrame(content2, true)));

    HttpResponse response = ch.readInbound();
    assertEquals(200, response.status().code());
    assertSame(headers, ((Http1Headers) response.headers()).getHttp2Headers());
    assertFalse(response instanceof FullHttpResponse);

    HttpContent httpContent = ch.readInbound();
    assertSame(httpContent.content(), content1);
    assertFalse(httpContent instanceof LastHttpContent);

    httpContent = ch.readInbound();
    assertSame(httpContent.content(), content2);
    assertTrue(httpContent instanceof LastHttpContent);

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }
}
