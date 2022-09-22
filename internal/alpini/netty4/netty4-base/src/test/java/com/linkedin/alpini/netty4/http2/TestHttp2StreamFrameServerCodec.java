/*
 * Copyright 2017 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.alpini.netty4.http2;

import static org.testng.AssertJUnit.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.CharsetUtil;
import java.util.Iterator;
import java.util.Map;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp2StreamFrameServerCodec {
  public void testUpgradeEmptyFullResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    assertTrue(ch.writeOutbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertTrue(headersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeEmptyFullResponseFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    FullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK);
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertTrue(headersFrame.isEndStream());
    assertSame(((Http1Headers) response.headers()).getHttp2Headers(), headersFrame.headers());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void encode100ContinueAsHttp2HeadersFrameThatIsNotEndStream() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    assertTrue(ch.writeOutbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE)));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("100", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void encode100ContinueAsHttp2HeadersFrameThatIsNotEndStreamFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    FullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.CONTINUE);
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("100", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());
    assertSame(((Http1Headers) response.headers()).getHttp2Headers(), headersFrame.headers());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  @Test(expectedExceptions = EncoderException.class)
  public void encodeNonFullHttpResponse100ContinueIsRejected() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    try {
      ch.writeOutbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(expectedExceptions = EncoderException.class)
  public void encodeNonFullHttpResponse100ContinueIsRejectedFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    try {
      ch.writeOutbound(new Http1Response(request, HttpResponseStatus.CONTINUE));
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  public void testUpgradeNonEmptyFullResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    assertTrue(ch.writeOutbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, hello)));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertTrue(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeNonEmptyFullResponseFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    FullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK, hello);
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());
    assertSame(((Http1Headers) response.headers()).getHttp2Headers(), headersFrame.headers());

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertTrue(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeEmptyFullResponseWithTrailers() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpHeaders trailers = response.trailingHeaders();
    trailers.set("key", "value");
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    Http2HeadersFrame trailersFrame = ch.readOutbound();
    assertEquals("value", trailersFrame.headers().get("key").toString());
    assertTrue(trailersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeEmptyFullResponseWithTrailersFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    FullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK);
    HttpHeaders trailers = response.trailingHeaders();
    trailers.set("key", "value");
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    Http2HeadersFrame trailersFrame = ch.readOutbound();
    assertEquals("value", trailersFrame.headers().get("key").toString());
    assertTrue(trailersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeNonEmptyFullResponseWithTrailers() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, hello);
    HttpHeaders trailers = response.trailingHeaders();
    trailers.set("key", "value");
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertFalse(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    Http2HeadersFrame trailersFrame = ch.readOutbound();
    assertEquals("value", trailersFrame.headers().get("key").toString());
    assertTrue(trailersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeNonEmptyFullResponseWithTrailersFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    FullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK, hello);
    HttpHeaders trailers = response.trailingHeaders();
    trailers.set("key", "value");
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertFalse(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    Http2HeadersFrame trailersFrame = ch.readOutbound();
    assertEquals("value", trailersFrame.headers().get("key").toString());
    assertTrue(trailersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeHeaders() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeHeadersFast() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    HttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    HttpResponse response = new Http1Response(request, HttpResponseStatus.OK);
    assertTrue(ch.writeOutbound(response));

    Http2HeadersFrame headersFrame = ch.readOutbound();
    assertEquals("200", headersFrame.headers().status().toString());
    assertFalse(headersFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeChunk() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    HttpContent content = new DefaultHttpContent(hello);
    assertTrue(ch.writeOutbound(content));

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertFalse(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeEmptyEnd() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    LastHttpContent end = LastHttpContent.EMPTY_LAST_CONTENT;
    assertTrue(ch.writeOutbound(end));

    Http2DataFrame emptyFrame = ch.readOutbound();
    try {
      assertEquals(0, emptyFrame.content().readableBytes());
      assertTrue(emptyFrame.isEndStream());
    } finally {
      emptyFrame.release();
    }

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeDataEnd() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    LastHttpContent end = new DefaultLastHttpContent(hello, true);
    assertTrue(ch.writeOutbound(end));

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertTrue(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeTrailers() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    LastHttpContent trailers = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, true);
    HttpHeaders headers = trailers.trailingHeaders();
    headers.set("key", "value");
    assertTrue(ch.writeOutbound(trailers));

    Http2HeadersFrame headerFrame = ch.readOutbound();
    assertEquals("value", headerFrame.headers().get("key").toString());
    assertTrue(headerFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testUpgradeDataEndWithTrailers() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    LastHttpContent trailers = new DefaultLastHttpContent(hello, true);
    HttpHeaders headers = trailers.trailingHeaders();
    headers.set("key", "value");
    assertTrue(ch.writeOutbound(trailers));

    Http2DataFrame dataFrame = ch.readOutbound();
    try {
      assertEquals("hello world", dataFrame.content().toString(CharsetUtil.UTF_8));
      assertFalse(dataFrame.isEndStream());
    } finally {
      dataFrame.release();
    }

    Http2HeadersFrame headerFrame = ch.readOutbound();
    assertEquals("value", headerFrame.headers().get("key").toString());
    assertTrue(headerFrame.isEndStream());

    assertNull(ch.readOutbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeHeaders() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    Http2Headers headers = new DefaultHttp2Headers();
    headers.path("/");
    headers.method("GET");

    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers)));

    HttpRequest request = ch.readInbound();
    assertEquals("/", String.valueOf(request.uri()));
    assertSame(HttpMethod.GET, request.method());
    // assertEquals(HttpVersion.HTTP_1_1, request.protocolVersion());
    assertSame(Http2StreamFrameServerCodec.HTTP_2_0, request.protocolVersion());
    assertFalse(request instanceof FullHttpRequest);
    assertTrue(HttpUtil.isTransferEncodingChunked(request));
    assertSame(headers, ((Http1Headers) request.headers()).getHttp2Headers());

    Iterator<Map.Entry<String, String>> it = request.headers().iteratorAsString();
    assertTrue(it.hasNext());
    Map.Entry<String, String> entry = it.next();
    assertEquals("x-http2-path", entry.getKey());
    assertEquals("/", entry.getValue());
    entry = it.next();
    assertEquals("x-http2-stream-id", entry.getKey());
    entry = it.next();
    assertEquals("transfer-encoding", entry.getKey()); // because no content-length was specified.
    assertEquals("chunked", entry.getValue());
    assertFalse(it.hasNext());

    assertEquals("0", request.headers().get("x-http2-stream-id"));
    assertEquals("/", request.headers().get(HttpConversionUtil.ExtensionHeaderNames.PATH.text()));
    assertNull(request.headers().get(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text()));
    assertEquals("none", request.headers().get(HttpHeaderNames.HOST, "none"));
    assertNull(request.headers().get(Http2Headers.PseudoHeaderName.PATH.value()));

    request.headers().set(HttpHeaderNames.HOST, "localhost");
    assertEquals("localhost", headers.authority().toString());
    request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");
    assertEquals("http", headers.scheme().toString());
    request.headers().set(HttpConversionUtil.ExtensionHeaderNames.PATH.text(), "/foo");
    assertEquals("/foo", headers.path().toString());
    request.headers().set(Http2Headers.PseudoHeaderName.PATH.value(), "/bar");
    assertEquals("/foo", headers.path().toString());

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeHeadersWithContentLength() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    Http2Headers headers = new DefaultHttp2Headers();
    headers.path("/");
    headers.method("GET");
    headers.setInt("content-length", 0);

    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers)));

    HttpRequest request = ch.readInbound();
    assertEquals("/", String.valueOf(request.uri()));
    assertSame(HttpMethod.GET, request.method());
    // assertEquals(HttpVersion.HTTP_1_1, request.protocolVersion());
    assertSame(Http2StreamFrameServerCodec.HTTP_2_0, request.protocolVersion());
    assertFalse(request instanceof FullHttpRequest);
    assertFalse(HttpUtil.isTransferEncodingChunked(request));
    assertSame(headers, ((Http1Headers) request.headers()).getHttp2Headers());

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeFullHeaders() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    Http2Headers headers = new DefaultHttp2Headers();
    headers.path("/");
    headers.method("GET");

    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, true)));

    FullHttpRequest request = ch.readInbound();
    try {
      assertEquals("/", String.valueOf(request.uri()));
      assertSame(HttpMethod.GET, request.method());
      // assertSame(HttpVersion.HTTP_1_1, request.protocolVersion());
      assertSame(Http2StreamFrameServerCodec.HTTP_2_0, request.protocolVersion());
      assertEquals(0, request.content().readableBytes());
      assertTrue(request.trailingHeaders().isEmpty());
      assertFalse(HttpUtil.isTransferEncodingChunked(request));
      assertSame(headers, ((Http1Headers) request.headers()).getHttp2Headers());
    } finally {
      request.release();
    }

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeTrailers() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    Http2Headers headers = new DefaultHttp2Headers();
    headers.set("key", "value");
    assertTrue(ch.writeInbound(new DefaultHttp2HeadersFrame(headers, true)));

    LastHttpContent trailers = ch.readInbound();
    try {
      assertEquals(0, trailers.content().readableBytes());
      assertEquals("value", trailers.trailingHeaders().get("key"));
      assertFalse(trailers instanceof FullHttpRequest);
    } finally {
      trailers.release();
    }

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeData() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    assertTrue(ch.writeInbound(new DefaultHttp2DataFrame(hello)));

    HttpContent content = ch.readInbound();
    try {
      assertEquals("hello world", content.content().toString(CharsetUtil.UTF_8));
      assertFalse(content instanceof LastHttpContent);
    } finally {
      content.release();
    }

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testDowngradeEndData() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    ByteBuf hello = Unpooled.copiedBuffer("hello world", CharsetUtil.UTF_8);
    assertTrue(ch.writeInbound(new DefaultHttp2DataFrame(hello, true)));

    LastHttpContent content = ch.readInbound();
    try {
      assertEquals("hello world", content.content().toString(CharsetUtil.UTF_8));
      assertTrue(content.trailingHeaders().isEmpty());
    } finally {
      content.release();
    }

    assertNull(ch.readInbound());
    assertFalse(ch.finish());
  }

  public void testPassThroughOther() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2StreamFrameServerCodec());
    Http2ResetFrame reset = new DefaultHttp2ResetFrame(0);
    Http2GoAwayFrame goaway = new DefaultHttp2GoAwayFrame(0);
    assertTrue(ch.writeInbound(reset));
    assertTrue(ch.writeInbound(goaway.retain()));

    assertEquals(reset, ch.readInbound());

    Http2GoAwayFrame frame = ch.readInbound();
    try {
      assertEquals(goaway, frame);
      assertNull(ch.readInbound());
      assertFalse(ch.finish());
    } finally {
      goaway.release();
      frame.release();
    }
  }
}
