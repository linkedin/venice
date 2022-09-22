/*
 * Copyright 2012 The Netty Project
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
package com.linkedin.alpini.netty4.handlers;

import static io.netty.util.AsciiString.of;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpRequestDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.DecoderResultProvider;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;


@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestBasicHttpObjectAggregator extends AbstractLeakDetect {
  @Test
  public void testAggregate() {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(1024 * 1024);
    EmbeddedChannel embedder = new EmbeddedChannel(aggr);

    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://localhost");
    message.headers().set(of("X-Test"), true);
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));
    HttpContent chunk3 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));
    assertFalse(embedder.writeInbound(chunk2));

    // this should trigger a channelRead event so return true
    assertTrue(embedder.writeInbound(chunk3));
    assertTrue(embedder.finish());
    FullHttpRequest aggregatedMessage = embedder.readInbound();
    assertNotNull(aggregatedMessage);

    assertEquals(
        chunk1.content().readableBytes() + chunk2.content().readableBytes(),
        HttpUtil.getContentLength(aggregatedMessage));
    assertEquals(Boolean.TRUE.toString(), aggregatedMessage.headers().get(of("X-Test")));
    checkContentBuffer(aggregatedMessage);
    assertNull(embedder.readInbound());
  }

  private static void checkContentBuffer(FullHttpRequest aggregatedMessage) {
    CompositeByteBuf buffer = (CompositeByteBuf) aggregatedMessage.content();
    assertEquals(2, buffer.numComponents());
    List<ByteBuf> buffers = buffer.decompose(0, buffer.capacity());
    assertEquals(2, buffers.size());
    for (ByteBuf buf: buffers) {
      // This should be false as we decompose the buffer before to not have deep hierarchy
      assertFalse(buf instanceof CompositeByteBuf);
    }
    aggregatedMessage.release();
  }

  @Test
  public void testAggregateWithTrailer() {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(1024 * 1024);
    EmbeddedChannel embedder = new EmbeddedChannel(aggr);
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://localhost");
    message.headers().set(of("X-Test"), true);
    HttpUtil.setTransferEncodingChunked(message, true);
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));
    LastHttpContent trailer = new DefaultLastHttpContent();
    trailer.trailingHeaders().set(of("X-Trailer"), true);

    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));
    assertFalse(embedder.writeInbound(chunk2));

    // this should trigger a channelRead event so return true
    assertTrue(embedder.writeInbound(trailer));
    assertTrue(embedder.finish());
    FullHttpRequest aggregatedMessage = embedder.readInbound();
    assertNotNull(aggregatedMessage);

    assertEquals(
        chunk1.content().readableBytes() + chunk2.content().readableBytes(),
        HttpUtil.getContentLength(aggregatedMessage));
    assertEquals(Boolean.TRUE.toString(), aggregatedMessage.headers().get(of("X-Test")));
    assertEquals(Boolean.TRUE.toString(), aggregatedMessage.trailingHeaders().get(of("X-Trailer")));
    checkContentBuffer(aggregatedMessage);
    assertNull(embedder.readInbound());
  }

  @Test
  public void testOversizedRequest() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(4));
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));
    HttpContent chunk3 = LastHttpContent.EMPTY_LAST_CONTENT;

    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));
    assertFalse(embedder.writeInbound(chunk2));

    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
    assertFalse(embedder.isOpen());

    try {
      assertFalse(embedder.writeInbound(chunk3));
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof ClosedChannelException);
    }

    assertFalse(embedder.finish());
  }

  @Test
  public void testOversizedRequestWithoutKeepAlive() {
    // send a HTTP/1.0 request with no keep-alive header
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.PUT, "http://localhost");
    HttpUtil.setContentLength(message, 5);
    checkOversizedRequest(message);
  }

  @Test
  public void testOversizedRequestWithContentLength() {
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");
    HttpUtil.setContentLength(message, 5);
    checkOversizedRequest(message);
  }

  private static void checkOversizedRequest(HttpRequest message) {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(4));

    assertFalse(embedder.writeInbound(message));
    HttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    if (serverShouldCloseConnection(message, response)) {
      assertFalse(embedder.isOpen());
      assertFalse(embedder.finish());
    } else {
      assertTrue(embedder.isOpen());
    }
  }

  private static boolean serverShouldCloseConnection(HttpRequest message, HttpResponse response) {
    // If the response wasn't keep-alive, the server should close the connection.
    if (!HttpUtil.isKeepAlive(response)) {
      return true;
    }
    // The connection should only be kept open if Expect: 100-continue is set,
    // or if keep-alive is on.
    if (HttpUtil.is100ContinueExpected(message)) {
      return false;
    }
    if (HttpUtil.isKeepAlive(message)) {
      return false;
    }
    return true;
  }

  @Test
  public void testOversizedResponse() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(4));
    HttpResponse message = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));

    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));

    try {
      embedder.writeInbound(chunk2);
      fail();
    } catch (TooLongFrameException expected) {
      // Expected
    }

    assertFalse(embedder.isOpen());
    assertFalse(embedder.finish());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidConstructorUsage() {
    new BasicHttpObjectAggregator(-1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidMaxCumulationBufferComponents() {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(Integer.MAX_VALUE);
    aggr.setMaxCumulationBufferComponents(1);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testSetMaxCumulationBufferComponentsAfterInit() throws Exception {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(Integer.MAX_VALUE);
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    aggr.handlerAdded(ctx);
    Mockito.verifyNoMoreInteractions(ctx);
    aggr.setMaxCumulationBufferComponents(10);
  }

  @Test
  public void testAggregateTransferEncodingChunked() {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(1024 * 1024);
    EmbeddedChannel embedder = new EmbeddedChannel(aggr);

    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");
    message.headers().set(of("X-Test"), true);
    message.headers().set(of("Transfer-Encoding"), of("Chunked"));
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));
    HttpContent chunk3 = LastHttpContent.EMPTY_LAST_CONTENT;
    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));
    assertFalse(embedder.writeInbound(chunk2));

    // this should trigger a channelRead event so return true
    assertTrue(embedder.writeInbound(chunk3));
    assertTrue(embedder.finish());
    FullHttpRequest aggregatedMessage = embedder.readInbound();
    assertNotNull(aggregatedMessage);

    assertEquals(
        chunk1.content().readableBytes() + chunk2.content().readableBytes(),
        HttpUtil.getContentLength(aggregatedMessage));
    assertEquals(Boolean.TRUE.toString(), aggregatedMessage.headers().get(of("X-Test")));
    checkContentBuffer(aggregatedMessage);
    assertNull(embedder.readInbound());
  }

  @Test
  public void testBadRequest() {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpServerCodec(), new BasicHttpObjectAggregator(1024 * 1024));
    ch.writeInbound(Unpooled.copiedBuffer("GET / HTTP/1.0 with extra\r\n", StandardCharsets.UTF_8));
    Object inbound = ch.readInbound();
    assertTrue(inbound instanceof BasicFullHttpRequest);
    assertTrue(((DecoderResultProvider) inbound).decoderResult().isFailure(), inbound.toString());
    assertNull(ch.readInbound());
    ch.finish();
  }

  @Test
  public void testBadResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new HttpResponseDecoder(), new BasicHttpObjectAggregator(1024 * 1024));
    ch.writeInbound(Unpooled.copiedBuffer("HTTP/1.0 BAD_CODE Bad Server\r\n", StandardCharsets.UTF_8));
    Object inbound = ch.readInbound();
    assertTrue(inbound instanceof FullHttpResponse);
    assertTrue(((DecoderResultProvider) inbound).decoderResult().isFailure());
    assertNull(ch.readInbound());
    ch.finish();
  }

  @Test
  public void testOversizedRequestWith100Continue() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(8));

    // Send an oversized request with 100 continue.
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");
    HttpUtil.set100ContinueExpected(message, true);
    HttpUtil.setContentLength(message, 16);

    HttpContent chunk1 = new DefaultHttpContent(encodeString("some", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk3 = LastHttpContent.EMPTY_LAST_CONTENT;

    // Send a request with 100-continue + large Content-Length header value.
    assertFalse(embedder.writeInbound(message));

    // The aggregator should respond with '413.'
    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    // An ill-behaving client could continue to send data without a respect, and such data should be discarded.
    assertFalse(embedder.writeInbound(chunk1));

    // The aggregator should not close the connection because keep-alive is on.
    assertTrue(embedder.isOpen());

    // Now send a valid request.
    HttpRequest message2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");

    assertFalse(embedder.writeInbound(message2));
    assertFalse(embedder.writeInbound(chunk2));
    assertTrue(embedder.writeInbound(chunk3));

    FullHttpRequest fullMsg = embedder.readInbound();
    assertNotNull(fullMsg);

    assertEquals(
        chunk2.content().readableBytes() + chunk3.content().readableBytes(),
        HttpUtil.getContentLength(fullMsg));

    assertEquals(HttpUtil.getContentLength(fullMsg), fullMsg.content().readableBytes());

    fullMsg.release();
    assertFalse(embedder.finish());
  }

  @Test
  public void testUnsupportedExpectHeaderExpectation() {
    runUnsupportedExceptHeaderExceptionTest(true);
    runUnsupportedExceptHeaderExceptionTest(false);
  }

  private static void runUnsupportedExceptHeaderExceptionTest(final boolean close) {
    final BasicHttpObjectAggregator aggregator;
    final int maxContentLength = 4;
    if (close) {
      aggregator = new BasicHttpObjectAggregator(maxContentLength, true);
    } else {
      aggregator = new BasicHttpObjectAggregator(maxContentLength);
    }
    final EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpRequestDecoder(), aggregator);

    assertFalse(
        embedder.writeInbound(
            encodeString(
                "GET / HTTP/1.1\r\n" + "Expect: chocolate=yummy\r\n" + "Content-Length: 100\r\n\r\n",
                StandardCharsets.US_ASCII)));
    assertNull(embedder.readInbound());

    final FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.EXPECTATION_FAILED, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
    response.release();

    if (close) {
      assertFalse(embedder.isOpen());
    } else {
      // keep-alive is on by default in HTTP/1.1, so the connection should be still alive
      assertTrue(embedder.isOpen());

      // the decoder should be reset by the aggregator at this point and be able to decode the next request
      assertTrue(embedder.writeInbound(Unpooled.copiedBuffer("GET / HTTP/1.1\r\n\r\n", StandardCharsets.US_ASCII)));

      final FullHttpRequest request = embedder.readInbound();
      assertSame(request.method(), HttpMethod.GET);
      assertEquals(request.uri(), "/");
      assertEquals(request.content().readableBytes(), 0);
      request.release();
    }

    assertFalse(embedder.finish());
  }

  @Test
  public void testValidRequestWith100ContinueAndDecoder() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpRequestDecoder(), new BasicHttpObjectAggregator(100));
    embedder.writeInbound(
        encodeString(
            "GET /upload HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 0\r\n\r\n",
            StandardCharsets.US_ASCII));

    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.CONTINUE, response.status());
    FullHttpRequest request = embedder.readInbound();
    assertFalse(request.headers().contains(HttpHeaderNames.EXPECT));
    request.release();
    response.release();
    assertFalse(embedder.finish());
  }

  @Test
  public void testOversizedRequestWith100ContinueAndDecoder() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpRequestDecoder(), new BasicHttpObjectAggregator(4));
    embedder.writeInbound(
        encodeString(
            "PUT /upload HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 100\r\n\r\n",
            StandardCharsets.US_ASCII));

    assertNull(embedder.readInbound());

    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status(), response.toString());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    // Keep-alive is on by default in HTTP/1.1, so the connection should be still alive.
    assertTrue(embedder.isOpen());

    // The decoder should be reset by the aggregator at this point and be able to decode the next request.
    embedder.writeInbound(Unpooled.copiedBuffer("GET /max-upload-size HTTP/1.1\r\n\r\n", StandardCharsets.US_ASCII));

    FullHttpRequest request = embedder.readInbound();
    assertSame(request.method(), HttpMethod.GET);
    assertEquals(request.uri(), "/max-upload-size");
    assertEquals(request.content().readableBytes(), 0);
    request.release();

    assertFalse(embedder.finish());
  }

  @Test
  public void testOversizedRequestWith100ContinueAndDecoderCloseConnection() {
    EmbeddedChannel embedder =
        new EmbeddedChannel(new BasicHttpRequestDecoder(), new BasicHttpObjectAggregator(4, true));
    embedder.writeInbound(
        encodeString(
            "PUT /upload HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 100\r\n\r\n",
            StandardCharsets.US_ASCII));

    assertNull(embedder.readInbound());

    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    // We are forcing the connection closed if an expectation is exceeded.
    assertFalse(embedder.isOpen());
    assertFalse(embedder.finish());
  }

  @Test
  public void testRequestAfterOversized100ContinueAndDecoder() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpRequestDecoder(), new BasicHttpObjectAggregator(15));

    // Write first request with Expect: 100-continue.
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");
    HttpUtil.set100ContinueExpected(message, true);
    HttpUtil.setContentLength(message, 16);

    HttpContent chunk1 = new DefaultHttpContent(encodeString("some", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk3 = LastHttpContent.EMPTY_LAST_CONTENT;

    // Send a request with 100-continue + large Content-Length header value.
    assertFalse(embedder.writeInbound(message));

    // The aggregator should respond with '413'.
    FullHttpResponse response = embedder.readOutbound();
    assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    // An ill-behaving client could continue to send data without a respect, and such data should be discarded.
    assertFalse(embedder.writeInbound(chunk1));

    // The aggregator should not close the connection because keep-alive is on.
    assertTrue(embedder.isOpen());

    // Now send a valid request.
    HttpRequest message2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "http://localhost");

    assertFalse(embedder.writeInbound(message2));
    assertFalse(embedder.writeInbound(chunk2));
    assertTrue(embedder.writeInbound(chunk3));

    FullHttpRequest fullMsg = embedder.readInbound();
    assertNotNull(fullMsg);

    assertEquals(
        chunk2.content().readableBytes() + chunk3.content().readableBytes(),
        HttpUtil.getContentLength(fullMsg));

    assertEquals(HttpUtil.getContentLength(fullMsg), fullMsg.content().readableBytes());

    fullMsg.release();
    assertFalse(embedder.finish());
  }

  @Test
  public void testReplaceAggregatedRequest() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(1024 * 1024));

    Exception boom = new Exception("boom");
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://localhost");
    req.setDecoderResult(DecoderResult.failure(boom));

    assertTrue(embedder.writeInbound(req) && embedder.finish());

    FullHttpRequest aggregatedReq = embedder.readInbound();
    FullHttpRequest replacedReq = aggregatedReq.replace(Unpooled.EMPTY_BUFFER);

    assertSame(replacedReq.decoderResult(), aggregatedReq.decoderResult());
    aggregatedReq.release();
    replacedReq.release();
  }

  @Test
  public void testReplaceAggregatedResponse() {
    EmbeddedChannel embedder = new EmbeddedChannel(new BasicHttpObjectAggregator(1024 * 1024));

    Exception boom = new Exception("boom");
    HttpResponse rep = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    rep.setDecoderResult(DecoderResult.failure(boom));

    assertTrue(embedder.writeInbound(rep) && embedder.finish());

    FullHttpResponse aggregatedRep = embedder.readInbound();
    FullHttpResponse replacedRep = aggregatedRep.replace(Unpooled.EMPTY_BUFFER);

    assertSame(replacedRep.decoderResult(), aggregatedRep.decoderResult());
    aggregatedRep.release();
    replacedRep.release();
  }

  @Test
  public void testSetURI() {
    BasicHttpObjectAggregator aggr = new BasicHttpObjectAggregator(1024 * 1024);
    EmbeddedChannel embedder = new EmbeddedChannel(aggr);
    HttpRequest message = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://localhost");
    message.headers().set(of("X-Test"), true);
    HttpContent chunk1 = new DefaultHttpContent(encodeString("test", StandardCharsets.US_ASCII));
    HttpContent chunk2 = new DefaultHttpContent(encodeString("test2", StandardCharsets.US_ASCII));
    HttpContent chunk3 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
    assertFalse(embedder.writeInbound(message));
    assertFalse(embedder.writeInbound(chunk1));
    assertFalse(embedder.writeInbound(chunk2));
    // this should trigger a channelRead event so return true
    assertTrue(embedder.writeInbound(chunk3));
    assertTrue(embedder.finish());
    FullHttpRequest aggregatedMessage = embedder.readInbound();
    assertNotNull(aggregatedMessage);
    aggregatedMessage.setUri("bla");
    assertSame(aggregatedMessage.getUri(), aggregatedMessage.uri());
    assertTrue(aggregatedMessage.decoderResult().isSuccess());
    aggregatedMessage.release();
    embedder.finishAndReleaseAll();
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
