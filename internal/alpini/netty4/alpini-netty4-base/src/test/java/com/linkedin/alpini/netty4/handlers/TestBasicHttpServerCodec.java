package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Based upon test {@code netty-4.1.14.Final/codec-http/src/test/java/io/netty/handler/codec/http/HttpServerCodecTest.java}
 *
 * Created by acurtis on 2/7/18.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestBasicHttpServerCodec extends AbstractLeakDetect {
  /**
   * Testcase for https://github.com/netty/netty/issues/433
   */
  @Test
  public void testUnfinishedChunkedHttpRequestIsLastFlag() throws Exception {

    int maxChunkSize = 2000;
    BasicHttpServerCodec httpServerCodec = new BasicHttpServerCodec(1000, 1000, maxChunkSize);
    EmbeddedChannel decoderEmbedder = new EmbeddedChannel(httpServerCodec);
    decoderEmbedder.config().setAllocator(POOLED_ALLOCATOR);

    int totalContentLength = maxChunkSize * 5;
    decoderEmbedder.writeInbound(
        encodeString(
            "PUT /test HTTP/1.1\r\n" + "Content-Length: " + totalContentLength + "\r\n" + "\r\n",
            StandardCharsets.UTF_8));

    int offeredContentLength = (int) (maxChunkSize * 2.5);
    decoderEmbedder.writeInbound(prepareDataChunk(offeredContentLength));
    decoderEmbedder.finish();

    HttpMessage httpMessage = decoderEmbedder.readInbound();
    Assert.assertNotNull(httpMessage);
    Assert.assertFalse(httpMessage instanceof HttpContent);

    boolean empty = true;
    int totalBytesPolled = 0;
    for (;;) {
      HttpContent httpChunk = decoderEmbedder.readInbound();
      if (httpChunk == null) {
        break;
      }
      empty = false;
      totalBytesPolled += httpChunk.content().readableBytes();
      Assert.assertFalse(httpChunk instanceof LastHttpContent);
      httpChunk.release();
    }
    Assert.assertFalse(empty);
    Assert.assertEquals(totalBytesPolled, offeredContentLength);
  }

  @Test
  public void test100Continue() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpServerCodec(), new BasicHttpObjectAggregator(1024));
    ch.config().setAllocator(POOLED_ALLOCATOR);

    // Send the request headers.
    ch.writeInbound(
        encodeString(
            "PUT /upload-large HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 1\r\n\r\n",
            StandardCharsets.UTF_8));

    // Ensure the aggregator generates nothing.
    Assert.assertNull(ch.readInbound());

    // Ensure the aggregator writes a 100 Continue response.
    ByteBuf continueResponse = ch.readOutbound();
    Assert.assertEquals(continueResponse.toString(StandardCharsets.UTF_8), "HTTP/1.1 100 Continue\r\n\r\n");
    continueResponse.release();

    // But nothing more.
    Assert.assertNull(ch.readOutbound());

    // Send the content of the request.
    ch.writeInbound(copyOf(new byte[] { 42 }));

    // Ensure the aggregator generates a full request.
    FullHttpRequest req = ch.readInbound();
    Assert.assertEquals(req.headers().get(HttpHeaderNames.CONTENT_LENGTH), "1");
    Assert.assertEquals(req.content().readableBytes(), 1);
    Assert.assertEquals(req.content().readByte(), (byte) 42);
    req.release();

    // But nothing more.
    Assert.assertNull(ch.readInbound());

    // Send the actual response.
    FullHttpResponse res = new BasicFullHttpResponse(req, HttpResponseStatus.CREATED, POOLED_ALLOCATOR.buffer());
    res.content().writeBytes("OK".getBytes(StandardCharsets.UTF_8));
    res.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 2);
    ch.writeOutbound(res);

    // Ensure the encoder handles the response after handling 100 Continue.
    ByteBuf encodedRes = ch.readOutbound();
    Assert.assertEquals(
        encodedRes.toString(StandardCharsets.UTF_8),
        "HTTP/1.1 201 Created\r\n" + HttpHeaderNames.CONTENT_LENGTH + ": 2\r\n\r\nOK");
    encodedRes.release();

    ch.finish();
  }

  @Test
  public void testChunkedHeadResponse() {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpServerCodec());
    ch.config().setAllocator(POOLED_ALLOCATOR);

    // Send the request headers.
    Assert.assertTrue(ch.writeInbound(encodeString("HEAD / HTTP/1.1\r\n\r\n", StandardCharsets.UTF_8)));

    HttpRequest request = ch.readInbound();
    Assert.assertEquals(request.method(), HttpMethod.HEAD);
    LastHttpContent content = ch.readInbound();
    Assert.assertFalse(content.content().isReadable());
    content.release();

    HttpResponse response = new BasicHttpResponse(request, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    Assert.assertTrue(ch.writeOutbound(response));
    Assert.assertTrue(ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT));
    Assert.assertTrue(ch.finish());

    ByteBuf buf = ch.readOutbound();
    Assert
        .assertEquals(buf.toString(StandardCharsets.US_ASCII), "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n");
    buf.release();

    buf = ch.readOutbound();
    Assert.assertFalse(buf.isReadable());
    buf.release();

    Assert.assertFalse(ch.finishAndReleaseAll());
  }

  @Test
  public void testChunkedHeadFullHttpResponse() {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpServerCodec());
    ch.config().setAllocator(POOLED_ALLOCATOR);

    // Send the request headers.
    Assert.assertTrue(ch.writeInbound(encodeString("HEAD / HTTP/1.1\r\n\r\n", StandardCharsets.UTF_8)));

    HttpRequest request = ch.readInbound();
    Assert.assertEquals(HttpMethod.HEAD, request.method());
    LastHttpContent content = ch.readInbound();
    Assert.assertFalse(content.content().isReadable());
    content.release();

    FullHttpResponse response = new BasicFullHttpResponse(request, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    Assert.assertTrue(ch.writeOutbound(response));
    Assert.assertTrue(ch.finish());

    ByteBuf buf = ch.readOutbound();
    Assert
        .assertEquals(buf.toString(StandardCharsets.US_ASCII), "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n");
    buf.release();

    Assert.assertFalse(ch.finishAndReleaseAll());
  }

  @Test(invocationCount = 5)
  public void testOutOfOrderResponses() {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpServerCodec());
    ch.config().setAllocator(POOLED_ALLOCATOR);

    // Send the pipelined request headers.
    Assert.assertTrue(
        ch.writeInbound(encodeString("HEAD / HTTP/1.1\r\n\r\n" + "HEAD / HTTP/1.1\r\n\r\n", StandardCharsets.UTF_8)));

    HttpRequest request1 = ch.readInbound();
    Assert.assertEquals(request1.method(), HttpMethod.HEAD);
    Assert.assertFalse(request1 instanceof HttpContent);
    LastHttpContent content = ch.readInbound();
    Assert.assertFalse(content.content().isReadable());
    content.release();

    HttpRequest request2 = ch.readInbound();
    Assert.assertEquals(request2.method(), HttpMethod.HEAD);
    Assert.assertFalse(request2 instanceof HttpContent);
    content = ch.readInbound();
    Assert.assertFalse(content.content().isReadable());
    content.release();

    HttpResponse response = new BasicHttpResponse(request2, HttpResponseStatus.OK);
    HttpUtil.setTransferEncodingChunked(response, true);
    ChannelFuture future = ch.writeOneOutbound(response);

    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isSuccess());
    Assert.assertTrue(ch.isOpen());

    ch.pipeline().fireExceptionCaught(future.cause());

    Assert.assertFalse(ch.isOpen());

    ch.finishAndReleaseAll();
  }

  private static ByteBuf prepareDataChunk(int size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; ++i) {
      sb.append('a');
    }
    return encodeString(sb, StandardCharsets.UTF_8);
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
